use crate::{configs::OmniPaxosSqlConfig, database::Database, network::{self, Network}, server::OmniPaxosServer, server::MediatorMessage, server::Mediator};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use serde::de::value::U64Deserializer;
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;
use omnipaxos_sql::common::{messages::*, sql::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::sync::Arc;
use std::{fs::File, io::Write, time::Duration};
use tokio::{net, sync::Mutex};
use std::rc::Rc;
use std::cell::RefCell;
use async_trait::async_trait;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const SHARD_TIMEOUT: Duration = Duration::from_millis(100);


pub struct Shard {
    id: NodeId,
    network: Arc<tokio::sync::Mutex<Network>>,
    database: Arc<Database>,
    omnipaxos: OmniPaxosInstance,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    current_decided_shard_idx: usize,
    config: OmniPaxosSqlConfig,
    peers: Vec<NodeId>,
    mediator: Mediator,
    shard_id: i32,
}
    

impl Shard {
    pub async fn new(config: OmniPaxosSqlConfig, database: Arc<Database>, network: Arc<tokio::sync::Mutex<Network>>,mediator: Mediator, shard_id:i32) -> Self {
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.clone().build(storage).unwrap();


        Shard {
            id: config.local.server_id,
            network,
            database,
            omnipaxos,
            omnipaxos_msg_buffer,
            current_decided_shard_idx: 0,
            peers: config.get_peers(config.local.server_id),
            config,
            mediator,
            shard_id
        }
    }

    // pub fn set_callback(&mut self, callback: Arc<dyn ServerCallback>) {
    //     self.callback = Some(callback);
    // }

    pub async fn run(&mut self, rx: Receiver<MediatorMessage>) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");

        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf).await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        // let mut shardTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);
        let mut mpscTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                        self.send_outgoing_msgs().await;
                },
                _ = async {
                    let mut net = self.network.lock().await;
                    if self.shard_id == 1 {
                        net.cluster2_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                    } else {
                        net.cluster3_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                    }
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = mpscTimeoutInterval.tick() => {
                    let mut messages = Vec::new();
                    while let Ok(message) = rx.try_recv() {
                        messages.push(message);
                    }
                    for message in messages {
                        if let MediatorMessage::PrepareFromServer(cmd) = message.clone() {
                            self.send_prepare_to_shard(cmd).await;
                        }
                        if let MediatorMessage::CommitOrAbortFromServer(cmd) = message {
                            self.commit_or_abort_on_shard(cmd).await;
                        }
                    }
                }
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader shard fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start).await;
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership for shard", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs().await;
                },
                _ = async {
                    if self.shard_id == 1 {
                        self.network.lock().await.cluster2_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await
                    } else {
                        self.network.lock().await.cluster3_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await
                    }
                } => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
            }
        }
    }

    // shard
    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_shard_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    // self.send_client_start_signals(start_time).await;
                }
                ClusterMessage::ReadRequest(client_id, sender_id, command_id, sql_command) => {
                    let response = self.database.prepare_command(sql_command, command_id).await;
                    let msg = ClusterMessage::ReadResponse(client_id, sender_id, command_id, response);
                    let mut network = self.network.lock().await;
                    if self.shard_id == 1 {
                        network.send_to_cluster2(sender_id, msg);
                    } else {
                        network.send_to_cluster3(sender_id, msg);
                    }
                }
                ClusterMessage::ReadResponse(client_id, coord_id, command_id, response) => {
                    // will be always the shard that also send the cluster so we can just return result to our own coordinator
                    // This is due to the fact that a ReadRequest responds with the result to the shard first, who then forwards it to its own coordaintor, who also sent the query originally. We could skip this step over the intermediate shard, but this is also fine. (one more message)
                    let msg = ClusterMessage::ReadResponse(client_id, coord_id, command_id, response);
                    info!("{} sending from shard {} to {}", self.id, self.id, coord_id);
                    self.mediator.response_from_shard(msg);
                }
            }
        }
        self.send_outgoing_msgs().await;
        received_start_signal
    }

    // shard
    async fn handle_decided_shard_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_shard_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_shard_idx)
                .unwrap();
            self.current_decided_shard_idx = new_decided_idx;
            let decided_commands = decided_entries
                .into_iter()
                .filter_map(|e| match e {
                    LogEntry::Decided(cmd) => Some(cmd),
                    _ => unreachable!(),
                })
                .collect();
            self.update_database_and_respond(decided_commands, true).await;     // is_decided boolean = true means that we expect the shard to respond with the result to the coordiantor the query came from. (This is the case if we are doing a linearizable read)
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>, is_decided: bool) {
        // TODO: batching responses possible here (batch at handle_cluster_messages)
        // This todo was already in the repo, dont think we actually need to do batching
        // For now lets just do write-through
        for command in commands {
            let response = match command.phase {
                Some(Phase::Prepare) => self.database.prepare_command(command.sql_cmd.clone(), command.id).await,
                Some(Phase::Commit) => self.database.commit_command(command.id).await,
                Some(Phase::Abort) => self.database.abort_command(command.id).await,
                None => None,
            };

            if is_decided {
                if command.coordinator_id == self.id {
                    match command.phase {
                        Some(Phase::Prepare) => {
                            match command.sql_cmd.query_type {
                                QueryType::Select => {
                                    // TODO: this is the only case where the read result is not coming from the same shard process as the coordinator send it from. This is the read that is with Lineraizable consistency and was therefore decided by omnipaxos.
                                    let msg = ClusterMessage::ReadResponse(command.client_id, command.coordinator_id, command.id, response);
                                    let mut network = self.network.lock().await;
                                    info!("{} sending from shard {} to {}", self.id, self.id,command.coordinator_id);
                                    network.send_to_cluster(command.coordinator_id, msg);
                                    match command.sql_cmd.consistency.unwrap() {
                                        Consistency::Linearizable => {
                                            info!("{} shard: Acknowledging command (that is linerarizable) {}", self.id, command.id);
                                            self.mediator.ack_from_shard(command.id);
                                        }
                                        _ => {}
                                    }
                                }
                                _ => {
                                    info!("{} shard: Acknowledging command {}", self.id, command.id);
                                    self.mediator.ack_from_shard(command.id);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    
    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, sql_command: SqlCommand, phase: Phase) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            sql_cmd: sql_command,
            phase: Some(phase),
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }


    // Sends outgoing messages from the second omnipaxos instance using the cluster2 channel
    async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos.take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            let mut network = self.network.lock().await;
            if self.shard_id == 1 {
                network.send_to_cluster2(to, cluster_msg);
            } else {
                network.send_to_cluster3(to, cluster_msg);
            }
        }
    }


    async fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            let mut network = self.network.lock().await;
            if self.shard_id == 1 {
                network.send_to_cluster2(*peer, msg);
            } else if self.shard_id == 2 {
                network.send_to_cluster3(*peer, msg);
            }
        }
    }


    // confusing name, but this is the function that the coordinator of this proccess cals to send a propose to the shard
    pub async fn send_prepare_to_shard(&mut self, command: Command) {
        let sql_cmd = command.sql_cmd.clone();
        match sql_cmd.query_type {
            // selects are not decided on the shard level and the result is just send back.
            QueryType::Select => {
                self.handle_read_message(command.client_id, command.coordinator_id, command.id, sql_cmd).await;
            }
            _ => {
                self.append_to_log(command.client_id, command.id, sql_cmd, command.phase.unwrap());
            }
        }
        self.send_outgoing_msgs().await;
    }

    // to send a commit and abort transaction once the prepare is already acked. This doesnt ened to have the omnipaxos stuff. We are using update_database_and_respond in two ways: 1. here just to update the databse and 2. to react to omnipaxos decide msg. these usecases are distinguished by the is_decided boolean.
    pub async fn commit_or_abort_on_shard(&mut self, command: Command) {
        // is decided is false here as this value is imposed by the coordiantor. aggrement is ensured as this was already proposed earlier in the 2pc protocoll. the is_decide boolean contolls wether to respond to the coordinaot with te result. which we only need if the value was freshly decided by omnipaxos (we did a Linearizable Read)
        self.update_database_and_respond(vec![command], false).await;
    }

    // shard
    async fn handle_read_message(
        &mut self,
        client_id: ClientId,
        coordinator_id: NodeId,
        command_id: CommandId,
        sql_command: SqlCommand,
    ) {
        match sql_command
            .consistency
            .clone()
            .unwrap_or(Consistency::Local)
        {
            Consistency::Local => {
                // Read from local DB directly
                let response = self.database.prepare_command(sql_command, command_id).await;
                let msg = ClusterMessage::ReadResponse(client_id, coordinator_id, command_id, response);
                info!("{} sending from shard {} to {}", self.id, self.id,coordinator_id);
                self.mediator.response_from_shard(msg);    // send response back to the coorinator the request came from
            }
            Consistency::Leader => {
                if let Some((leader_id, is_accept_phase)) = self.omnipaxos.get_current_leader() {
                    if leader_id == self.id && is_accept_phase {
                        // We are the leader, process locally
                        let response = self.database.prepare_command(sql_command, command_id).await;
                        let msg = ClusterMessage::ReadResponse(client_id, coordinator_id, command_id, response);
                        info!("{} sending from shard {} to {}", self.id, self.id,coordinator_id);
                        self.mediator.response_from_shard(msg);    // send response back to the coorinator the request came from
                    } else {
                        // Forward to leader
                        let forward_msg = ClusterMessage::ReadRequest(
                            client_id,
                            self.id,
                            command_id,
                            sql_command,
                        );
                        info!("{}: Forwarding read request to leader {}", self.id, leader_id);
                        let mut network = self.network.lock().await;
                        if self.shard_id == 1 {
                            network.send_to_cluster2(leader_id, forward_msg);
                        } else if self.shard_id == 2 {
                            network.send_to_cluster3(leader_id, forward_msg);
                        }
                    }
                }
            }
            Consistency::Linearizable => {
                // For linearizable consistency, we can use a read-impose operation
                // by appending a no-op or read operation to the log
                let read_command = Command {
                    client_id,
                    coordinator_id: self.id,
                    id: command_id,
                    sql_cmd: sql_command,
                    phase: None,
                };
                // Append the read command to the log to ensure linearizability
                match self.omnipaxos.append(read_command) {
                    Ok(_) => {
                        // TODO: Verify this works as expected.
                        // The read will be processed when it's decided
                        // No need to send response here as it will be sent
                        // in update_database_and_respond when the command is decided
                    }
                    Err(e) => {
                        // TODO: implement this special case to respond to the coordinator with a special abort message, that removes this transaction from the pending transactions vector and sends abort to client

                        // somethings one the lines of this but this left over from somwhere else
                        // let response = format!("Failed to achieve linearizable read: {:?}", e);
                        // let msg = ClusterMessage::ReadResponse(client_id, coordinator_id, command_id, Some(response));
                        // info!("{} sending from shard {} to {}", self.id, self.id,coordinator_id);
                        // self.mediator.response_from_shard(msg);
                    }
                }
            }
        };
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }

}

