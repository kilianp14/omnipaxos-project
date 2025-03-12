use crate::{configs::OmniPaxosSqlConfig, database::Database, network::{self, Network}, server::OmniPaxosServer, server::MediatorMessage, server::Mediator};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
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
const SHARD_TIMEOUT: Duration = Duration::from_secs(1);


// Define a callback trait with an async function.
// #[async_trait]
// #[async_trait]
// pub trait ServerCallback: Send + Sync {
//     // async fn send_to_cluster2(&self, sender_Id: NodeId, msg: ClusterMessage);
//     // async fn send_to_client(&self, client_id: ClientId, msg: ServerMessage);
//     async fn ack_from_shard(&self, command_id: CommandId);
// }

// #[async_trait]
// impl ServerCallback for tokio::sync::Mutex<OmniPaxosServer> {
//     // async fn send_to_cluster2(&self, sender_Id: NodeId, msg: ClusterMessage) {
//     //     let guard = self.lock().await;
//     //     guard.send_to_cluster2(sender_Id, msg).await;

//     // }
//     // async fn send_to_client(&self, client_id: ClientId, msg: ServerMessage) {
//     //     let guard = self.lock().await;
//     //     let mut net = guard.network.lock().await;
//     //     net.send_to_client(client_id, msg);
//     // }
//     async fn ack_from_shard(&self, command_id: CommandId) {
//         let mut guard = self.lock().await;
//         guard.ack_from_shard(command_id);
//     }
// }


pub struct Shard {
    id: NodeId,
    network: Arc<tokio::sync::Mutex<Network>>,
    database: Arc<Database>,
    // omnipaxos: OmniPaxosInstance,
    // omnipaxos_msg_buffer: Vec<Message<Command>>,
    // New second omnipaxos instance and its message buffer
    omnipaxos2: OmniPaxosInstance,
    omnipaxos_msg_buffer2: Vec<Message<Command>>,
    // current_decided_idx: usize,
    current_decided_shard_idx: usize,
    config: OmniPaxosSqlConfig,
    peers: Vec<NodeId>,
    // callback: Option<Arc<dyn ServerCallback>>,
    mediator: Mediator,
}
    

impl Shard {
    pub async fn new(config: OmniPaxosSqlConfig, database: Arc<Database>, network: Arc<tokio::sync::Mutex<Network>>,mediator: Mediator) -> Self {
        // Initialize first OmniPaxos instance
        // let storage: MemoryStorage<Command> = MemoryStorage::default();
        // let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        // let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        // let omnipaxos = omnipaxos_config.build(storage).unwrap();

        // Initialize second OmniPaxos instance (separate from the first one)
        let storage2: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config2: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer2 = Vec::with_capacity(omnipaxos_config2.server_config.buffer_size);
        let omnipaxos2 = omnipaxos_config2.clone().build(storage2).unwrap();


        Shard {
            id: config.local.server_id,
            network,
            database,
            // omnipaxos,
            // omnipaxos_msg_buffer,
            omnipaxos2,
            omnipaxos_msg_buffer2,
            // current_decided_idx: 0,
            current_decided_shard_idx: 0,
            peers: config.get_peers(config.local.server_id),
            config,
            // callback: None,
            mediator,
        }
    }

    // pub fn set_callback(&mut self, callback: Arc<dyn ServerCallback>) {
    //     self.callback = Some(callback);
    // }

    pub async fn run(&mut self, rx: Receiver<MediatorMessage>) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");

        // let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster2_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster2_msg_buf).await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        // let mut shardTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);
        let mut mpscTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    // self.omnipaxos.tick();
                    // self.send_outgoing_msgs().await;
                    self.omnipaxos2.tick();
                    self.send_outgoing_msgs2().await;
                },
                _ = async {
                    let mut net = self.network.lock().await;
                    net.cluster2_messages.recv_many(&mut cluster2_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster2_messages(&mut cluster2_msg_buf).await;
                },
                // _ = async {
                //     if let Some(callback) = &self.callback {
                //         callback.read_cluster_msgs(&mut cluster2_msg_buf).await;
                //     }
                //     // self.network.cluster2_messages.recv_many(&mut cluster2_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                   
                //     self.handle_cluster2_messages(&mut cluster2_msg_buf).await;
                // },
                // _ = async {
                //     self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_client_messages(&mut client_msg_buf).await;
                // },
                // _ = shardTimeoutInterval.tick() => {
                //     self.check_pending_transactions().await;
                // }
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

    
    // pub async fn run_mpsc(&self, rx: Receiver<MediatorMessage>) {
    //     tokio::spawn(async move {
    //         for message in rx {
    //             if let MediatorMessage::PrepareFromServer(cmd) = message.clone() {
    //                 // println!("Shard received: Prepare from server{:?}", cmd);
    //                 self.send_prepare_to_shard(cmd).await;
    //             }
    //             if let MediatorMessage::CommitOrAbortFromServer(cmd) = message {
    //                 println!("Shard received: commit or abort from server{:?}", cmd);
    //             }
    //         }
    //     });
    // }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster2_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
    ) {
        // let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        let mut leader_takeover_interval2 = tokio::time::interval(LEADER_WAIT);
        // loop {
        //     tokio::select! {
        //         _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
        //             if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
        //                 if curr_leader == self.id && is_accept_phase {
        //                     info!("{}: Leader coodinator fully initialized", self.id);
        //                     let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
        //                     self.send_cluster_start_signals(experiment_sync_start).await;
        //                     self.send_client_start_signals(experiment_sync_start).await;
        //                     break;
        //                 }
        //             }
        //             info!("{}: Attempting to take leadership for coordinator", self.id);
        //             self.omnipaxos.try_become_leader();
        //             self.send_outgoing_msgs().await;
        //         },
        //         _ = async {
        //             self.network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await
        //         } => {
        //             let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
        //             if recv_start {
        //                 break;
        //             }
        //         },
        //         _ = async {
        //             self.network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE).await
        //         } => {
        //             self.handle_client_messages(client_msg_buffer).await;
        //         },
        //     }
        // }
        
        // get the shards leader
        loop {
            tokio::select! {
                _ = leader_takeover_interval2.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos2.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader shard fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster2_start_signals(experiment_sync_start).await;
                            // self.send_client_start_signals(experiment_sync_start).await;
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership for shard", self.id);
                    self.omnipaxos2.try_become_leader();
                    self.send_outgoing_msgs2().await;
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.cluster2_messages.recv_many(cluster2_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    let recv_start = self.handle_cluster2_messages(cluster2_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                // _ = async {
                //     if let Some(callback) = &self.callback {
                //         callback.read_cluster_msgs(cluster2_msg_buffer).await;
                //     }
                //     // self.network.cluster2_messages.recv_many(cluster2_msg_buffer, NETWORK_BATCH_SIZE).await
                // } => {
                //     let recv_start = self.handle_cluster2_messages(cluster2_msg_buffer).await;
                //     if recv_start {
                //         break;
                //     }
                // },
            }
        }
    }

    // shard
    async fn handle_cluster2_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos2.handle_incoming(m);
                    self.handle_decided_shard_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    // self.send_client_start_signals(start_time).await;
                }
                ClusterMessage::ReadRequest(client_id, sender_id, command_id, sql_command) => {
                    let response = self.database.prepare_command(sql_command, command_id).await;
                    let msg = ClusterMessage::ReadResponse(client_id, command_id, response);
                    // if let Some(callback) = &self.callback {
                    //     callback.send_to_cluster2(sender_id, msg).await;
                    // }
                    let mut network = self.network.lock().await;
                    network.send_to_cluster2(sender_id, msg);
                }
                ClusterMessage::ReadResponse(client_id, command_id, response) => {
                    let msg = ServerMessage::Answer(command_id, response);
                    // if let Some(callback) = &self.callback {
                    //     callback.send_to_client(client_id, msg).await;
                    // }
                    let mut network = self.network.lock().await;
                    network.send_to_client(client_id, msg);
                }
            }
        }
        self.send_outgoing_msgs2().await;
        received_start_signal
    }

    // shard
    async fn handle_decided_shard_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
        let new_decided_idx = self.omnipaxos2.get_decided_idx();
        if self.current_decided_shard_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos2
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
            self.update_database_and_respond(decided_commands).await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here (batch at handle_cluster_messages)
        // This todo was already in the repo, dont think we actually need to do batching
        // For now lets just do write-through
        for command in commands {
            let response = match command.phase {
                Some(Phase::Prepare) => self.database.prepare_command(command.sql_cmd, command.id).await,
                Some(Phase::Commit) => self.database.commit_command(command.id).await,
                Some(Phase::Abort) => self.database.abort_command(command.id).await,
                None => None,
            };

            if command.coordinator_id == self.id {
                match command.phase {
                    Some(Phase::Prepare) => {
                        info!("{} shard: Acknowledging command {}", self.id, command.id);
                        // if let Some(callback) = &self.callback {
                        //     callback.ack_from_shard(command.id).await;
                        // }
                        // self.ack_from_shard(command.id);
                        self.mediator.ack_from_shard(command.id);
                    }
                    _ => {}
                }
                let msg = ServerMessage::Answer(command.id, response);
                // if let Some(callback) = &self.callback {
                //     callback.send_to_client(command.client_id, msg).await;
                // }
                let mut network = self.network.lock().await;
                network.send_to_client(command.client_id, msg);
            }
        }
    }
    
    fn append_to_log2(&mut self, from: ClientId, command_id: CommandId, sql_command: SqlCommand, phase: Phase) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            sql_cmd: sql_command,
            phase: Some(phase),
        };
        self.omnipaxos2
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }


    // Sends outgoing messages from the second omnipaxos instance using the cluster2 channel
    async fn send_outgoing_msgs2(&mut self) {
        self.omnipaxos2.take_outgoing_messages(&mut self.omnipaxos_msg_buffer2);
        for msg in self.omnipaxos_msg_buffer2.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            // if let Some(callback) = &self.callback {
            //     callback.send_to_cluster2(to, cluster_msg).await;
            // }
            let mut network = self.network.lock().await;
            network.send_to_cluster2(to, cluster_msg);
        }
    }


    async fn send_cluster2_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            // if let Some(callback) = &self.callback {
            //     callback.send_to_cluster2(*peer, msg).await;
            // }
            let mut network = self.network.lock().await;
            network.send_to_cluster2(*peer, msg);
        }
    }


    pub async fn send_prepare_to_shard(&mut self, command: Command) {
        let sql_cmd = command.sql_cmd.clone();
        match sql_cmd.query_type {
            QueryType::Select => {
                self.handle_read_message(command.client_id, command.id, sql_cmd).await;
                // self.ack_from_shard(command.id);
            }
            _ => {
                self.append_to_log2(command.client_id, command.id, sql_cmd, command.phase.unwrap());
            }
        }
        self.send_outgoing_msgs2().await;
    }

    pub async fn commit_or_abort_on_shard(&mut self, command: Command) {
        self.update_database_and_respond(vec![command]).await;
    }

    // shard
    async fn handle_read_message(
        &mut self,
        client_id: ClientId,
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
                let msg = ServerMessage::Answer(command_id, response);
                // if let Some(callback) = &self.callback {
                //     callback.send_to_client(client_id, msg).await;
                // }
                let mut network = self.network.lock().await;
                network.send_to_client(client_id, msg);
                // self.ack_from_shard(command_id);
            }
            Consistency::Leader => {
                if let Some((leader_id, is_accept_phase)) = self.omnipaxos2.get_current_leader() {
                    if leader_id == self.id && is_accept_phase {
                        // We are the leader, process locally
                        let response = self.database.prepare_command(sql_command, command_id).await;
                        let msg = ServerMessage::Answer(command_id, response);
                        // if let Some(callback) = &self.callback {
                        //     callback.send_to_client(client_id, msg).await;
                        // }
                        let mut network = self.network.lock().await;
                        network.send_to_client(client_id, msg);
                        // self.ack_from_shard(command_id);
                    } else {
                        // Forward to leader
                        let forward_msg = ClusterMessage::ReadRequest(
                            client_id,
                            self.id,
                            command_id,
                            sql_command,
                        );
                        info!("{}: Forwarding read request to leader {}", self.id, leader_id);
                        // if let Some(callback) = &self.callback {
                        //     callback.send_to_cluster2(leader_id, forward_msg).await;
                        // }
                        let mut network = self.network.lock().await;
                        network.send_to_cluster2(leader_id, forward_msg);
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
                match self.omnipaxos2.append(read_command) {
                    Ok(_) => {
                        // TODO: Verify this works as expected.
                        // The read will be processed when it's decided
                        // No need to send response here as it will be sent
                        // in update_database_and_respond when the command is decided
                    }
                    Err(e) => {
                        //TODO:send abort to coordinator
                        let response = format!("Failed to achieve linearizable read: {:?}", e);
                        let msg = ServerMessage::Answer(command_id, Some(response));
                        // if let Some(callback) = &self.callback {
                        //     callback.send_to_client(client_id, msg).await;
                        // }
                        let mut network = self.network.lock().await;
                        network.send_to_client(client_id, msg);
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

