use crate::{database::Database, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_sql::{common::{messages::*, sql::*, utils::Timestamp}, server::configs::OmniPaxosShardConfig};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{sync::Arc, fs::File, io::Write, time::Duration};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const SHARD_TIMEOUT: Duration = Duration::from_millis(100);


pub struct OmniPaxosShard {
    id: NodeId,
    shard_id: ShardId,
    network: Network,
    database: Arc<Database>,
    omnipaxos: OmniPaxosInstance,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    current_decided_idx: usize,
    config: OmniPaxosShardConfig,
    peers: Vec<NodeId>,
}
    

impl OmniPaxosShard {
    pub async fn new(config: OmniPaxosShardConfig, database: Arc<Database>) -> Self {
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let peers = config.get_peers(config.local.server_id);
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.clone().build(storage).unwrap();
        let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;

        OmniPaxosShard {
            id: config.local.server_id,
            shard_id: config.local.shard_id,
            network,
            database,
            omnipaxos,
            omnipaxos_msg_buffer,
            current_decided_idx: 0,
            config,
            peers: peers,
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut coordinator_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut coordinator_msg_buf)
            .await;
        info!{"{}: Initial election phase over", self.id};
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    match self.omnipaxos.get_current_leader() {
                        Some(leader) => debug!("{}: Current Leader: {}, QC: {}", self.id, leader.0, leader.1),
                        None => {}
                    }
                    self.send_outgoing_msgs();
                },
                _ = self.network.recv_many(&mut cluster_msg_buf, &mut coordinator_msg_buf, NETWORK_BATCH_SIZE) => {
                    if !cluster_msg_buf.is_empty() {
                        self.handle_cluster_messages(&mut cluster_msg_buf).await;
                    }
                    if !coordinator_msg_buf.is_empty() {
                        self.handle_coordinator_messages(&mut coordinator_msg_buf).await;
                    }
                },
            }
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        coordinator_msg_buffer: &mut Vec<CoordinatorMessage>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start);
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.recv_many(cluster_msg_buffer, coordinator_msg_buffer, NETWORK_BATCH_SIZE) => {
                    if !cluster_msg_buffer.is_empty() {
                        let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                        if recv_start {
                            break;
                        }
                    }
                    if !coordinator_msg_buffer.is_empty() {
                        self.handle_coordinator_messages(coordinator_msg_buffer).await;
                    }
                },
            }
        }
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries().await;
                }
                ClusterMessage::LeaderStartSignal(_) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                }
                ClusterMessage::ReadRequest(client_id, sender_id, command_id, sql_command) => {
                    let response = self.database.prepare_command(sql_command, command_id).await;
                    let msg = ClusterMessage::ReadResponse(client_id, command_id, response);
                    self.network.send_to_cluster(sender_id, msg);
                }
                ClusterMessage::ReadResponse(client_id, command_id, response) => {
                    // will be always the shard that also send the cluster so we can just return result to our own coordinator
                    // This is due to the fact that a ReadRequest responds with the result to the shard first, who then forwards it to its own coordaintor, who also sent the query originally. We could skip this step over the intermediate shard, but this is also fine. (one more message)
                    let msg = ShardMessage::Answer(client_id, command_id, response);
                    self.network.send_to_coordinator(msg);
                }
            }
        }
        self.send_outgoing_msgs().await;
        received_start_signal
    }

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx = self.omnipaxos.get_decided_idx();
        if self.current_decided_idx < new_decided_idx {
            let decided_entries = self
                .omnipaxos
                .read_decided_suffix(self.current_decided_idx)
                .unwrap();
            self.current_decided_idx = new_decided_idx;
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
        for command in commands {
            let response = match command.phase {
                Phase::Prepare => self.database.prepare_command(command.sql_cmd.clone(), command.id).await,
                Phase::Commit => self.database.commit_command(command.id).await,
                Phase::Abort => self.database.abort_command(command.id).await,
            };

            if is_decided {
                if command.coordinator_id == self.id {
                    match command.phase {
                        Some(Phase::Prepare) => {
                            match command.sql_cmd.query_type {
                                QueryType::Select => {
                                    // TODO: this is the only case where the read result is not coming from the same shard process as the coordinator send it from. This is the read that is with Lineraizable consistency and was therefore decided by omnipaxos.
                                    let msg = ClusterMessage::ReadResponse(command.client_id, command.coordinator_id, command.id, response);
                                    debug!("{} sending from shard {} to {}", self.id, self.id,command.coordinator_id);
                                    network.send_to_cluster(command.coordinator_id, msg);
                                    match command.sql_cmd.consistency.unwrap() {
                                        Consistency::Linearizable => {
                                            debug!("{} shard: Acknowledging command (that is linerarizable) {}", self.id, command.id);
                                            self.network.send_to_coordinator(ShardMessage::Ack(command.client_id, command.id));
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
            phase:phase,
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
            let msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, msg);
        }
    }


    async fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);

        }
    }

    // confusing name, but this is the function that the coordinator of this proccess cals to send a propose to the shard
    pub async fn send_prepare_to_shard(&mut self, command: Command) {
        let sql_cmd = command.sql_cmd.clone();
        match sql_cmd.query_type {
            // selects are not decided on the shard level and the result is just send back.
            QueryType::Select => {
                self.handle_read_message(command.client_id,  command.id, sql_cmd).await;
            }
            _ => {
                self.append_to_log(command.client_id, command.id, sql_cmd, command.phase);
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
                let msg = ShardMessage::Answer(client_id, command_id, response);
                self.network.send_to_coordinator(msg); // send response back to the coorinator the request came from
            }
            Consistency::Leader => {
                if let Some((leader_id, is_accept_phase)) = self.omnipaxos.get_current_leader() {
                    if leader_id == self.id && is_accept_phase {
                        // We are the leader, process locally
                        let response = self.database.prepare_command(sql_command, command_id).await;
                        let msg = ShardMessage::Answer(client_id, command_id, response);
                        self.network.send_to_coordinator(msg);
                    } else {
                        // Forward to leader
                        let forward_msg = ClusterMessage::ReadRequest(
                            client_id,
                            self.id,
                            command_id,
                            sql_command,
                        );
                        debug!("{}: Forwarding read request to leader {}", self.id, leader_id);
                        self.network.send_to_cluster(leader_id, forward_msg);
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
                        // No need to send response here as it will be sent
                        // in update_database_and_respond when the command is decided
                    }
                    Err(_) => {
                        warn!("Unable to append read command to log for linearizable read");
                        self.network.send_to_coordinator(ShardMessage::Answer(client_id, command_id, Some("Unable to do linearizable read".to_string()))); 
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

