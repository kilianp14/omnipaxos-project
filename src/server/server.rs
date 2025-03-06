use crate::{configs::OmniPaxosSqlConfig, database::Database, network::{self, Network}};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_sql::common::{messages::*, sql::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::sync::Arc;
use std::{fs::File, io::Write, time::Duration};
use tokio::{net, sync::Mutex};
use std::rc::Rc;
use std::cell::RefCell;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);

pub struct OmniPaxosServer {
    id: NodeId,
    network: Arc<Mutex<Box<Network>>>,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosSqlConfig,
    peers: Vec<NodeId>,
    shard1: Option<Arc<Mutex<OmniPaxosShard>>>,
    pending_transactions: Vec<(Timestamp,CommandId,ClientId,Vec<bool>)>,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosSqlConfig, network:Arc<Mutex<Box<Network>>>, shard1:Arc<Mutex<OmniPaxosShard>>) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let pending_transactions = Vec::new();
        // let config = config.clone();
        // let network = network.clone();
        // Waits for client and server network connections to be established
        // let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;

        
         
        OmniPaxosServer {
            id: config.local.server_id,
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
            shard1: Some(shard1),
            pending_transactions,
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");

        if let Some(shard) = self.shard1.take() {
            tokio::spawn(async move {
                shard.lock().await.run().await;
            });
        }

        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf).await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs().await;
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                _ = tokio::time::sleep(Duration::from_secs(1)) => {
                    self.check_pending_transactions().await;
                }
            }
        }
    }

    async fn check_pending_transactions(&mut self) {
        let completed_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)> = self.pending_transactions
            .iter()
            .filter(|(_, _, _, acks)| acks.iter().all(|&ack| ack))
            .cloned()
            .collect();

        self.pending_transactions
            .retain(|(_, _, _, acks)| !acks.iter().all(|&ack| ack));

        for (_, cmd_id, client_id, _) in completed_transactions {
            let dummy_sql_command = SqlCommand {
                query_type: QueryType::Insert,
                table: "dummy".to_string(),
                columns: vec![("dummy".to_string(), "dummy".to_string())],
                consistency: Some(Consistency::Linearizable),
                conditions: Some(String::new()),
                values: Some(vec![]),
            };
            let command = Command {
                client_id: client_id,
                coordinator_id: self.id,
                id: cmd_id,
                sql_cmd: dummy_sql_command,
                phase: Some(Phase::Commit),
            };
            self.shard1.as_ref().unwrap().lock().await.process_message(command.clone()).await;
        }

        let now = Utc::now().timestamp_millis();
        let threshold = now - 10_000;
        let timedout_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)> = self
            .pending_transactions
            .iter()
            .filter(|(ts, _, _, _)| *ts <= threshold)
            .cloned()
            .collect();

        self.pending_transactions.retain(|(ts, _, _,  _)| *ts > threshold);

        for (_, cmd_id, client_id, _) in timedout_transactions {
            let dummy_sql_command = SqlCommand {
                query_type: QueryType::Insert,
                table: "dummy".to_string(),
                columns: vec![("dummy".to_string(), "dummy".to_string())],
                consistency: Some(Consistency::Linearizable),
                conditions: Some(String::new()),
                values: Some(vec![]),
            };
            let command = Command {
                client_id: client_id,
                coordinator_id: self.id,
                id: cmd_id,
                sql_cmd: dummy_sql_command,
                phase: Some(Phase::Commit),
            };
            self.shard1.as_ref().unwrap().lock().await.process_message(command.clone()).await;
        }
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start).await;
                            self.send_client_start_signals(experiment_sync_start).await;
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs().await;
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await} => {
                        let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                        if recv_start {
                            break;
                        }
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE).await} => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
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
            self.process_decided_entries(decided_commands).await;
        }
    }

    async fn process_decided_entries(&mut self, commands: Vec<Command>) {
        for mut command in commands {
            if command.coordinator_id == self.id {

                info!("{}: Processing decided command {}, content: {:?}", self.id, command.id, command.clone());

                command.phase = Some(Phase::Prepare);
                self.shard1.as_ref().unwrap().lock().await.process_message(command.clone()).await;

                if !matches!(command.sql_cmd.query_type, QueryType::Select) {
                    // append false as often as we have shards. will be replaced with true if the shard acked.
                    self.pending_transactions.push((Utc::now().timestamp_millis(), command.id, command.client_id ,vec![false]));
                }

                let msg = ServerMessage::Answer(command.id, Some("THIS IS A TEST RESPONSE".to_string()));
                let mut network = self.network.lock().await;
                network.send_to_client(command.client_id, msg);
            }
        }
    }

    // For each shard we append the command.id to the vec. The ack removes it again. If we have the comand.id in the vec after the timout, then we now a shard failed.
    fn ack_from_shard(&mut self, command_id: CommandId) {
        let mut found = false;
        if let Some(pos) = self.pending_transactions.iter().position(|(_, cmd,_,_)| *cmd == command_id) {
            let pending = &mut self.pending_transactions[pos].3;
            if let Some(idx) = pending.iter().position(|ack| !*ack) {
                pending[idx] = true;
                found = true;
            } else {
                panic!("No False value left for pending transaction with id {}", command_id);
            }
        }
        if !found {
            panic!("Command with id {} not found in pending_transactions", command_id);
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg,0);
            let mut network = self.network.lock().await;
            network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Handle(command_id, sql_command) => match sql_command.query_type {
                    _ => self.append_to_log(from, command_id, sql_command),
                },
            }
        }
        self.send_outgoing_msgs().await;
    }
    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        let mut unprocessed = Vec::new();
        for (from, message) in messages.drain(..) {
            // Extract the source value regardless of the variant.
            let source = match message {
                ClusterMessage::OmniPaxosMessage(_, source) => source,
                ClusterMessage::LeaderStartSignal(_, source) => source,
                ClusterMessage::ReadRequest(_, _, _, _, source) => source,
                ClusterMessage::ReadResponse(_, _, _, source) => source,
            };
            // If source is non-zero (not for the coordinator cluster), keep it in the buffer.
            if source != 0 {
                unprocessed.push((from, message));
                continue;
            }
            trace!("{}: Received {message:?}", self.id);
            match message {
            ClusterMessage::OmniPaxosMessage(m, _) => {
                self.omnipaxos.handle_incoming(m);
                self.handle_decided_entries().await;
            }
            ClusterMessage::LeaderStartSignal(start_time, _) => {
                debug!("Received start message from peer {from}");
                received_start_signal = true;
                self.send_client_start_signals(start_time).await;
            }
            other => {
                debug!(
                "{}: !!! RECEIVED UNHANDLED CLUSTER MESSAGE: {:?}",
                self.id, other
                );
            }
            }
        }
        messages.extend(unprocessed);
        self.send_outgoing_msgs().await;
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, sql_command: SqlCommand) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            sql_cmd: sql_command,
            phase: None,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    async fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time,0);
            let mut network = self.network.lock().await;
            network.send_to_cluster(*peer, msg);
        }
    }

    async fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            let mut network = self.network.lock().await;
            network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}



pub struct OmniPaxosShard {
    id: NodeId,
    network: Arc<Mutex<Box<Network>>>,
    database: Arc<Database>,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosSqlConfig,
    peers: Vec<NodeId>,
    pub server: Option<Arc<Mutex<OmniPaxosServer>>>,
    shard_id: u32,
}

impl OmniPaxosShard {
    pub async fn new(config: OmniPaxosSqlConfig,network: Arc<Mutex<Box<Network>>>) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();
        let shard_id = 1;
        // Waits for client and server network connections to be established
        // let network = Network::new(config.clone(), NETWORK_BATCH_SIZE).await;

        let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
        let database = Arc::new(Database::new(base_url).await);

        OmniPaxosShard {
            id: config.local.server_id,
            network,
            database,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            peers: config.get_peers(config.local.server_id),
            config,
            server:None,
            shard_id
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf)
            .await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs().await;
                },
                _ = async  {
                    let mut network = self.network.lock().await;
                    network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await}   => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                // _ = async {
                //     let mut network = self.network.lock().await;
                //     network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await} => {
                //     self.handle_client_messages(&mut client_msg_buf).await;
                // },
            }
        }
    }


    pub async fn process_message(&mut self, command: Command) {
        let sql_cmd = command.sql_cmd.clone();
        match sql_cmd.query_type {
            QueryType::Select => {
                self.handle_read_message(command.client_id, command.id, sql_cmd).await;

                self.server.as_ref().unwrap().lock().await.ack_from_shard(command.id);
            }
            _ => {
                self.append_to_log(command.client_id, command.id, sql_cmd, command.phase);
            }
        }
        self.send_outgoing_msgs().await;
    }

    // Ensures cluster is connected and initial leader is promoted before returning.
    // Once the leader is established it chooses a synchronization point which the
    // followers relay to their clients to begin the experiment.
    async fn establish_initial_leader(
        &mut self,
        cluster_msg_buffer: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start).await;
                            // self.send_client_start_signals(experiment_sync_start).await;
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs().await;
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await} => {
                        let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                        if recv_start {
                            break;
                        }
                },
                // _ = async {
                //     let mut network = self.network.lock().await;
                //     network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE).await} => {
                //     self.handle_client_messages(client_msg_buffer).await;
                // },
            }
        }
    }

    async fn handle_decided_entries(&mut self) {
        // TODO: Can use a read_raw here to avoid allocation
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
            self.update_database_and_respond(decided_commands).await;
        }
    }

    async fn update_database_and_respond(&mut self, commands: Vec<Command>) {
        // TODO: batching responses possible here (batch at handle_cluster_messages)
        // This todo was already in the repo, dont think we actually need to do batching
        // For now lets just do write-through
        for command in commands {
            self.server.as_ref().unwrap().lock().await.ack_from_shard(command.id);
            let response = match command.phase {
                Some(Phase::Prepare) => self.database.prepare_command(command.sql_cmd, command.id).await,
                Some(Phase::Commit) => self.database.commit_command(command.id).await,
                Some(Phase::Abort) => self.database.abort_command(command.id).await,
                None => None,
            };

            if command.coordinator_id == self.id {
                let msg = ServerMessage::Answer(command.id, response);
                let mut network = self.network.lock().await;
                network.send_to_client(command.client_id, msg);
            }
        }
    }

    async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg,self.shard_id);
            let mut network = self.network.lock().await;
            network.send_to_cluster(to, cluster_msg);
        }
    }

    // async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
    //     for (from, message) in messages.drain(..) {
    //         match message {
    //             ClientMessage::Handle(command_id, sql_command) => match sql_command.query_type {
    //                 QueryType::Select => {
    //                     self.handle_read_message(from, command_id, sql_command)
    //                         .await;
    //                 }
    //                 _ => self.append_to_log(from, command_id, sql_command, None),
    //             },
    //         }
    //     }
    //     self.send_outgoing_msgs().await;
    // }

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
                let mut network = self.network.lock().await;
                network.send_to_client(client_id, msg);
                self.server.as_ref().unwrap().lock().await.ack_from_shard(command_id);
            }
            Consistency::Leader => {
                if let Some((leader_id, is_accept_phase)) = self.omnipaxos.get_current_leader() {
                    if leader_id == self.id && is_accept_phase {
                        // We are the leader, process locally
                        let response = self.database.prepare_command(sql_command, command_id).await;
                        let msg = ServerMessage::Answer(command_id, response);
                        let mut network = self.network.lock().await;
                        network.send_to_client(client_id, msg);
                        self.server.as_ref().unwrap().lock().await.ack_from_shard(command_id);
                    } else {
                        // Forward to leader
                        let forward_msg = ClusterMessage::ReadRequest(
                            client_id,
                            self.id,
                            command_id,
                            sql_command,
                            self.shard_id,
                        );
                        info!("{}: Forwarding read request to leader {}", self.id, leader_id);
                        let mut network = self.network.lock().await;
                        network.send_to_cluster(leader_id, forward_msg);
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
                        //TODO:send abort to coordinator
                        let response = format!("Failed to achieve linearizable read: {:?}", e);
                        let msg = ServerMessage::Answer(command_id, Some(response));
                        let mut network = self.network.lock().await;
                        network.send_to_client(client_id, msg);
                    }
                }
            }
        };
    }

    async fn handle_cluster_messages(
        &mut self,
        messages: &mut Vec<(NodeId, ClusterMessage)>,
    ) -> bool {
        let mut received_start_signal = false;
        let mut unprocessed = Vec::new();
        for (from, message) in messages.drain(..) {
            // Extract the source value regardless of the variant.
            let source = match message {
                ClusterMessage::OmniPaxosMessage(_, source) => source,
                ClusterMessage::LeaderStartSignal(_, source) => source,
                ClusterMessage::ReadRequest(_, _, _, _, source) => source,
                ClusterMessage::ReadResponse(_, _, _, source) => source,
            };
            // If source is non-zero (not for the coordinator cluster), keep it in the buffer.
            if source != 0 {
                unprocessed.push((from, message));
                continue;
            }
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m,_) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time,_) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time).await;
                }
                ClusterMessage::ReadRequest(client_id, sender_id, command_id, sql_command, _) => {
                    let response = self.database.prepare_command(sql_command, command_id).await;
                    let msg = ClusterMessage::ReadResponse(client_id, command_id, response, self.shard_id);
                    let mut network = self.network.lock().await;
                    network.send_to_cluster(sender_id, msg);
                }
                ClusterMessage::ReadResponse(client_id, command_id, response, _) => {
                    let msg = ServerMessage::Answer(command_id, response);
                    let mut network = self.network.lock().await;
                    network.send_to_client(client_id, msg);
                }
            }
        }
        messages.extend(unprocessed);
        self.send_outgoing_msgs().await;
        received_start_signal
    }

    fn append_to_log(&mut self, from: ClientId, command_id: CommandId, sql_command: SqlCommand, phase: Option<Phase>) {
        let command = Command {
            client_id: from,
            coordinator_id: self.id,
            id: command_id,
            sql_cmd: sql_command,
            phase: phase,
        };
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
    }

    async fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time, self.shard_id);
            let mut network = self.network.lock().await;
            network.send_to_cluster(*peer, msg);
        }
    }

    async fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            let mut network = self.network.lock().await;
            network.send_to_client(client_id, msg);
        }
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}
