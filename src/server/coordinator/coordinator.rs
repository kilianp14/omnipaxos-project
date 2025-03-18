use crate::network::NetworkTrait;
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_sql::common::{messages::*, sql::*, utils::Timestamp};
use omnipaxos_sql::server::configs::OmniPaxosCoordinatorConfig;
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::{fs::File, io::Write, time::Duration, ops::Range, collections::HashMap};

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(5);

pub struct OmniPaxosCoordinator {
    id: NodeId,
    network: Box<dyn NetworkTrait>,
    omnipaxos: OmniPaxosInstance,
    current_decided_idx: usize,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    config: OmniPaxosCoordinatorConfig,
    peers: Vec<NodeId>,
    shard_ranges: Vec<(Range<i64>, ShardId)>,
    // Maps command id to client, involved shards, and received answers
    pending_transactions: HashMap<CommandId, (ClientId, Vec<ShardId>, Vec<String>)>,
    // Maps command id to client, number of expected acks, received answers
    pending_executions: HashMap<CommandId, (ClientId, usize, Vec<Result<String, DatabaseError>>)>,
}

impl OmniPaxosCoordinator {
    pub async fn new(config: OmniPaxosCoordinatorConfig, network: Box<dyn NetworkTrait>) -> Self {
        // Initialize OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let shard_map: Vec<(Range<i64>, ShardId)> = config.clone().local.shard_ranges
            .into_iter()
            .zip(config.clone().local.shards.into_iter()) // Pair each range with a shard
            .map(|((start, end), shard)| (start..end, shard))
            .collect(); 
        let peers = config.clone().get_peers(config.clone().local.server_id);
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();

        OmniPaxosCoordinator {
            id: config.local.server_id,
            network,
            omnipaxos,
            current_decided_idx: 0,
            omnipaxos_msg_buffer,
            config,
            peers: peers,
            shard_ranges: shard_map,
            pending_transactions: HashMap::new(),
            pending_executions: HashMap::new(),
        }
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");
        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut shard_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(
            &mut cluster_msg_buf, &mut client_msg_buf, &mut shard_msg_buf
        ).await;
        info!{"{}: Initial election phase over", self.id};
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs();
                },
                _ = self.network.recv_many(&mut cluster_msg_buf, &mut client_msg_buf, &mut shard_msg_buf, NETWORK_BATCH_SIZE) => {
                    if !cluster_msg_buf.is_empty() {
                        self.handle_cluster_messages(&mut cluster_msg_buf).await;
                    }
                    if !client_msg_buf.is_empty() {
                        self.handle_client_messages(&mut client_msg_buf).await;
                    }
                    if !shard_msg_buf.is_empty() {
                        self.handle_shard_messages(&mut shard_msg_buf).await;
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
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
        shard_msg_buffer: &mut Vec<(ShardId, ShardMessage)>,
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
                            self.send_client_start_signals(experiment_sync_start).await;
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs();
                },
                _ = self.network.recv_many(cluster_msg_buffer, client_msg_buffer, shard_msg_buffer, NETWORK_BATCH_SIZE) => {
                    if !cluster_msg_buffer.is_empty() {
                        let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                        if recv_start {
                            break;
                        }
                    }
                    if !client_msg_buffer.is_empty() {
                        self.handle_client_messages(client_msg_buffer).await;
                    }
                    if !shard_msg_buffer.is_empty() {
                        self.handle_shard_messages(shard_msg_buffer).await;
                    }
                },
            }
        }
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
            self.process_decided_entries(decided_commands);
        }
    }

    fn process_decided_entries(&mut self, commands: Vec<Command>) {
        // iterate over all commands that we are the coordinator for
        for command in commands{
            match command {
                Command::Prepare(command_id, client_id, coordinator_id, sql_command) => {
                    let shard_sql_commands: Vec<(ShardId, SqlCommand)>;
                    if matches!(sql_command.query_type, QueryType::Create) {
                        shard_sql_commands = self.config.local.shards.clone().into_iter().map(|n| (n, sql_command.clone())).collect();
                    }
                    else {
                        shard_sql_commands = self.split_by_shard(sql_command);
                    }
                    if shard_sql_commands.is_empty() {
                        self.network.send_to_client(client_id, ServerMessage::Answer(command_id.clone(), "All keys invalid".to_string()));
                        continue;
                    }
                    let shards_in_transaction = shard_sql_commands.clone().into_iter().map(|(shard_id, _)| shard_id).collect();
                    self.pending_transactions.insert(command_id.clone(), (client_id, shards_in_transaction, Vec::with_capacity(shard_sql_commands.len())));
                    if coordinator_id == self.id {
                        for (shard_id, sql_command) in shard_sql_commands {
                            self.network.send_to_shard(shard_id, CoordinatorMessage{command: Command::Prepare(command_id.clone(), client_id, coordinator_id, sql_command)});
                        }
                    }
                },
                Command::Commit(command_id, coordinator_id) => {
                    if let Some((client_id, involved_shards, answers)) = self.pending_transactions.get(&command_id.clone()) {
                        if coordinator_id == self.id {
                            for shard_id in involved_shards {
                                self.network.send_to_shard(*shard_id, CoordinatorMessage{command: Command::Commit(command_id.clone(), coordinator_id)});
                            }
                            self.network.send_to_client(*client_id, ServerMessage::Answer(command_id.clone(), answers.join(";")));
                        }
                        self.pending_transactions.remove(&command_id);
                    }
                    else {
                        error!("Command to be committed that is not in pending transactions");
                        continue;
                    }
                },
                Command::Abort(command_id, coordinator_id) => {
                    if coordinator_id == self.id {
                        if let Some((_, involved_shards, _)) = self.pending_transactions.get(&command_id.clone()) {
                            for shard_id in involved_shards {
                                self.network.send_to_shard(*shard_id, CoordinatorMessage{command: Command::Abort(command_id.clone(), coordinator_id)});
                            }
                        }
                    }
                    self.pending_transactions.remove(&command_id);
                },
                Command::Ack(command_id, coordinator_id, answer) => {
                    debug!("Ack to {}: {}", command_id, answer);
                    if let Some((_, involved_shards, answers)) = self.pending_transactions.get_mut(&command_id) {
                        answers.push(answer);
                        if answers.len() == involved_shards.len() && self.id == coordinator_id {
                            debug!("Command can be committed");
                            self.omnipaxos
                                .append(Command::Commit(command_id, self.id))
                                .expect("Append to Omnipaxos log failed");
                        }
                    }
                }
                Command::Nack(command_id, coordinator_id) => {
                    warn!("Command has to be aborted: {}", command_id);
                    if let Some((_, _, _)) = self.pending_transactions.get(&command_id) {
                        if self.id == coordinator_id {
                            self.omnipaxos
                                .append(Command::Abort(command_id, self.id))
                                .expect("Append to Omnipaxos log failed");
                        }
                    }
                }
                _ => {
                    error!("Invalid command type in coordinator log");
                }
            };
        }
    }

    fn send_outgoing_msgs(&mut self) {
        self.omnipaxos
            .take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.send_to_cluster(to, cluster_msg);
        }
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            match message {
                ClientMessage::Handle(command_id, sql_command) => {
                    // For leader and local reads, we dont have to start a transaction
                    if matches!(sql_command.query_type, QueryType::Select) && matches!(sql_command.consistency, Some(Consistency::Leader) | Some(Consistency::Local)) {
                        let shard_sql_commands = self.split_by_shard(sql_command);
                        if shard_sql_commands.is_empty() {
                            self.network.send_to_client(from,  ServerMessage::Answer(command_id.clone(), "All keys invalid".to_string()));
                            continue;
                        }
                        self.pending_executions.insert(command_id.clone(), (from, shard_sql_commands.len(), Vec::with_capacity(shard_sql_commands.len())));
                        for (shard_id, shard_sql) in shard_sql_commands {
                            self.network.send_to_shard(shard_id, CoordinatorMessage{command: Command::Execute(command_id.clone(), shard_sql)});
                        }
                    }
                    else {
                        self.omnipaxos
                            .append(Command::Prepare(command_id, from, self.id, sql_command))
                            .expect("Append to Omnipaxos log failed");
                    }
                },
            }
        }
        self.send_outgoing_msgs();
    }

    async fn handle_cluster_messages(&mut self, messages: &mut Vec<(NodeId, ClusterMessage)>) -> bool {
        let mut received_start_signal = false;
        for (from, message) in messages.drain(..) {
            trace!("{}: Received {message:?}", self.id);
            match message {
                ClusterMessage::OmniPaxosMessage(m) => {
                    self.omnipaxos.handle_incoming(m);
                    self.handle_decided_entries().await;
                }
                ClusterMessage::LeaderStartSignal(start_time) => {
                    debug!("Received start message from peer {from}");
                    received_start_signal = true;
                    self.send_client_start_signals(start_time).await;
                }
                _ => {warn!("Weird message received from cluster")}
            }
        }
        self.send_outgoing_msgs();
        received_start_signal
    }

    async fn handle_shard_messages(&mut self, messages: &mut Vec<(ShardId, ShardMessage)>) {
        for (from, message) in messages.drain(..){
            match message {
                ShardMessage::Ack(command_id, res) => {
                    debug!("Coordinator received acknowledgement from shard {}", from);
                    self.omnipaxos
                        .append(Command::Ack(command_id, from, res))
                        .expect("Append to omnipaxos log failed")
                }
                ShardMessage::Nack(command_id) => {
                    self.omnipaxos
                        .append(Command::Nack(command_id, from))
                        .expect("Append to omnipaxos log failed")
                }
                ShardMessage::Answer(command_id, result) => {
                    let execution = self.pending_executions.get_mut(&command_id);
                    match execution {
                        Some((client_id, expected_acks, results)) => {
                            results.push(result);
                            if results.len() == *expected_acks {
                                let mut answers: Vec<String> = Vec::with_capacity(*expected_acks);
                                for result in results {
                                    match result {
                                        Ok(str) => answers.push(str.clone()),
                                        Err(err) => {
                                            answers.push(format!("Internal Error: {}", err.message));
                                        }
                                    }
                                }
                                self.network.send_to_client(*client_id, ServerMessage::Answer(command_id.clone(), answers.join(";")));
                                self.pending_executions.remove(&command_id);
                            }
                        }
                        None => error!("Answer from shard to a non-pending execution")
                    } 
                }
            }
        }
    }

    fn send_cluster_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.send_to_cluster(*peer, msg);
        }
    }

    async fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.send_to_client(client_id, msg);
        }
    }

    fn split_by_shard(&self, sql_command: SqlCommand) -> Vec<(ShardId, SqlCommand)> {
        if let Some(all_keys) = sql_command.keys.clone() {
            match sql_command.values.clone() {
                None => {
                    let mut shard_map: HashMap<ShardId, Vec<i64>> = HashMap::new();
                    for key in all_keys {
                        if let Some(shard_id) = self.find_shard_for_key(key) {
                            shard_map
                                .entry(*shard_id)
                                .or_insert_with(|| Vec::new())
                                .push(key.clone());
                        }
                    }
                    shard_map
                        .into_iter()
                        .map(|(shard_id, keys)| {
                            (
                                shard_id,
                                SqlCommand {
                                    query_type: sql_command.query_type.clone(),
                                    table: sql_command.table.clone(),
                                    columns: sql_command.columns.clone(),
                                    keys: Some(keys),
                                    values: None,
                                    consistency: sql_command.consistency.clone(),
                                },
                            )
                        })
                        .collect()
                }
                Some(all_values) => {
                    let mut shard_map: HashMap<ShardId, (Vec<i64>, Vec<Vec<String>>)> = HashMap::new();
                    for (key, value) in all_keys.into_iter().zip(all_values.into_iter()) {
                        if let Some(shard_id) = self.find_shard_for_key(key) {
                            shard_map
                                .entry(*shard_id)
                                .or_insert_with(|| (Vec::new(), Vec::new()))
                                .0
                                .push(key.clone());
                            shard_map.get_mut(&shard_id).unwrap().1.push(value.clone());
                        }
                    }
            
                    shard_map
                        .into_iter()
                        .map(|(shard_id, (keys, values))| {
                            (
                                shard_id,
                                SqlCommand {
                                    query_type: sql_command.query_type.clone(),
                                    table: sql_command.table.clone(),
                                    columns: sql_command.columns.clone(),
                                    keys: Some(keys),
                                    values: Some(values),
                                    consistency: sql_command.consistency.clone(),
                                },
                            )
                        })
                        .collect()
                }
            }
        }
        else {
            return Vec::new();
        }
    }

    fn find_shard_for_key(&self, key: i64) -> Option<&ShardId> {
        self.shard_ranges.iter().find(|(range, _)| range.contains(&key)).map(|(_, shard)| shard)
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }
}
