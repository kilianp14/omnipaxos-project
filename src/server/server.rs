use crate::{configs::OmniPaxosSqlConfig, network::{self, Network}};
use chrono::Utc;
use log::*;
use omnipaxos::{
    messages::Message,
    util::{LogEntry, NodeId},
    OmniPaxos, OmniPaxosConfig,
};
use omnipaxos_sql::common::{messages::*, sql::*, utils::Timestamp};
use omnipaxos_storage::memory_storage::MemoryStorage;
use std::sync::mpsc::{self, Sender, Receiver};
use std::sync::Arc;
use std::{fs::File, io::Write, time::Duration};


type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const SHARD_TIMEOUT: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub enum MediatorMessage {
    AckFromShard(usize),
    PrepareFromServer(Command),
    CommitOrAbortFromServer(Command),
    ResponseFromShard(ClusterMessage),
}

#[derive(Clone)]
pub struct Mediator {
    shard1_tx: Sender<MediatorMessage>,
    shard2_tx: Sender<MediatorMessage>,
    server_tx: Sender<MediatorMessage>,
}

impl Mediator {
    pub fn new(shard1_tx: Sender<MediatorMessage>, shard2_tx: Sender<MediatorMessage>, server_tx: Sender<MediatorMessage>) -> Self {
        Self { shard1_tx, shard2_tx, server_tx }
    }

    // Dispatch functions
    fn send_prepare_to_shard1(&self,  cmd: Command) {
        let _ = self.shard1_tx.send(MediatorMessage::PrepareFromServer(cmd));
    }

    fn commit_or_abort_on_shard1(&self,  cmd: Command) {
        let _ = self.shard1_tx.send(MediatorMessage::CommitOrAbortFromServer(cmd));
    }

    fn send_prepare_to_shard2(&self,  cmd: Command) {
        let _ = self.shard2_tx.send(MediatorMessage::PrepareFromServer(cmd));
    }

    fn commit_or_abort_on_shard2(&self,  cmd: Command) {
        let _ = self.shard2_tx.send(MediatorMessage::CommitOrAbortFromServer(cmd));
    }

    pub fn ack_from_shard(&self,  cmd_id: usize) {
        let _ = self.server_tx.send(MediatorMessage::AckFromShard(cmd_id));
    }

    pub fn response_from_shard(&self, response:ClusterMessage) {
        let _ = self.server_tx.send(MediatorMessage::ResponseFromShard(response));
    }
}


pub struct OmniPaxosServer {
    id: NodeId,
    network: Arc<tokio::sync::Mutex<Network>>,
    // shard1: Shard,
    // database: Arc<Database>,
    omnipaxos: OmniPaxosInstance,
    omnipaxos_msg_buffer: Vec<Message<Command>>,
    // New second omnipaxos instance and its message buffer
    // omnipaxos2: OmniPaxosInstance,
    // omnipaxos_msg_buffer2: Vec<Message<Command>>,
    current_decided_idx: usize,
    // current_decided_shard_idx: usize,
    config: OmniPaxosSqlConfig,
    peers: Vec<NodeId>,
    pending_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)>,
    pending_read_results: Vec<(CommandId, ClientId, Vec<String>,i32)>,
    mediator: Mediator,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosSqlConfig, network: Arc<tokio::sync::Mutex<Network>>, mediator:Mediator) -> Self {
        // Initialize first OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();

        let pending_transactions = Vec::new();
        let pending_read_results = Vec::new();


        OmniPaxosServer {
            id: config.local.server_id,
            network,
            omnipaxos,
            omnipaxos_msg_buffer,
            current_decided_idx: 0,
            peers: config.get_peers(config.local.server_id),
            config,
            pending_transactions,
            pending_read_results,
            mediator,
        }
    }

    pub async fn run(&mut self, rx: Receiver<MediatorMessage>) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");

        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf).await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let mut shardTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);
        let mut mpscTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);

        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs().await;
                },
                _ = async {
                    let mut net = self.network.lock().await;
                    net.recv_many(&mut cluster_msg_buf, &mut client_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    if !cluster_msg_buf.is_empty() {
                        self.handle_cluster_messages(&mut cluster_msg_buf).await;
                    }
                    if !client_msg_buf.is_empty() {
                        self.handle_client_messages(&mut client_msg_buf).await;
                    }
                },
                _ = shardTimeoutInterval.tick() => {
                    self.check_pending_transactions().await;
                },
                _ = mpscTimeoutInterval.tick() => {
                    let mut messages = Vec::new();
                    while let Ok(message) = rx.try_recv() {
                        messages.push(message);
                    }
                    for message in messages {
                        if let MediatorMessage::AckFromShard(command_id) = message {
                            self.ack_from_shard(command_id);
                        } else if let MediatorMessage::ResponseFromShard(msg) = message {
                            self.process_response_from_shard(msg);
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
        client_msg_buffer: &mut Vec<(ClientId, ClientMessage)>,
    ) {
        let mut leader_takeover_interval = tokio::time::interval(LEADER_WAIT);
        // let mut leader_takeover_interval2 = tokio::time::interval(LEADER_WAIT);
        loop {
            tokio::select! {
                _ = leader_takeover_interval.tick(), if self.config.cluster.initial_leader == self.id => {
                    if let Some((curr_leader, is_accept_phase)) = self.omnipaxos.get_current_leader(){
                        if curr_leader == self.id && is_accept_phase {
                            info!("{}: Leader coodinator fully initialized", self.id);
                            let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
                            self.send_cluster_start_signals(experiment_sync_start).await;
                            self.send_client_start_signals(experiment_sync_start).await;
                            break;
                        }
                    }
                    info!("{}: Attempting to take leadership for coordinator", self.id);
                    self.omnipaxos.try_become_leader();
                    self.send_outgoing_msgs().await;
                },
                _ = async {
                    let mut net = self.network.lock().await;
                    net.recv_many(cluster_msg_buffer, client_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    if !cluster_msg_buffer.is_empty() {
                        let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                        if recv_start {
                            break;
                        }
                    }
                    if !client_msg_buffer.is_empty() {
                        self.handle_client_messages(client_msg_buffer).await;
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
            trace!("{}: Received {message:?} for {}", self.id, 0);
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
            ClusterMessage::ReadResponse(client_id, coordinator_id, command_id, response) => {      // this is guaranted to be a response comming from a shard that is the result of a read query. The coordinstor doesnt use ReadResponse messages otherwise.
                info!("{}: Received read response from shard for command {}", self.id, command_id);
                if let Some((_, _, responses, _)) = self.pending_read_results.iter_mut().find(|(id, _,  _, _)| *id == command_id) {
                    responses.push(response.unwrap_or_default());
                }
            }
            other => {
                debug!(
                "{}: !!! RECEIVED UNHANDLED CLUSTER MESSAGE: {:?}",
                self.id, other
                );
            }
            }
        }
        self.send_outgoing_msgs().await;
        received_start_signal
    }

    async fn handle_client_messages(&mut self, messages: &mut Vec<(ClientId, ClientMessage)>) {
        for (from, message) in messages.drain(..) {
            info!("{}: Received {message:?} from client {from}", self.id);
            match message {
                ClientMessage::Handle(command_id, sql_command) => match sql_command.query_type {
                    _ => self.append_to_log(from, command_id, sql_command),
                },
            }
        }
        self.send_outgoing_msgs().await;
    }

    async fn handle_decided_entries(&mut self) {
        let new_decided_idx: usize = self.omnipaxos.get_decided_idx();
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
            if matches!(command.phase, Some(Phase::Prepare)) || command.phase.is_none() {
                if command.coordinator_id == self.id {
                    let (below_10, above_10): (Vec<String>, Vec<String>) = command.clone().sql_cmd.keys.unwrap_or_default().into_iter()
                        .partition(|key| key.parse::<i32>().unwrap_or(0) < 10);

                    // figre out how we can split the command for the writes.

                    let mut number_of_involed_shards = 0;
                    let mut cmd_below_10 = command.clone();
                    let mut cmd_above_10 = command.clone();
                    info!("{}: Processing command {:?}", self.id, command);
                    if !below_10.is_empty() {
                        cmd_below_10.sql_cmd.keys = Some(below_10.clone());
                        number_of_involed_shards += 1;  
                    }
                    if !above_10.is_empty() {
                        cmd_above_10.sql_cmd.keys = Some(above_10.clone());
                        number_of_involed_shards += 1; 
                    }
                    info!("{}: Processing below command {:?}", self.id, cmd_below_10);
                    info!("{}: push command {} to pending", self.id, command.id);
                    // select tranactions are appended to the vector in order to keep track which shards already send the result back
                    if matches!(command.sql_cmd.query_type, QueryType::Select) {
                        self.pending_read_results.push((command.id, command.client_id, vec![], number_of_involed_shards));
                        match command.sql_cmd.consistency {
                            Some(Consistency::Linearizable) => {
                                // linearizable read get decided so they can abort. whe need to keep track of the acks for that. like other write commands. But since its a read we also need to collect and merge all results
                                self.pending_transactions.push((Utc::now().timestamp_millis(), command.id, command.client_id, vec![false; number_of_involed_shards as usize]));
                            }
                            _ => {}
                        }
                    } else {
                    // Insert and create transactions are appended to this vector, so we can periodically check if the acked and if not send a abort. For select queries we dont need to wait for acks as reading doesnt need to be decided.
                        self.pending_transactions.push((Utc::now().timestamp_millis(), command.id, command.client_id, vec![false]));
                    }
                    info!("{}: Pending array: {:?}", self.id, self.pending_transactions);
                    info!("{}: pending read results: {:?}", self.id, self.pending_read_results);
                    command.phase = Some(Phase::Prepare);
                    if matches!(command.sql_cmd.query_type, QueryType::Create) {
                        // Ceate table hast to be send to all shards
                        self.mediator.send_prepare_to_shard1(command.clone());
                        self.mediator.send_prepare_to_shard2(command.clone());
                    }else{
                        // TODO: reads have to wait until the table is created. otherwise the read will fail.
                        if !below_10.is_empty() { 
                            cmd_below_10.phase = Some(Phase::Prepare);
                            self.mediator.send_prepare_to_shard1(command.clone()); // "sends" message to the shard1 to process
                        }
                        if !above_10.is_empty() {
                            cmd_above_10.phase = Some(Phase::Prepare);
                            self.mediator.send_prepare_to_shard2(command.clone()); // "sends" message to the shard2 to process
                        }
                    }
                }
            } else {
                match command.phase {
                    Some(Phase::Commit) => {
                        info!("{}: Committing command {}", self.id, command.id);
                    }
                    Some(Phase::Abort) => {
                        info!("{}: Aborting command {}", self.id, command.id);
                    }
                    _ => {}
                }
                
                // TODO: an we just sent to all shards here as the abort and commit are ignored in psql if the id is not known?
                // check if the command_id (which is used as the pending transaction id in psql) is unqiue over all shards. If not we cant do this!
                self.mediator.commit_or_abort_on_shard1(command.clone());
                self.mediator.commit_or_abort_on_shard2(command);
            }
        }
    }

    // Sends outgoing messages from the primary omnipaxos instance using the standard cluster channel
    async fn send_outgoing_msgs(&mut self) {
        self.omnipaxos.take_outgoing_messages(&mut self.omnipaxos_msg_buffer);
        for msg in self.omnipaxos_msg_buffer.drain(..) {
            let to = msg.get_receiver();
            let cluster_msg = ClusterMessage::OmniPaxosMessage(msg);
            self.network.lock().await.send_to_cluster(to, cluster_msg);
        }
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
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            self.network.lock().await.send_to_cluster(*peer, msg);
        }
    }

    async fn send_client_start_signals(&mut self, start_time: Timestamp) {
        for client_id in 1..self.config.local.num_clients as ClientId + 1 {
            debug!("Sending start message to client {client_id}");
            let msg = ServerMessage::StartSignal(start_time);
            self.network.lock().await.send_to_client(client_id, msg);
        }
    }

    // For each shard we append the command.id to the vec. The ack removes it again.
    pub fn ack_from_shard(&mut self, command_id: CommandId) {
        // Find the transaction with the given command_id
        if let Some(transaction) = self.pending_transactions.iter_mut()
            .find(|(_, id, _, _)| *id == command_id) {
            // Find the first unacknowledged entry and mark it as acknowledged
            if let Some(ack) = transaction.3.iter_mut().find(|ack| !**ack) {
            *ack = true;
            info!("{}: Acknowledged command with id {}", self.id, command_id);
            } else {
            info!("{}: No unacknowledged entry found for transaction with id {}", self.id, command_id);
            }
        } else {
            info!("{}: Transaction with id {} not found", self.id, command_id);
        }

    }

    pub fn process_response_from_shard(&mut self, msg:ClusterMessage) {
        if let ClusterMessage::ReadResponse(client_id, _, command_id, response) = msg {
            if let Some((_, _, responses, _)) = self.pending_read_results.iter_mut().find(|(id, cli_id, _, _)| *id == command_id && *cli_id == client_id) {
            responses.push(response.unwrap_or_default());
            }
        }
    }

    async fn check_pending_transactions(&mut self) {
        let dummy_sql_command = SqlCommand {
            query_type: QueryType::Insert,
            table: "dummy".to_string(),
            columns: vec![("dummy".to_string(), "dummy".to_string())],
            consistency: Some(Consistency::Linearizable),
            keys: Some(vec!["dummy".to_string()]),
            values: Some(vec!["dummy".to_string()]),
        };      // A dummy command as we only need the transaction id in sql to commit or rollback the command. Therefore this command is never used.
        let now = Utc::now().timestamp_millis();
        let threshold = now - 5_000;

        // CHECK FOR COMMITS
        // completed if all shards acked
        let completed_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)> = self.pending_transactions
            .iter()
            .filter(|(ts, _, _, acks)| acks.iter().all(|&ack| ack))
            .cloned()
            .collect();

        for (_, cmd_id, client_id, _) in completed_transactions {
            let command = Command {
                client_id: client_id,
                coordinator_id: self.id,
                id: cmd_id,
                sql_cmd: dummy_sql_command.clone(),
                phase: Some(Phase::Commit),
            };
            info!("{}: Completed transaction with id {}. Sending off to shard as commit", self.id, cmd_id);
            // apend to the coordaintors log
            self.append_commit_abort_to_log(command).await;
            let msg = ServerMessage::Answer(cmd_id, Some("Write Transaction successfully commited!".to_string()));
            self.network.lock().await.send_to_client(client_id, msg);
        }

        self.pending_transactions
            .retain(|(_, _, _, acks)| !acks.iter().all(|&ack| ack));
        
        // CHECK FOR ABORTS

        let timedout_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)> = self
            .pending_transactions
            .iter()
            .filter(|(ts, _, _, _)| *ts <= threshold)
            .cloned()
            .collect();

        for (_, cmd_id, client_id, _) in timedout_transactions {
            let command = Command {
                client_id: client_id,
                coordinator_id: self.id,
                id: cmd_id,
                sql_cmd: dummy_sql_command.clone(),
                phase: Some(Phase::Commit),
            };
            info!("{}: Timed out transaction with id {}. Sending off to shard as abort", self.id, cmd_id);
            // apend to the coordaintors log
            self.append_commit_abort_to_log(command).await;
            let msg = ServerMessage::Answer(cmd_id, Some("Transaction aborted!".to_string()));
            self.network.lock().await.send_to_client(client_id, msg);
        }

        self.pending_transactions
            .retain(|(ts, _, _, _)| *ts > threshold);

        // CHECK RESPONSES
        let valid_read_results: Vec<(CommandId, ClientId, Vec<String>, i32)> = self
            .pending_read_results
            .iter()
            .filter(|(_, _, responses, expected_len)| responses.len() == *expected_len as usize)
            .cloned()
            .collect();

        for (cmd_id, client_id, responses, _) in valid_read_results {
            info!("{}: Received read responses: {:?}", self.id, responses);
            // let response_str = responses.join(", ");
            // let msg = ServerMessage::Answer(cmd_id, Some(response_str));
            // self.network.lock().await.send_to_client(client_id, msg);
        }

        self.pending_read_results
            .retain(|(_, _, responses, expected_len)| responses.len() != *expected_len as usize);
    }

    async fn append_commit_abort_to_log(&mut self, command: Command) {
        self.omnipaxos
            .append(command)
            .expect("Append to Omnipaxos log failed");
        self.send_outgoing_msgs().await;
    }

    fn save_output(&mut self) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&self.config)?;
        let mut output_file = File::create(&self.config.local.output_filepath)?;
        output_file.write_all(config_json.as_bytes())?;
        output_file.flush()?;
        Ok(())
    }


}
