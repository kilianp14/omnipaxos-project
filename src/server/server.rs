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
use async_trait::async_trait;

type OmniPaxosInstance = OmniPaxos<Command, MemoryStorage<Command>>;
const NETWORK_BATCH_SIZE: usize = 100;
const LEADER_WAIT: Duration = Duration::from_secs(1);
const ELECTION_TIMEOUT: Duration = Duration::from_secs(1);
const SHARD_TIMEOUT: Duration = Duration::from_secs(1);


// Define a callback trait with an async function.
#[async_trait]
pub trait ServerCallback: Send + Sync {
    async fn read_cluster_msgs(&self, msg_buffer: &mut Vec<(NodeId, ClusterMessage)>);
    async fn send_to_cluster2(&self, sender_Id: NodeId, msg: ClusterMessage);
    async fn send_to_client(&self, client_id: ClientId, msg: ServerMessage);
    async fn ack_from_shard(&self, command_id: CommandId);
}

#[async_trait]
impl ServerCallback for tokio::sync::Mutex<OmniPaxosServer> {
    async fn read_cluster_msgs(&self, msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
        let guard = self.lock().await;
        guard.read_cluster_msgs(msg_buffer).await;
        for (from, message) in msg_buffer.iter() {
            info!("Received cluster2 message from {}: {:?}", from, message);
        }
        if msg_buffer.is_empty() {
            warn!("No cluster2 messages received");
        }
    }
    async fn send_to_cluster2(&self, sender_Id: NodeId, msg: ClusterMessage) {
        let guard = self.lock().await;
        guard.send_to_cluster2(sender_Id, msg).await;

    }
    async fn send_to_client(&self, client_id: ClientId, msg: ServerMessage) {
        let guard = self.lock().await;
        let mut net = guard.network.lock().await;
        net.send_to_client(client_id, msg);
    }
    async fn ack_from_shard(&self, command_id: CommandId) {
        let mut guard = self.lock().await;
        guard.ack_from_shard(command_id);
    }
}


pub struct OmniPaxosServer {
    id: NodeId,
    network: Arc<tokio::sync::Mutex<Network>>,
    shard1: Arc<tokio::sync::Mutex<Shard>>,
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
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosSqlConfig, network: Arc<tokio::sync::Mutex<Network>>, shard1: Arc<tokio::sync::Mutex<Shard>>) -> Arc<tokio::sync::Mutex<OmniPaxosServer>> {
        // Initialize first OmniPaxos instance
        let storage: MemoryStorage<Command> = MemoryStorage::default();
        let omnipaxos_config: OmniPaxosConfig = config.clone().into();
        let omnipaxos_msg_buffer = Vec::with_capacity(omnipaxos_config.server_config.buffer_size);
        let omnipaxos = omnipaxos_config.build(storage).unwrap();

        // // Initialize second OmniPaxos instance (separate from the first one)
        // let storage2: MemoryStorage<Command> = MemoryStorage::default();
        // let omnipaxos_config2: OmniPaxosConfig = config.clone().into();
        // let omnipaxos_msg_buffer2 = Vec::with_capacity(omnipaxos_config2.server_config.buffer_size);
        // let omnipaxos2 = omnipaxos_config2.clone().build(storage2).unwrap();

        let pending_transactions = Vec::new();

        let server = Arc::new(Mutex::new(OmniPaxosServer {
            id: config.local.server_id,
            network,
            shard1,
            // database,
            omnipaxos,
            omnipaxos_msg_buffer,
            // omnipaxos2,
            // omnipaxos_msg_buffer2,
            current_decided_idx: 0,
            // current_decided_shard_idx: 0,
            peers: config.get_peers(config.local.server_id),
            config,
            pending_transactions,
        }));
        {
            let server_clone = server.clone();
            let mut server_guard = server.lock().await;
            let mut shard_guard = server_guard.shard1.lock().await;
            shard_guard.set_callback(server_clone);
        }
        server
    }

    pub async fn run(&mut self) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");

        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // let mut cluster2_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // We don't use Omnipaxos leader election at first and instead force a specific initial leader
        // self.establish_initial_leader(&mut cluster_msg_buf, &mut client_msg_buf).await;
        // Main event loop with leader election
        let mut election_interval = tokio::time::interval(ELECTION_TIMEOUT);
        let mut shardTimeoutInterval = tokio::time::interval(SHARD_TIMEOUT);
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    self.omnipaxos.tick();
                    self.send_outgoing_msgs().await;
                    // self.omnipaxos2.tick();
                    // self.send_outgoing_msgs2().await;
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_cluster_messages(&mut cluster_msg_buf).await;
                },
                // _ = async {
                //     self.network.cluster2_messages.recv_many(&mut cluster2_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_cluster2_messages(&mut cluster2_msg_buf).await;
                // },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_client_messages(&mut client_msg_buf).await;
                },
                _ = shardTimeoutInterval.tick() => {
                    self.check_pending_transactions().await;
                }
            }
        }
    }

    async fn read_cluster_msgs(&self, msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
        let mut network = self.network.lock().await;
        network.cluster2_messages.recv_many(msg_buffer, NETWORK_BATCH_SIZE).await;
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
                    let mut network = self.network.lock().await;
                    network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
                _ = async {
                    let mut network = self.network.lock().await;
                    network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    self.handle_client_messages(client_msg_buffer).await;
                },
            }
        }
        
        // // get the shards leader
        // loop {
        //     tokio::select! {
        //         _ = leader_takeover_interval2.tick(), if self.config.cluster.initial_leader == self.id => {
        //             if let Some((curr_leader, is_accept_phase)) = self.omnipaxos2.get_current_leader(){
        //                 if curr_leader == self.id && is_accept_phase {
        //                     info!("{}: Leader shard fully initialized", self.id);
        //                     let experiment_sync_start = (Utc::now() + Duration::from_secs(2)).timestamp_millis();
        //                     self.send_cluster2_start_signals(experiment_sync_start).await;
        //                     self.send_client_start_signals(experiment_sync_start).await;
        //                     break;
        //                 }
        //             }
        //             info!("{}: Attempting to take leadership for shard", self.id);
        //             self.omnipaxos2.try_become_leader();
        //             self.send_outgoing_msgs2().await;
        //         },
        //         _ = async {
        //             self.network.cluster2_messages.recv_many(cluster2_msg_buffer, NETWORK_BATCH_SIZE).await
        //         } => {
        //             let recv_start = self.handle_cluster2_messages(cluster2_msg_buffer).await;
        //             if recv_start {
        //                 break;
        //             }
        //         },
        //     }
        // }
    }

    // coordinator
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
            match message {
                ClientMessage::Handle(command_id, sql_command) => match sql_command.query_type {
                    _ => self.append_to_log(from, command_id, sql_command),
                },
            }
        }
        self.send_outgoing_msgs().await;
    }

    // coordaintor 
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
            self.process_decided_entries(decided_commands).await;
        }
    }

    // coordinator
    async fn process_decided_entries(&mut self, commands: Vec<Command>) {
        for mut command in commands {
            if matches!(command.phase, Some(Phase::Prepare)) || command.phase.is_none() {
                if command.coordinator_id == self.id {
                    // TODO: For using multiple shard this needs to be split accoring to shards keyranges and send to the respective shards. Also put as many false entries in the vector as we use shard in this transaction.
                    info!("{}: push command {} to pending", self.id, command.id);
                    if !matches!(command.sql_cmd.query_type, QueryType::Select) {
                        self.pending_transactions.push((Utc::now().timestamp_millis(), command.id, command.client_id, vec![false]));
                    }
                    info!("{}: Pending array: {:?}", self.id, self.pending_transactions);
                    command.phase = Some(Phase::Prepare);
                    self.shard1.lock().await.send_prepare_to_shard(command.clone()).await; // "sends" message to the shard to process
                }
            }
            else if let Some(Phase::Commit) = command.phase {
                info!("{}: Committing command {}", self.id, command.id);
                self.shard1.lock().await.commit_or_abort_on_shard(command).await;
            } else if let Some(Phase::Abort) = command.phase {
                info!("{}: Aborting command {}", self.id, command.id);
                self.shard1.lock().await.commit_or_abort_on_shard(command).await;
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
    fn ack_from_shard(&mut self, command_id: CommandId) {
        // Find the transaction with the given command_id
        let transaction = self.pending_transactions.iter_mut()
            .find(|(_, id, _, _)| *id == command_id)
            .expect(&format!("Transaction with id {} not found", command_id));

        // Find the first unacknowledged entry and mark it as acknowledged
        if let Some(ack) = transaction.3.iter_mut().find(|ack| !**ack) {
            *ack = true;
            info!("{}: Acknowledged command with id {}", self.id, command_id);
        } else {
            panic!("No unacknowledged entry found for transaction with id {}", command_id);
        }
    }

    async fn send_to_cluster2(&self, sender_Id: NodeId, msg: ClusterMessage) {
        info!("Sending cluster2 message from {}: {:?}", sender_Id, msg);
        self.network.lock().await.send_to_cluster2(sender_Id, msg);
    }

    async fn check_pending_transactions(&mut self) {
        let dummy_sql_command = SqlCommand {
            query_type: QueryType::Insert,
            table: "dummy".to_string(),
            columns: vec![("dummy".to_string(), "dummy".to_string())],
            consistency: Some(Consistency::Linearizable),
            conditions: Some(String::new()),
            values: Some(vec![]),
        };      // A dummy command as we only need the transaction id in sql to commit or rollback the command. Therefore this command is never used.

        let completed_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)> = self.pending_transactions
            .iter()
            .filter(|(_, _, _, acks)| acks.iter().all(|&ack| ack))
            .cloned()
            .collect();

        self.pending_transactions
            .retain(|(_, _, _, acks)| !acks.iter().all(|&ack| ack));

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
            self.append_commit_abort_to_log(command).await
            // TODO: Collect and merge results from shards and send back to client.
        }

        let now = Utc::now().timestamp_millis();
        let threshold = now - 5_000;
        let timedout_transactions: Vec<(Timestamp, CommandId, ClientId, Vec<bool>)> = self
            .pending_transactions
            .iter()
            .filter(|(ts, _, _, _)| *ts <= threshold)
            .cloned()
            .collect();

        self.pending_transactions.retain(|(ts, _, _,  _)| *ts > threshold);

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
            self.append_commit_abort_to_log(command).await
        }
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

// ############### SHARD ###############


pub struct Shard {
    id: NodeId,
    // network: Network,
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
    callback: Option<Arc<dyn ServerCallback>>,
}
    

impl Shard {
    pub async fn new(config: OmniPaxosSqlConfig, database: Arc<Database>) -> Self {
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
            // network,
            database,
            // omnipaxos,
            // omnipaxos_msg_buffer,
            omnipaxos2,
            omnipaxos_msg_buffer2,
            // current_decided_idx: 0,
            current_decided_shard_idx: 0,
            peers: config.get_peers(config.local.server_id),
            config,
            callback: None,
        }
    }

    pub fn set_callback(&mut self, callback: Arc<dyn ServerCallback>) {
        self.callback = Some(callback);
    }

    pub async fn run(&mut self) {
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
        loop {
            tokio::select! {
                _ = election_interval.tick() => {
                    // self.omnipaxos.tick();
                    // self.send_outgoing_msgs().await;
                    self.omnipaxos2.tick();
                    self.send_outgoing_msgs2().await;
                },
                // _ = async {
                //     self.network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_cluster_messages(&mut cluster_msg_buf).await;
                // },
                _ = async {
                    if let Some(callback) = &self.callback {
                        callback.read_cluster_msgs(&mut cluster2_msg_buf).await;
                    }
                    // self.network.cluster2_messages.recv_many(&mut cluster2_msg_buf, NETWORK_BATCH_SIZE).await
                } => {
                   
                    self.handle_cluster2_messages(&mut cluster2_msg_buf).await;
                },
                // _ = async {
                //     self.network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_client_messages(&mut client_msg_buf).await;
                // },
                // _ = shardTimeoutInterval.tick() => {
                //     self.check_pending_transactions().await;
                // }
            }
        }
    }


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
                    if let Some(callback) = &self.callback {
                        callback.read_cluster_msgs(cluster2_msg_buffer).await;
                    }
                    // self.network.cluster2_messages.recv_many(cluster2_msg_buffer, NETWORK_BATCH_SIZE).await
                } => {
                    let recv_start = self.handle_cluster2_messages(cluster2_msg_buffer).await;
                    if recv_start {
                        break;
                    }
                },
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
                    if let Some(callback) = &self.callback {
                        callback.send_to_cluster2(sender_id, msg).await;
                    }
                    // self.network.send_to_cluster2(sender_id, msg);
                }
                ClusterMessage::ReadResponse(client_id, command_id, response) => {
                    let msg = ServerMessage::Answer(command_id, response);
                    if let Some(callback) = &self.callback {
                        callback.send_to_client(client_id, msg).await;
                    }
                    // self.network.send_to_client(client_id, msg);
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
                        if let Some(callback) = &self.callback {
                            callback.ack_from_shard(command.id).await;
                        }
                        // self.ack_from_shard(command.id);
                    }
                    _ => {}
                }
                let msg = ServerMessage::Answer(command.id, response);
                if let Some(callback) = &self.callback {
                    callback.send_to_client(command.client_id, msg).await;
                }
                // self.network.send_to_client(command.client_id, msg);
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
            if let Some(callback) = &self.callback {
                callback.send_to_cluster2(to, cluster_msg).await;
            }
            // self.network.send_to_cluster2(to, cluster_msg);
        }
    }


    async fn send_cluster2_start_signals(&mut self, start_time: Timestamp) {
        for peer in &self.peers {
            debug!("Sending start message to peer {peer}");
            let msg = ClusterMessage::LeaderStartSignal(start_time);
            if let Some(callback) = &self.callback {
                callback.send_to_cluster2(*peer, msg).await;
            }
            // self.network.send_to_cluster2(*peer, msg);
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

    async fn commit_or_abort_on_shard(&mut self, command: Command) {
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
                if let Some(callback) = &self.callback {
                    callback.send_to_client(client_id, msg).await;
                }
                // self.network.send_to_client(client_id, msg);
                // self.ack_from_shard(command_id);
            }
            Consistency::Leader => {
                if let Some((leader_id, is_accept_phase)) = self.omnipaxos2.get_current_leader() {
                    if leader_id == self.id && is_accept_phase {
                        // We are the leader, process locally
                        let response = self.database.prepare_command(sql_command, command_id).await;
                        let msg = ServerMessage::Answer(command_id, response);
                        if let Some(callback) = &self.callback {
                            callback.send_to_client(client_id, msg).await;
                        }
                        // self.network.send_to_client(client_id, msg);
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
                        if let Some(callback) = &self.callback {
                            callback.send_to_cluster2(leader_id, forward_msg).await;
                        }
                        // self.network.send_to_cluster2(leader_id, forward_msg);
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
                        if let Some(callback) = &self.callback {
                            callback.send_to_client(client_id, msg).await;
                        }
                        // self.network.send_to_client(client_id, msg);
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

