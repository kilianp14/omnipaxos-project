use crate::{configs::OmniPaxosSqlConfig, database::Database, network::{self, Network}, shard::Shard};
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
use std::thread;
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

#[derive(Clone)]
pub enum MediatorMessage {
    AckFromShard(usize),
    PrepareFromServer(Command),
    CommitOrAbortFromServer(Command),
}

#[derive(Clone)]
pub struct Mediator {
    shard_tx: Sender<MediatorMessage>,
    server_tx: Sender<MediatorMessage>,
}

impl Mediator {
    pub fn new(shard_tx: Sender<MediatorMessage>, server_tx: Sender<MediatorMessage>) -> Self {
        Self { shard_tx, server_tx }
    }

    // Dispatch functions if needed
    fn send_prepare_to_shard(&self,  cmd: Command) {
        let _ = self.shard_tx.send(MediatorMessage::PrepareFromServer(cmd));
    }

    fn commit_or_abort_on_shard(&self,  cmd: Command) {
        let _ = self.shard_tx.send(MediatorMessage::CommitOrAbortFromServer(cmd));
    }

    pub fn ack_from_shard(&self,  cmd_id: usize) {
        let _ = self.server_tx.send(MediatorMessage::AckFromShard(cmd_id));
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
    mediator: Mediator,
}

impl OmniPaxosServer {
    pub async fn new(config: OmniPaxosSqlConfig, network: Arc<tokio::sync::Mutex<Network>>, mediator:Mediator) -> Self {
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

        OmniPaxosServer {
            id: config.local.server_id,
            network,
            // shard1,
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
            mediator,
        }
        // {
        //     let server_clone = server.clone();
        //     let mut server_guard = server.lock().await;
        //     let mut shard_guard = server_guard.shard1.lock().await;
        //     shard_guard.set_callback(server_clone);
        // }
        // server
    }

    pub async fn run(&mut self, rx: Receiver<MediatorMessage>) {
        // Save config to output file
        self.save_output().expect("Failed to write to file");

        let mut client_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        let mut cluster_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
        // let mut cluster2_msg_buf = Vec::with_capacity(NETWORK_BATCH_SIZE);
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
                    // self.omnipaxos2.tick();
                    // self.send_outgoing_msgs2().await;
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
                // _ = async {
                //     let mut network = self.network.lock().await;
                //     network.cluster_messages.recv_many(&mut cluster_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_cluster_messages(&mut cluster_msg_buf).await;
                // },
                // _ = async {
                //     self.network.cluster2_messages.recv_many(&mut cluster2_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_cluster2_messages(&mut cluster2_msg_buf).await;
                // },
                // _ = async {
                //     let mut network = self.network.lock().await;
                //     network.client_messages.recv_many(&mut client_msg_buf, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_client_messages(&mut client_msg_buf).await;
                // },
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
                        }
                    }
                }
            }
        }
    }

    // pub fn run_mpsc(&self, rx: Receiver<MediatorMessage>) {
    //     thread::spawn(move || {
    //         for message in rx {
    //             if let MediatorMessage::AckFromShard(cmd) = message {
    //                 self.pending_transactions.iter_mut()
    //                     .find(|(_, id, _, _)| *id == cmd)
    //                     .expect(&format!("Transaction with id {} not found", cmd))
    //                     .3.iter_mut()
    //                     .find(|ack| !**ack)
    //                     .expect("No unacknowledged entry found");
    //             }
    //         }
    //     });
    // }

    // async fn read_cluster_msgs(&self, msg_buffer: &mut Vec<(NodeId, ClusterMessage)>) {
    //     let mut network = self.network.lock().await;
    //     network.cluster2_messages.recv_many(msg_buffer, NETWORK_BATCH_SIZE).await;
    // }

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
                // _ = async {
                //     let mut network = self.network.lock().await;
                //     network.cluster_messages.recv_many(cluster_msg_buffer, NETWORK_BATCH_SIZE).await
                // } => {
                //     let recv_start = self.handle_cluster_messages(cluster_msg_buffer).await;
                //     if recv_start {
                //         break;
                //     }
                // },
                // _ = async {
                //     let mut network = self.network.lock().await;
                //     network.client_messages.recv_many(client_msg_buffer, NETWORK_BATCH_SIZE).await
                // } => {
                //     self.handle_client_messages(client_msg_buffer).await;
                // },
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
            info!("{}: Received {message:?} from client {from}", self.id);
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
                    // self.shard1.lock().await.send_prepare_to_shard(command.clone()).await; // "sends" message to the shard to process
                    self.mediator.send_prepare_to_shard(command.clone()); // "sends" message to the shard to process
                }
            }
            else if let Some(Phase::Commit) = command.phase {
                info!("{}: Committing command {}", self.id, command.id);
                // self.shard1.lock().await.commit_or_abort_on_shard(command).await;
                self.mediator.commit_or_abort_on_shard(command);
            } else if let Some(Phase::Abort) = command.phase {
                info!("{}: Aborting command {}", self.id, command.id);
                // self.shard1.lock().await.commit_or_abort_on_shard(command).await;
                self.mediator.commit_or_abort_on_shard(command);
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
            keys: Some(vec!["dummy".to_string()]),
            values: Some(vec!["dummy".to_string()]),
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
