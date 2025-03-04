use crate::{configs::CoordinatorConfig, data_collection::ClientData, network::Network};
use chrono::Utc;
use log::*;
use omnipaxos_sql::common::{messages::*, sql::*};
use rand::Rng;
use std::time::Duration;
use tokio::time::interval;

const NETWORK_BATCH_SIZE: usize = 100;

pub struct TestCoordinator {
    id: ClientId,
    network: Network,
    client_data: ClientData,
    config: CoordinatorConfig,
    active_server: NodeId,
    final_request_count: Option<usize>,
    next_request_id: usize,
}

impl TestCoordinator {
    pub async fn new(config: CoordinatorConfig) -> Self {
        let server_addresses: Vec<String> = config.server_address.clone()
            .iter()
            .cloned()
            .collect();
        let server_connections: Vec<(NodeId, String)> = config
            .server_id.clone()
            .into_iter()
            .zip(server_addresses.into_iter())
            .collect();
        info!("Server connections: {:?}", server_connections);
        let network = Network::new(
            // vec![(config.server_id, config.server_address.clone())],
            server_connections,
            NETWORK_BATCH_SIZE,
        )
        .await;
    TestCoordinator {
            id: config.client_id,
            network,
            client_data: ClientData::new(),
            active_server: config.client_id,
            config,
            final_request_count: None,
            next_request_id: 0,
        }
    }

    pub async fn run(&mut self) {
        // Wait for server to signal start
        info!("{}: Waiting for start signal from server", self.id);
        match self.network.server_messages.recv().await {
            Some(ServerMessage::StartSignal(start_time)) => {
                Self::wait_until_sync_time(&mut self.config, start_time).await;
            }
            _ => panic!("Error waiting for start signal"),
        }
        self.send_request(QueryType::Create).await;
        
        let requests = self.config.requests.clone();
        let mut req_iter = requests.iter();

        let mut current_interval = tokio::time::interval(req_iter.next().unwrap().get_interval());

        loop {
            tokio::select! {
                _ = current_interval.tick(), if self.final_request_count.is_none() => {
                    
                    //TODO: send request
                    
                    if let Some(next_req) = req_iter.next() {
                        current_interval = tokio::time::interval(next_req.get_interval());
                    }
                },
                Some(msg) = self.network.server_messages.recv() => {
                    self.handle_server_message(msg);
                    if self.run_finished() {
                        break;
                    }
                },
            }
        }

        info!(
            "{}: Client finished: collected {} responses",
            self.id,
            self.client_data.response_count(),
        );
        self.network.shutdown();
        self.save_results().expect("Failed to save results");
    }

    fn handle_server_message(&mut self, msg: ServerMessage) {
        debug!("Recieved {msg:?}");
        match msg {
            ServerMessage::StartSignal(_) => (),
            server_response => {
                let (cmd_id , response)= server_response.command_id();
                self.client_data.new_response(cmd_id, response);
            }
            //TODO: Handle, forward and delay messages from the servers here
        }
    }

    async fn send_request(&mut self, query_type: QueryType) {
        // Prevent subtract overflow
        let prev_key = if self.next_request_id == 0 {
            0
        } else {
            self.next_request_id - 1
        };
        let key = self.next_request_id.to_string();
        let cmd = match query_type {
            QueryType::Create => SqlCommand::create_table_cmd(),
            QueryType::Insert => SqlCommand::insert_cmd(self.id.to_string() , key),
            // It's not very interesting to select a key that doesn't exist, so we'll just select the previous key.
            // TODO use different consistency levels for reads.
            _ => SqlCommand::select_cmd(prev_key.to_string(), Consistency::Leader),
        };
        let request = ClientMessage::Handle(self.next_request_id, cmd.clone());
        debug!("Sending {request:?}");
        self.network.send(self.active_server, request).await;
        self.client_data
            .new_request(cmd, self.next_request_id);
        self.next_request_id += 1;
    }

    fn run_finished(&self) -> bool {
        if let Some(count) = self.final_request_count {
            if self.client_data.request_count() >= count {
                return true;
            }
        }
        false
    }

    // Wait until the scheduled start time to synchronize client starts.
    // If start time has already passed, start immediately.
    async fn wait_until_sync_time(config: &mut CoordinatorConfig, scheduled_start_utc_ms: i64) {
        // // Desync the clients a bit
        // let mut rng = rand::thread_rng();
        // let scheduled_start_utc_ms = scheduled_start_utc_ms + rng.gen_range(1..100);
        let now = Utc::now();
        let milliseconds_until_sync = scheduled_start_utc_ms - now.timestamp_millis();
        config.sync_time = Some(milliseconds_until_sync);
        if milliseconds_until_sync > 0 {
            tokio::time::sleep(Duration::from_millis(milliseconds_until_sync as u64)).await;
        } else {
            warn!("Started after synchronization point!");
        }
    }

    fn save_results(&self) -> Result<(), std::io::Error> {
        self.client_data.save_summary(self.config.clone())?;
        self.client_data
            .to_csv(self.config.output_filepath.clone())?;
        Ok(())
    }
}
