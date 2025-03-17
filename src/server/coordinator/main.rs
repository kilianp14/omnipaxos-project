use omnipaxos_sql::server::configs::OmniPaxosCoordinatorConfig;
use crate::{coordinator::OmniPaxosCoordinator, network::Network};
use env_logger;
use log::*;

mod network;
mod coordinator;

const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosCoordinatorConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    info!("Starting up coodinator: {}", server_config.local.server_id);
    let network = Box::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await);
    let mut server = OmniPaxosCoordinator::new(server_config, network).await;
    server.run().await;
}
