use crate::{coordinator::OmniPaxosCoordinator, network::Network};
use env_logger;
use log::*;
use omnipaxos_sql::server::configs::OmniPaxosCoordinatorConfig;

mod coordinator;
mod network;

const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosCoordinatorConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e} while parsing coordinator config"),
    };

    info!(
        "Starting up coordinator: {}, {}",
        server_config.local.server_id, server_config.local.listen_port
    );
    let network = Box::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await);
    let mut server = OmniPaxosCoordinator::new(server_config, network).await;
    server.run().await;
}
