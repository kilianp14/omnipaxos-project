use omnipaxos_sql::server::configs::OmniPaxosCoordinatorConfig;
use crate::{coordinator::OmniPaxosCoordinator, network::{NetworkTrait, Network}, network_test::NetworkTest};
use env_logger;
use log::*;

mod network;
mod network_test;
mod coordinator;

const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosCoordinatorConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e} while parsing coordinator config"),
    };

    info!("Starting up coodinator: {}", server_config.local.server_id);    info!(
        "Starting up coordinator: {}, {}",
        server_config.local.server_id, server_config.local.listen_port
    );
    let network: Box<dyn NetworkTrait> = if std::env::var("TESTING") == Result::Ok("TRUE".to_string()) {
        info!("Running with a test network");
        Box::new(NetworkTest::new(server_config.clone(), NETWORK_BATCH_SIZE).await)
    } else {
        Box::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await)
    };
    let mut server = OmniPaxosCoordinator::new(server_config, network).await;
    server.run().await;
}
