use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer, network::Network, network_test::TestNetwork};
use log::*;
use env_logger;
use network::NetworkTrait;
use std::sync::Arc;
use tokio::sync::Mutex;

mod configs;
mod database;
mod network;
mod server;
mod network_test;

const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosSqlConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
    let db = Arc::new(Database::new(base_url).await);
    let network: Arc<Mutex<Box<dyn NetworkTrait>>> = if std::env::var("TESTING") == Result::Ok("TRUE".to_string()) {
        info!("Running with a test network");
        Arc::new(Mutex::new(Box::new(TestNetwork::new(server_config.clone(), NETWORK_BATCH_SIZE).await)))
    } else {
        Arc::new(Mutex::new(Box::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await)))
    };

    let mut server = OmniPaxosServer::new(server_config, db, network).await;
    server.run().await;
}
