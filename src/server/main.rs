use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer, network::{Network, NetworkTrait}};
use env_logger;
use std::sync::Arc;

mod configs;
mod database;
mod test_network;
mod network;
mod server;

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

    let network = Box::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await);

    let mut server = OmniPaxosServer::new(server_config, db, network).await;
    server.run().await;
}
