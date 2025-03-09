use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer, network::Network};
use env_logger;
use log::info;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use tokio::{net, sync::Mutex, join};

mod configs;
mod database;
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

    let network = Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await;

    let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
    let database = Arc::new(Database::new(base_url).await);
    let database3 = Arc::new(Database::new(base_url).await);


    let mut server = OmniPaxosServer::new(server_config.clone(), network, database, database3).await;

    server.run().await;
}
