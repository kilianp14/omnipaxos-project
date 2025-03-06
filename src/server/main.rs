use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer, server::OmniPaxosShard, network::Network};
use env_logger;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use tokio::{net, sync::Mutex};

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
    
    let network = Arc::new(Mutex::new(Box::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await)));

    let shard1 = Arc::new(Mutex::new(OmniPaxosShard::new(server_config.clone(), network.clone()).await));
    let server = Arc::new(Mutex::new(OmniPaxosServer::new(server_config.clone(), network.clone(), Arc::clone(&shard1)).await));
    shard1.lock().await.server = Some(Arc::clone(&server));

    server.lock().await.run().await;
}
