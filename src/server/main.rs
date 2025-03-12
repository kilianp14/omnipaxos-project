use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer, network::Network, shard::Shard, server::Mediator};
use env_logger;
use log::info;
use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use tokio::{net, sync::Mutex, join};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;

mod configs;
mod database;
mod network;
mod server;
mod shard;

const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosSqlConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    let network = Arc::new(Mutex::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await));

    let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
    let database1 = Arc::new(Database::new(base_url).await);
    let database2 = Arc::new(Database::new(base_url).await);

    let (tx_shard1, rx_shard1) = mpsc::channel();
    let (tx_shard2, rx_shard2) = mpsc::channel();
    let (tx_server, rx_server) = mpsc::channel();

    let mediator = Mediator::new(tx_shard1.clone(), tx_shard2.clone(), tx_server.clone());

    let mut shard1 = Shard::new(server_config.clone(), database1, network.clone(), mediator.clone(),1).await;
    let mut shard2 = Shard::new(server_config.clone(), database2, network.clone(), mediator.clone(),2).await;
    let mut server = OmniPaxosServer::new(server_config.clone(), network.clone(),mediator).await;

    // server.run().await;

    let server_task = tokio::spawn(async move {
        server.run(rx_server).await;
    });

    let shard_task1 = tokio::spawn(async move {
        shard1.run(rx_shard1).await;
    });

    let shard_task2 = tokio::spawn(async move {
        shard2.run(rx_shard2).await;
    });

    tokio::join!(server_task, shard_task1, shard_task2);
}
