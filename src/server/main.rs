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
    let database = Arc::new(Database::new(base_url).await);

    let (tx_shard, rx_shard) = mpsc::channel();
    let (tx_server, rx_server) = mpsc::channel();

    let mediator = Mediator::new(tx_shard.clone(), tx_server.clone());

    let mut shard = Shard::new(server_config.clone(), database, network.clone(), mediator.clone()).await;
    let mut server = OmniPaxosServer::new(server_config.clone(), network.clone(),mediator).await;

    // shard.run_mpsc(rx_shard);
    // server.run_mpsc(rx_server);

    // server.run().await;

    let server_task = tokio::spawn(async move {
        server.run(rx_server).await;
    });

    let shard_task = tokio::spawn(async move {
        shard.run(rx_shard).await;
    });

    tokio::join!(server_task, shard_task);
    

    // let test = server.clone();
    // let mut server_task = test.lock().await;

  
    // join!(server_task.run(), async {
    //     let mut shard_guard = shard.lock().await;
    //     shard_guard.run().await
    // });
}
