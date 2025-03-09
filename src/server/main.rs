use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer, network::Network, server::Shard};
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

    let network = Arc::new(Mutex::new(Network::new(server_config.clone(), NETWORK_BATCH_SIZE).await));

    let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
    let database = Arc::new(Database::new(base_url).await);


    let shard = Arc::new(Mutex::new(Shard::new(server_config.clone(), database).await));
    let server = OmniPaxosServer::new(server_config.clone(), network.clone(), shard.clone()).await;

    // server.run().await;

    // let server_clone = server.clone();
    // let server_task = tokio::spawn(async move {
    //     // Acquire the lock to get mutable access
    //     let mut guard = server_clone.lock().await;
    //     guard.run().await;
    // });

    let test = server.clone();
    let mut server_task = test.lock().await;

  
    join!(server_task.run(), async {
        let mut shard_guard = shard.lock().await;
        shard_guard.run().await
    });
}
