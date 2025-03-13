use omnipaxos_sql::server::configs::OmniPaxosShardConfig;
use crate::{database::Database, shard::OmniPaxosShard};
use env_logger;
use std::sync::Arc;

mod database;
mod network;
mod shard;

const NETWORK_BATCH_SIZE: usize = 100;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosShardConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
    let db = Arc::new(Database::new(base_url).await);
    let mut shard = OmniPaxosShard::new(server_config, db).await;
    shard.run().await;
}
