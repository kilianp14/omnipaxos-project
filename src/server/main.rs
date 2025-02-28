use crate::{configs::OmniPaxosSqlConfig, database::Database, server::OmniPaxosServer};
use env_logger;
use std::sync::Arc;

mod configs;
mod database;
mod network;
mod server;

#[tokio::main]
pub async fn main() {
    env_logger::init();

    let server_config = match OmniPaxosSqlConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };

    let base_url = "postgres://postgres@localhost:5432"; // Base DB URL
    let db = Arc::new(Database::new(base_url).await);

    let mut server = OmniPaxosServer::new(server_config, db).await;
    server.run().await;
}
