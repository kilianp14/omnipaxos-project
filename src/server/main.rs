use crate::{configs::OmniPaxosSqlConfig, server::OmniPaxosServer, database::Database};
use env_logger;
use std::sync::Arc;
use tokio::signal;

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

    let base_url = "postgres://user:password@localhost:5432/postgres"; // Base DB URL
    let db = Arc::new(Database::new(base_url).await);

    let mut server = OmniPaxosServer::new(server_config, Arc::clone(&db)).await;

    // Spawn server in a separate task
    let server_task = tokio::spawn(async move {
        server.run().await;
    });

    // Listen for Ctrl + C
    tokio::select! {
        _ = signal::ctrl_c() => {
            println!("Shutting down server...");

            // Cleanup the temporary database before exiting
            db.cleanup(base_url).await;
            println!("Temporary database removed.");
        },
        _ = server_task => {}
    }

    println!("Server stopped.");
}
    