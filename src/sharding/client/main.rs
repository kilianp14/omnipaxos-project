use client::Client;
use configs::ClientConfig;
use core::panic;
use env_logger;

mod client;
mod configs;
mod data_collection;
mod network;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let client_config = match ClientConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut client = Client::new(client_config).await;
    client.run().await;
}
