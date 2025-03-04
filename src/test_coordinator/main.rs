use test_coordinator::TestCoordinator;
use configs::CoordinatorConfig;
use core::panic;
use env_logger;

mod test_coordinator;
mod configs;
mod data_collection;
mod network;

#[tokio::main]
pub async fn main() {
    env_logger::init();
    let client_config = match CoordinatorConfig::new() {
        Ok(parsed_config) => parsed_config,
        Err(e) => panic!("{e}"),
    };
    let mut client = TestCoordinator::new(client_config).await;
    client.run().await;
}
