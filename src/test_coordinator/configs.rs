use std::{env, time::Duration};

use config::{Config, ConfigError, Environment, File};
use omnipaxos_sql::common::{sql::NodeId, utils::Timestamp};
use serde::{de::value::StringDeserializer, Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CoordinatorConfig {
    pub location: String,
    pub server_id: Vec<NodeId>,
    pub server_address: Vec<String>,
    pub requests: Vec<RequestInterval>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
    pub client_id: NodeId,
}

impl CoordinatorConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file = match env::var("CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                .try_parsing(true)
                .list_separator(",")
                .with_list_parse_key("server_address"),
            )
            // .add_source(Environment::with_prefix("OMNIPAXOS").try_parsing(true))
            .build()?;
        config.try_deserialize()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RequestInterval {
    pub duration_sec: u64,
    pub is_write: bool,
    pub value: String,
    pub to: NodeId
}

impl RequestInterval {
    pub fn get_interval(&self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }
}
