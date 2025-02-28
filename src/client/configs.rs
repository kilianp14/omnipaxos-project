use std::{env, time::Duration};

use config::{Config, ConfigError, Environment, File};
use omnipaxos_sql::common::{sql::NodeId, utils::Timestamp};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClientConfig {
    pub location: String,
    pub server_id: NodeId,
    pub server_address: String,
    pub requests: Vec<RequestInterval>,
    pub sync_time: Option<Timestamp>,
    pub summary_filepath: String,
    pub output_filepath: String,
}

impl ClientConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let config_file = match env::var("CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(Environment::with_prefix("OMNIPAXOS").try_parsing(true))
            .build()?;
        config.try_deserialize()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub struct RequestInterval {
    pub duration_sec: u64,
    pub requests_per_sec: u64,
    pub read_ratio: f64,
}

impl RequestInterval {
    pub fn get_read_ratio(&self) -> f64 {
        self.read_ratio
    }

    pub fn get_interval_duration(&self) -> Duration {
        Duration::from_secs(self.duration_sec)
    }

    pub fn get_request_delay(&self) -> Duration {
        if self.requests_per_sec == 0 {
            return Duration::from_secs(999999);
        }
        let delay_ms = 1000 / self.requests_per_sec;
        assert!(delay_ms != 0);
        Duration::from_millis(delay_ms)
    }
}
