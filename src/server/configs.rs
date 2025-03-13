use std::env;

use config::{Config, ConfigError, Environment, File};
use omnipaxos::{
    util::FlexibleQuorum,
    ClusterConfig as OmnipaxosClusterConfig, OmniPaxosConfig,
    ServerConfig as OmnipaxosServerConfig,
};
use crate::common::sql::{NodeId, ShardId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeId>,
    pub node_addrs: Vec<String>,
    pub initial_leader: NodeId,
    pub initial_flexible_quorum: Option<FlexibleQuorum>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CoordinatorConfig {
    pub location: Option<String>,
    pub server_id: NodeId,
    pub shard_id: ShardId,
    pub num_clients: usize,
    pub listen_address: String,
    pub shards: Vec<ShardId>,
    pub shard_addrs: Vec<String>,
    pub listen_port: u16,
    pub output_filepath: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ShardConfig {
    pub location: Option<String>,
    pub server_id: NodeId,
    pub shard_id: ShardId,
    pub listen_address: String,
    pub listen_port: u16,
    pub output_filepath: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosCoordinatorConfig {
    #[serde(flatten)]
    pub local: CoordinatorConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OmniPaxosShardConfig {
    #[serde(flatten)]
    pub local: ShardConfig,
    #[serde(flatten)]
    pub cluster: ClusterConfig,
}

impl Into<OmniPaxosConfig> for OmniPaxosCoordinatorConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: self.cluster.initial_flexible_quorum,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.local.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl OmniPaxosCoordinatorConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        config.try_deserialize()
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.cluster
            .nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}

impl Into<OmniPaxosConfig> for OmniPaxosShardConfig {
    fn into(self) -> OmniPaxosConfig {
        let cluster_config = OmnipaxosClusterConfig {
            configuration_id: 1,
            nodes: self.cluster.nodes,
            flexible_quorum: self.cluster.initial_flexible_quorum,
        };
        let server_config = OmnipaxosServerConfig {
            pid: self.local.server_id,
            ..Default::default()
        };
        OmniPaxosConfig {
            cluster_config,
            server_config,
        }
    }
}

impl OmniPaxosShardConfig {
    pub fn new() -> Result<Self, ConfigError> {
        let local_config_file = match env::var("SERVER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires SERVER_CONFIG_FILE environment variable to be set"),
        };
        let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
            Ok(file_path) => file_path,
            Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
        };
        let config = Config::builder()
            .add_source(File::with_name(&local_config_file))
            .add_source(File::with_name(&cluster_config_file))
            // Add-in/overwrite settings with environment variables (with a prefix of OMNIPAXOS)
            .add_source(
                Environment::with_prefix("OMNIPAXOS")
                    .try_parsing(true)
                    .list_separator(",")
                    .with_list_parse_key("node_addrs"),
            )
            .build()?;
        config.try_deserialize()
    }

    pub fn get_peers(&self, node: NodeId) -> Vec<NodeId> {
        self.cluster
            .nodes
            .iter()
            .cloned()
            .filter(|&id| id != node)
            .collect()
    }
}