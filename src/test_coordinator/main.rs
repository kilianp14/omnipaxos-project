use crate::network::{Network, CoordinatorMessage, ServerAnswer};
use std::process::{Command, Child};
use std::sync::{Arc, Mutex};
use std::{fs, thread, time};
use std::path::Path;
use ctrlc;
use omnipaxos_sql::common::{
    messages::*,
    sql::*,
};

mod network;
const NETWORK_BATCH_SIZE: usize = 100;

const CLUSTER_SIZE: usize = 3;
const LOG_DIR: &str = "./testing_configs/logs";
const CLUSTER_CONFIG_PATH: &str = "./testing_configs/cluster-config.toml";

#[tokio::main]
async fn main() {
    // Ensure log directory exists
    if !Path::new(LOG_DIR).exists() {
        fs::create_dir(LOG_DIR).expect("Failed to create logs directory");
    }

    let children: Arc<Mutex<Vec<Child>>> = Arc::new(Mutex::new(Vec::new()));

    // Setup signal handler for cleanup
    let children_clone = Arc::clone(&children);
    ctrlc::set_handler(move || {
        println!("\nInterrupt received. Cleaning up...");
        let mut processes = children_clone.lock().unwrap();
        for child in processes.iter_mut() {
            if let Err(e) = child.kill() {
                eprintln!("Failed to kill process {}: {}", child.id(), e);
            } else {
                if let Err(e) = child.wait() {
                    eprintln!("Failed to wait for process {}: {}", child.id(), e);
                }
            }
        }
        drop_postgres_databases();
        std::process::exit(0);
    }).expect("Failed to set Ctrl+C handler");

    println!("Starting {} servers...", CLUSTER_SIZE);

    // Spawn servers
    for i in 1..=CLUSTER_SIZE {
        let server_config_path = format!("./testing_configs/server-{}-config.toml", i);
        let child = Command::new("cargo")
            .args(&["run", "--bin", "server"])
            .env("RUST_LOG", "info")
            .env("SERVER_CONFIG_FILE", &server_config_path)
            .env("CLUSTER_CONFIG_FILE", CLUSTER_CONFIG_PATH)
            .env("TESTING", "TRUE")
            .spawn()
            .expect("Failed to start server process");

        println!("Started server {} (PID: {})", i, child.id());
        
        children.lock().unwrap().push(child);
    }
    let mut network = Network::new(
        vec![(1, "127.0.0.1:10001".to_string()), (2, "127.0.0.1:10002".to_string()), (3, "127.0.0.1:10003".to_string())],
        NETWORK_BATCH_SIZE,
    ).await;

    thread::sleep(time::Duration::from_secs(2));
    network.send(1, CoordinatorMessage::ClientMessage(ClientMessage::Handle(1, SqlCommand::create_table_cmd()))).await;
    //network.send(2, CoordinatorMessage::ClientMessage(ClientMessage::Handle(2, SqlCommand::insert_cmd("2".to_string(), "x".to_string())))).await;
    //thread::sleep(time::Duration::from_secs(2));
    //network.send(3, CoordinatorMessage::ClientMessage(ClientMessage::Handle(3, SqlCommand::insert_cmd("3".to_string(), "y".to_string())))).await;

    let mut message_buffer: Vec<ServerAnswer> = Vec::with_capacity(NETWORK_BATCH_SIZE);
    loop {
        network.server_messages.recv_many(&mut message_buffer, NETWORK_BATCH_SIZE).await;
        for message in message_buffer.drain(..) {
            match message {
                ServerAnswer::Decide(id, msg) => {
                    println!("Got decide to {}", id);
                    network.send(id, CoordinatorMessage::ClusterMessage(msg)).await;
                },
                ServerAnswer::ServerMessage(_) => {
                    println!("Got Answer");
                },
            }
        };
    }
}

// Function to drop temporary PostgreSQL databases
fn drop_postgres_databases() {
    println!("Cleaning up PostgreSQL temporary databases...");
    let output = Command::new("psql")
        .args(&["-U", "postgres", "-d", "postgres", "-t", "-c",
                "SELECT datname FROM pg_database WHERE datname LIKE 'omnipaxos_tempdb%';"])
        .output()
        .expect("Failed to execute psql command");

    let databases = String::from_utf8_lossy(&output.stdout);
    for db in databases.lines() {
        if !db.trim().is_empty() {
            println!("Dropping database: {}", db);
            let _ = Command::new("psql")
                .args(&["-U", "postgres", "-d", "postgres", "-c", &format!("DROP DATABASE \"{}\";", db.trim())])
                .status();
        }
    }
}
