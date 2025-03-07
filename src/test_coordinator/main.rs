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
use log::*;
use env_logger;

mod network;
const NETWORK_BATCH_SIZE: usize = 100;

const CLUSTER_SIZE: usize = 4;
const LOG_DIR: &str = "./testing_configs/logs";
const CLUSTER_CONFIG_PATH: &str = "./testing_configs/cluster-config.toml";

#[tokio::main]
async fn main() {
    std::env::set_var("RUST_LOG", "info");
    env_logger::init();
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
                error!("Failed to kill process {}: {}", child.id(), e);
            } else {
                if let Err(e) = child.wait() {
                    error!("Failed to wait for process {}: {}", child.id(), e);
                }
            }
        }
        drop_postgres_databases();
        std::process::exit(0);
    }).expect("Failed to set Ctrl+C handler");

    info!("Starting {} servers...", CLUSTER_SIZE);

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

        info!("Started server {} (PID: {})", i, child.id());
        
        children.lock().unwrap().push(child);
    }
    let mut network = Network::new(
        vec![
            (1, "127.0.0.1:10001".to_string()),
            (2, "127.0.0.1:10002".to_string()),
            (3, "127.0.0.1:10003".to_string()),
        ],
        NETWORK_BATCH_SIZE,
    ).await;


    // Wait for correct leader election
    thread::sleep(time::Duration::from_secs(2));
    
    // Send create table command
    info!("Sent create table command with id 1");
    network.send(3, CoordinatorMessage::ClientMessage(ClientMessage::Handle(1, SqlCommand::create_table_cmd()))).await;
    for _ in 0..3 {
        let msg = network.server_messages.recv().await;
        match msg {
            Some(ServerAnswer::Decide(id, decide_msg)) => {
                network.send(id, CoordinatorMessage::ClusterMessage(decide_msg)).await;
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, answer_msg))) => {
                match answer_msg {
                    Some(am) => info!("Got answer to command {}: {}", cmd_id, am),
                    None => {}
                }
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::StartSignal(_))) => info!("Coordinator received start signal"),
            None => error!("Connection closed")
        }
    }

    info!("Test local read:");
    info!("Send write 5 command with id 2");
    network.send(
        3, CoordinatorMessage::ClientMessage(ClientMessage::Handle(3, SqlCommand::insert_cmd("1".to_string(), "5".to_string())))
    ).await;
    let mut temp_messages: Vec<(NodeId, ClusterMessage)> = Vec::with_capacity(10);
    for _ in 0..3 {
        let msg = network.server_messages.recv().await;
        match msg {
            Some(ServerAnswer::Decide(1, decide_msg)) => {
                info!("Got decide to Node 1. Waiting before forwarding it");
                temp_messages.push((1, decide_msg));
            },
            Some(ServerAnswer::Decide(id, decide_msg)) => {
                info!("Forward decide to node {}", id);
                network.send(id, CoordinatorMessage::ClusterMessage(decide_msg)).await;
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, answer_msg))) => {
                match answer_msg {
                    Some(am) => info!("Got answer to command {}: {}", cmd_id, am),
                    None => {}
                }
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::StartSignal(_))) => info!("Coordinator received start signal"),
            None => error!("Connection closed")
        }
    }
    info!("Reading locally from node 2 with id 3");
    network.send(
        2, CoordinatorMessage::ClientMessage(ClientMessage::Handle(3, SqlCommand::select_cmd("1".to_string(), Consistency::Local)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, Some(am)))) = msg {
        info!("Got answer from node 2 with id {}: {}", cmd_id, am);
    }
    info!("Reading locally from node 1 with id 4");
    network.send(
        1, CoordinatorMessage::ClientMessage(ClientMessage::Handle(4, SqlCommand::select_cmd("1".to_string(), Consistency::Local)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, Some(am)))) = msg {
        info!("Got answer from node 1 with id {}: {}", cmd_id, am);
    }
    info!("Sending remaining messages");
    for (id, temp_msg)  in temp_messages {
        network.send(id, CoordinatorMessage::ClusterMessage(temp_msg)).await;
    }
    
    info!("Test Leader read:");
    info!("Send write 10 command with id 5 to non-leader");
    network.send(
        2, CoordinatorMessage::ClientMessage(ClientMessage::Handle(1, SqlCommand::insert_cmd("2".to_string(), "10".to_string())))
    ).await;
    let mut temp_messages: Vec<(NodeId, ClusterMessage)> = Vec::with_capacity(10);
    for _ in 0..3 {
        let msg = network.server_messages.recv().await;
        match msg {
            Some(ServerAnswer::Decide(1, decide_msg)) => {
                info!("Got decide to Node 1. Waiting before forwarding it");
                temp_messages.push((1, decide_msg));
            },
            Some(ServerAnswer::Decide(id, decide_msg)) => {
                info!("Forward decide to node {}", id);
                network.send(id, CoordinatorMessage::ClusterMessage(decide_msg)).await;
            },

            Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, answer_msg))) => {
                match answer_msg {
                    Some(am) => info!("Got answer to command {}: {}", cmd_id, am),
                    None => {}
                }
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::StartSignal(_))) => info!("Coordinator received start signal"),
            None => error!("Connection closed")
        }
    }
    info!("Performing leader read from node 1 with id 6");
    network.send(
        1, CoordinatorMessage::ClientMessage(ClientMessage::Handle(6, SqlCommand::select_cmd("2".to_string(), Consistency::Leader)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, Some(am)))) = msg {
        info!("Got answer from node 1 with id {}: {}", cmd_id, am);
    }
    info!("Sending remaining messages");
    for (id, temp_msg)  in temp_messages {
        network.send(id, CoordinatorMessage::ClusterMessage(temp_msg)).await;
    }


    info!("Test Leader and Linerizable read when disconnected from global leader:");
    info!("Disconnect leader(3) from 1 and 2");
    network.send(3, CoordinatorMessage::Disconnect(1)).await;
    network.send(3, CoordinatorMessage::Disconnect(2)).await;
    thread::sleep(time::Duration::from_secs(30));

    info!("Send write 15 command with id 7 to new leader(2)");
    network.send(
        2, CoordinatorMessage::ClientMessage(ClientMessage::Handle(1, SqlCommand::insert_cmd("3".to_string(), "15".to_string())))
    ).await;
    for _ in 0..2 {
        let msg = network.server_messages.recv().await;
        match msg {
            Some(ServerAnswer::Decide(id, decide_msg)) => {
                info!("Forward decide to node {}", id);
                network.send(id, CoordinatorMessage::ClusterMessage(decide_msg)).await;
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, answer_msg))) => {
                match answer_msg {
                    Some(am) => info!("Got answer to command {}: {}", cmd_id, am),
                    None => {}
                }
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::StartSignal(_))) => info!("Coordinator received start signal"),
            None => error!("Connection closed")
        }
    }
    info!("Doing leader read on old leader(3) with id 8");
    network.send(
        3, CoordinatorMessage::ClientMessage(ClientMessage::Handle(8, SqlCommand::select_cmd("3".to_string(), Consistency::Leader)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, Some(am)))) = msg {
        info!("Got answer from node 3 with id {}: {}", cmd_id, am);
    }
    info!("Doing linearizable read on 1 with id 9");
    network.send(
        1, CoordinatorMessage::ClientMessage(ClientMessage::Handle(9, SqlCommand::select_cmd("3".to_string(), Consistency::Linearizable)))
    ).await;
    for _ in 0..2 {
        let msg = network.server_messages.recv().await;
        match msg {
            Some(ServerAnswer::Decide(id, decide_msg)) => {
                info!("Forward decide to node {}", id);
                network.send(id, CoordinatorMessage::ClusterMessage(decide_msg)).await;
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, answer_msg))) => {
                match answer_msg {
                    Some(am) => info!("Got answer to command {}: {}", cmd_id, am),
                    None => {}
                }
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::StartSignal(_))) => info!("Coordinator received start signal"),
            None => error!("Connection closed")
        }
    }
    info!("Doing linearizable read on old leader(3) with id 10");
    network.send(
        3, CoordinatorMessage::ClientMessage(ClientMessage::Handle(10, SqlCommand::select_cmd("3".to_string(), Consistency::Linearizable)))
    ).await;
    thread::sleep(time::Duration::from_secs(10));
    match network.server_messages.try_recv() {
        Ok(_) => error!("Old leader actually returned an answer. Would be not linearizable"),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => info!("No answer, as node cant perform a linearizable read when disconnected from others"),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => error!("Something went really wrong"),
    }
    info!("Reconnecting node 3");
    network.send(3, CoordinatorMessage::Reconnect(1)).await;
    network.send(3, CoordinatorMessage::Reconnect(2)).await;
    thread::sleep(time::Duration::from_secs(5));
    info!("Retrying linearizable read on node 3 with id 11");
    network.send(
        3, CoordinatorMessage::ClientMessage(ClientMessage::Handle(11, SqlCommand::select_cmd("3".to_string(), Consistency::Linearizable)))
    ).await;
    for _ in 0..3 {
        let msg = network.server_messages.recv().await;
        match msg {
            Some(ServerAnswer::Decide(id, decide_msg)) => {
                info!("Forward decide to node {}", id);
                network.send(id, CoordinatorMessage::ClusterMessage(decide_msg)).await;
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::Answer(cmd_id, answer_msg))) => {
                match answer_msg {
                    Some(am) => info!("Got answer to command {}: {}", cmd_id, am),
                    None => {}
                }
            },
            Some(ServerAnswer::ServerMessage(ServerMessage::StartSignal(_))) => info!("Coordinator received start signal"),
            None => error!("Connection closed")
        }
    }
    
    // Clean up
    network.shutdown();
    let mut processes = children.lock().unwrap();
    for child in processes.iter_mut() {
        if let Err(e) = child.kill() {
            error!("Failed to kill process {}: {}", child.id(), e);
        } else {
            if let Err(e) = child.wait() {
                error!("Failed to wait for process {}: {}", child.id(), e);
            }
        }
    }
    drop_postgres_databases();
    std::process::exit(0);
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
