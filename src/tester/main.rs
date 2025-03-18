use crate::network::{Network, TesterMessage};
use std::process::{Command, Child};
use std::sync::{Arc, Mutex};
use std::{fs, thread, time, vec};
use std::path::Path;
use ctrlc;
use omnipaxos_sql::common::{
    messages::*,
    sql::*,
};
use log::*;
use env_logger;
use uuid::Uuid;

mod network;
const NETWORK_BATCH_SIZE: usize = 100;

const N_SERVERS: usize = 3;
const N_SHARDS: u64 = 2;
const LOG_DIR: &str = "./testing_configs/logs";

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

    // Spawn servers
    let coordinator_cluster_path = format!("./testing_configs/coordinator-cluster-config.toml");
    for i in 1..=N_SERVERS {
        let coordinator_config_path = format!("./testing_configs/coordinator-{}-config.toml", i);
        let child = Command::new("cargo")
            .args(&["run", "--bin", "coordinator"])
            .env("RUST_LOG", "info")
            .env("SERVER_CONFIG_FILE", &coordinator_config_path)
            .env("CLUSTER_CONFIG_FILE", &coordinator_cluster_path)
            .env("TESTING", "TRUE")
            .spawn()
            .expect("Failed to start server process");
        info!("Started coordinator {} (PID: {})", i, child.id());
        children.lock().unwrap().push(child);

        for j in 1..=N_SHARDS {
            let shard_cluster_path = format!("./testing_configs/shard{}-cluster-config.toml", j);
            let shard_config_path = format!("./testing_configs/shard{}-{}-config.toml", j, i);
            let child = Command::new("cargo")
                .args(&["run", "--bin", "shard"])
                .env("RUST_LOG", "info")
                .env("SERVER_CONFIG_FILE", &shard_config_path)
                .env("CLUSTER_CONFIG_FILE", &shard_cluster_path)
                .env("TESTING", "TRUE")
                .spawn()
                .expect("Failed to start server process");
                info!("Started shard {} with id {} (PID: {})", j, i, child.id());
            children.lock().unwrap().push(child);
        }
        
    }
    let mut network = Network::new(
        vec![
            // Order important!!
            (1, 0, "127.0.0.1:10001".to_string()),
            (1, 1, "127.0.0.1:10004".to_string()),
            (1, 2, "127.0.0.1:10007".to_string()),
            (2, 0, "127.0.0.1:10002".to_string()),
            (2, 1, "127.0.0.1:10005".to_string()),
            (2, 2, "127.0.0.1:10008".to_string()),
            (3, 0, "127.0.0.1:10003".to_string()),
            (3, 1, "127.0.0.1:10006".to_string()),
            (3, 2, "127.0.0.1:10009".to_string()),
        ],
        NETWORK_BATCH_SIZE,
        N_SHARDS,
    ).await;

    // Wait for correct leader election
    thread::sleep(time::Duration::from_secs(2));

    // Send create table command
    let id = Uuid::new_v4().to_string();
    info!("Sent create table command with id {}", id);
    network.send(3, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::create_table_cmd()))).await;
    let msg = network.server_messages.recv().await;
    match msg {
        Some(ServerMessage::Answer(cmd_id, answer_msg)) => {
            info!("Got answer to command {}: {}", cmd_id, answer_msg);
        },
        Some(ServerMessage::StartSignal(_)) => info!("Coordinator received start signal"),
        None => error!("Connection closed")
    }
    thread::sleep(time::Duration::from_secs(2)); 

    info!("Test local read");
    let id = Uuid::new_v4().to_string();
    info!("Disconnect first instance of shard 1 from its RSM");
    network.send(1, 1, TesterMessage::Disconnect(2)).await;
    network.send(1, 1, TesterMessage::Disconnect(3)).await;
    info!("Sent insert command [(1, 4), (101, 5)] with id {}", id);
    network.send(3, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::insert_cmd(vec![1, 101], vec!["4".to_string(), "5".to_string()])))).await;
    let msg = network.server_messages.recv().await;
    match msg {
        Some(ServerMessage::Answer(cmd_id, answer_msg)) => {
            info!("Got answer to command {}: {}", cmd_id, answer_msg);
        },
        Some(ServerMessage::StartSignal(_)) => info!("Coordinator received start signal"),
        None => error!("Connection closed")
    }

    thread::sleep(time::Duration::from_secs(2)); 

    let id = Uuid::new_v4().to_string();
    info!("Reading keys (1, 101) locally from node 2 with id {}", id);
    network.send(
        2, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::select_cmd(vec![1, 101], Consistency::Local)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerMessage::Answer(cmd_id, am)) = msg {
        info!("Got answer from node 2 with id {}: {}", cmd_id, am);
    }

    let id = Uuid::new_v4().to_string();
    info!("Reading keys (1, 101) locally from node 1 with id {}", id);
    network.send(
        1, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::select_cmd(vec![1, 101], Consistency::Local)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerMessage::Answer(cmd_id, am)) = msg {
        info!("Got answer from node 1 with id {}: {}", cmd_id, am);
    }

    network.send(1, 1, TesterMessage::Reconnect(2)).await;
    network.send(1, 1, TesterMessage::Reconnect(3)).await;



    info!("Test leader vs. linearizable");
    let id = Uuid::new_v4().to_string();
    info!("Disconnect entire cluster 3 (all leaders) from their RSMs");
    network.send(3, 0, TesterMessage::Disconnect(1)).await;
    network.send(3, 0, TesterMessage::Disconnect(2)).await;
    network.send(3, 1, TesterMessage::Disconnect(1)).await;
    network.send(3, 1, TesterMessage::Disconnect(2)).await;
    network.send(3, 2, TesterMessage::Disconnect(1)).await;
    network.send(3, 2, TesterMessage::Disconnect(2)).await;

    info!("Wait for leader changes");
    thread::sleep(time::Duration::from_secs(30));

    info!("Sent insert command [(2, 6), (102, 7)] with id {} to new coordinator leader", id);
    network.send(2, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::insert_cmd(vec![2, 102], vec!["6".to_string(), "7".to_string()])))).await;

    let msg = network.server_messages.recv().await;
    if let Some(ServerMessage::Answer(cmd_id, am)) = msg {
        info!("Got answer from node 2 with id {}: {}", cmd_id, am);
    }

    let id = Uuid::new_v4().to_string();
    info!("Doing linearizable read for keys (2, 102) on node 1 with id {}: ", id);
    network.send(
        1, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::select_cmd(vec![2, 102], Consistency::Linearizable)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerMessage::Answer(cmd_id, am)) = msg {
        info!("Got answer from node 1 with id {}: {}", cmd_id, am);
    }

    let id = Uuid::new_v4().to_string();
    info!("Doing leader read for keys (2, 102) on old leader(3) with id {}: ", id);
    network.send(
        3, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::select_cmd(vec![2, 102], Consistency::Leader)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerMessage::Answer(cmd_id, am)) = msg {
        info!("Got answer from node 3 with id {}: {}", cmd_id, am);
    }

    let id = Uuid::new_v4().to_string();
    info!("Doing linearizable read for keys (2, 102) on old leader(3) with id {}: ", id);
    network.send(
        3, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::select_cmd(vec![2, 102], Consistency::Linearizable)))
    ).await;
    thread::sleep(time::Duration::from_secs(10));
    match network.server_messages.try_recv() {
        Ok(_) => error!("Old leader actually returned an answer. Would be not linearizable"),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => info!("No answer, as node cant perform a linearizable read when disconnected from others"),
        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => error!("Something went really wrong"),
    }

    info!("Reconnecting node 3");
    network.send(3, 0, TesterMessage::Reconnect(1)).await;
    network.send(3, 0, TesterMessage::Reconnect(2)).await;
    network.send(3, 1, TesterMessage::Reconnect(1)).await;
    network.send(3, 1, TesterMessage::Reconnect(2)).await;
    network.send(3, 2, TesterMessage::Reconnect(1)).await;
    network.send(3, 2, TesterMessage::Reconnect(2)).await;

    thread::sleep(time::Duration::from_secs(10));

    let id = Uuid::new_v4().to_string();
    info!("Retrying linearizable read for keys (2, 102) on old leader(3) with id {}: ", id);
    network.send(
        3, 0, TesterMessage::ClientMessage(ClientMessage::Handle(id, SqlCommand::select_cmd(vec![2, 102], Consistency::Linearizable)))
    ).await;
    let msg = network.server_messages.recv().await;
    if let Some(ServerMessage::Answer(cmd_id, am)) = msg {
        info!("Got answer from node 3 with id {}: {}", cmd_id, am);
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

    // Fetch the list of temporary databases
    let output = Command::new("psql")
        .args(&["-U", "postgres", "-d", "postgres", "-t", "-c",
                "SELECT datname FROM pg_database WHERE datname LIKE 'omnipaxos_tempdb%';"])
        .output()
        .expect("Failed to execute psql command");

    let databases = String::from_utf8_lossy(&output.stdout);

    for db in databases.lines() {
        let db = db.trim();
        if !db.is_empty() {
            let _ = Command::new("psql")
                .args(&["-U", "postgres", "-d", db, "-c",
                        &format!("ROLLBACK;")])
                .status();
            println!("Checking for prepared transactions in: {}", db);

            // Abort any prepared transactions associated with this database
            let tx_output = Command::new("psql")
                .args(&["-U", "postgres", "-d", "postgres", "-t", "-c",
                        &format!("SELECT gid FROM pg_prepared_xacts WHERE database = '{}';", db)])
                .output()
                .expect("Failed to check prepared transactions");

            let transactions = String::from_utf8_lossy(&tx_output.stdout);

            for tx in transactions.lines() {
                let tx = tx.trim();
                if !tx.is_empty() {
                    println!("Aborting transaction: {}", tx);
                    let _ = Command::new("psql")
                        .args(&["-U", "postgres", "-d", db, "-c",
                                &format!("ROLLBACK PREPARED '{}';", tx)])
                        .status();
                }
            }

            // Now we can drop the database
            println!("Dropping database: {}", db);
            let _ = Command::new("psql")
                .args(&["-U", "postgres", "-d", "postgres", "-c", &format!("DROP DATABASE \"{}\";", db)])
                .status();
        }
    }
}