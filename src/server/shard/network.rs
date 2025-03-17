use futures::{SinkExt, StreamExt};
use async_trait::async_trait;
use log::*;
use omnipaxos_sql::common::{
    messages::*,
    sql::NodeId,
    utils::*,
};
use std::{net::{SocketAddr, ToSocketAddrs}, time::Duration, str::FromStr};
use tokio::{select, sync::mpsc::{Sender, UnboundedSender}};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};

use omnipaxos_sql::server::configs::OmniPaxosShardConfig;

#[async_trait]
pub trait NetworkTrait {
    fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage);
    fn send_to_coordinator(&mut self, msg: ShardMessage);
    async fn recv_many(
        &mut self,
        cluster_msg_buf: &mut Vec<(NodeId, ClusterMessage)>,
        coordinator_msg_buf: &mut Vec<CoordinatorMessage>,
        batch_size: usize,
    );
}

pub struct Network {
    peers: Vec<NodeId>,
    peer_connections: Vec<Option<PeerConnection>>,
    coordinator_connection: Option<CoordinatorConnection>,
    batch_size: usize,
    coordinator_message_sender: Sender<CoordinatorMessage>,
    cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
    cluster_messages: Receiver<(NodeId, ClusterMessage)>,
    coordinator_messages: Receiver<CoordinatorMessage>,
}

fn get_peer_addrs(config: OmniPaxosShardConfig) -> (SocketAddr, Vec<SocketAddr>) {
    let listen_address_str = format!(
        "{}:{}",
        config.local.listen_address, config.local.listen_port
    );
    let listen_address = SocketAddr::from_str(&listen_address_str).expect(&format!(
        "{listen_address_str} is an invalid listen address"
    ));
    let node_addresses: Vec<SocketAddr> = config
        .cluster
        .node_addrs
        .into_iter()
        .map(|addr_str| match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(e) => panic!("Address {addr_str} is invalid: {e}"),
        })
        .collect();
    (listen_address, node_addresses)
}


#[async_trait]
impl NetworkTrait for Network {
    fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        match self.cluster_id_to_idx(to) {
            Some(idx) => match &mut self.peer_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to peer {to}: {err}");
                        self.peer_connections[idx] = None;
                    }
                }
                None => warn!("Not connected to node {to}"),
            },
            None => error!("Sending to unexpected node {to}"),
        }
    }

    fn send_to_coordinator(&mut self, msg: ShardMessage) {
        match &mut self.coordinator_connection {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to coordinator: {err}");
                    self.coordinator_connection = None;
                }
            }
            None => warn!("Not connected to coordinator"),
        }
    }

    async fn recv_many(
        &mut self,
        cluster_msg_buf: &mut Vec<(NodeId, ClusterMessage)>,
        coordinator_msg_buf: &mut Vec<CoordinatorMessage>,
        batch_size: usize,
    ) {
        let mut timeout_interval = tokio::time::interval(Duration::from_millis(5));

        while cluster_msg_buf.len() < batch_size || coordinator_msg_buf.len() < batch_size {
            select! {
                Some(msg) = self.cluster_messages.recv(), if cluster_msg_buf.len() < batch_size => {
                    cluster_msg_buf.push(msg);
                }
                Some(msg) = self.coordinator_messages.recv(), if coordinator_msg_buf.len() < batch_size => {
                    coordinator_msg_buf.push(msg);
                }
                _ = timeout_interval.tick() => break,
            }
        }
    }


}

impl Network {
    pub async fn new(config: OmniPaxosShardConfig, batch_size: usize) -> Self {
        let (listen_address, node_addresses) = get_peer_addrs(config.clone());
        let id = config.local.server_id;
        let peer_addresses: Vec<(NodeId, SocketAddr)> = config
            .cluster
            .nodes
            .into_iter()
            .zip(node_addresses.into_iter())
            .filter(|(node_id, _addr)| *node_id != id)
            .collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peer_addresses.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (coordinator_message_sender, coordinator_messages) = tokio::sync::mpsc::channel(batch_size);
        let mut network = Self {
            peers: peer_addresses.iter().map(|(id, _)| *id).collect(),
            peer_connections: cluster_connections,
            coordinator_connection: None,
            batch_size,
            coordinator_message_sender,
            cluster_message_sender,
            cluster_messages,
            coordinator_messages,
        };
        network
            .initialize_connections(id, peer_addresses, listen_address)
            .await;
        network
    }

    async fn initialize_connections(
        &mut self,
        id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
        listen_address: SocketAddr,
    ) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle =
            self.spawn_connection_listener(connection_sink.clone(), listen_address);
        self.spawn_peer_connectors(connection_sink.clone(), id, peers);
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    self.peer_connections[peer_idx] = Some(connection);
                }
                NewConnection::ToCoordinator(connection) => {
                    self.coordinator_connection = Some(connection);
                }
            }
            let coordinator_connected = self.coordinator_connection.is_some();
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if coordinator_connected && all_cluster_connected {
                listener_handle.abort();
                break;
            }
        }
    }

    fn spawn_connection_listener(
        &self,
        connection_sender: Sender<NewConnection>,
        listen_address: SocketAddr,
    ) -> tokio::task::JoinHandle<()> {
        let coordinator_sender = self.coordinator_message_sender.clone();
        let cluster_sender = self.cluster_message_sender.clone();
        let batch_size = self.batch_size;
        tokio::spawn(async move {
            let listener = TcpListener::bind(listen_address).await.unwrap();
            loop {
                match listener.accept().await {
                    Ok((tcp_stream, socket_addr)) => {
                        info!("New connection from {socket_addr}");
                        tcp_stream.set_nodelay(true).unwrap();
                        tokio::spawn(Self::handle_incoming_connection(
                            tcp_stream,
                            coordinator_sender.clone(),
                            cluster_sender.clone(),
                            connection_sender.clone(),
                            batch_size,
                        ));
                    }
                    Err(e) => error!("Error listening for new connection: {:?}", e),
                }
            }
        })
    }

    async fn handle_incoming_connection(
        connection: TcpStream,
        coordinator_message_sender: Sender<CoordinatorMessage>,
        cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
        connection_sender: Sender<NewConnection>,
        batch_size: usize,
    ) {
        // Identify connector's ID and type by handshake
        let mut registration_connection = frame_registration_connection(connection);
        let registration_message = registration_connection.next().await;
        let new_connection = match registration_message {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Identified connection from node {node_id}");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToPeer(PeerConnection::new(
                    node_id,
                    underlying_stream,
                    batch_size,
                    cluster_message_sender,
                ))
            }
            Some(Ok(RegistrationMessage::CoordinatorRegister)) => {
                info!("Identified connection from coordinator");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToCoordinator(CoordinatorConnection::new(
                    underlying_stream,
                    batch_size,
                    coordinator_message_sender,
                ))
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                info!("Direct connection to Client dropped");
                return;
            }
            Some(Err(err)) => {
                error!("Error deserializing handshake: {:?}", err);
                return;
            }
            None => {
                info!("Connection to unidentified source dropped");
                return;
            }
        };
        connection_sender.send(new_connection).await.unwrap();
    }

    fn spawn_peer_connectors(
        &self,
        connection_sender: Sender<NewConnection>,
        my_id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
    ) {
        let peers_to_connect_to = peers.into_iter().filter(|(peer_id, _)| *peer_id < my_id);
        for (peer, peer_address) in peers_to_connect_to {
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let cluster_sender = self.cluster_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            tokio::spawn(async move {
                // Establish connection
                let peer_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(peer_address).await {
                        Ok(connection) => {
                            info!("New connection to node {peer}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to shard {peer}, {peer_address} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(peer_connection);
                let handshake = RegistrationMessage::NodeRegister(my_id);
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {peer}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let peer_actor =
                    PeerConnection::new(peer, underlying_stream, batch_size, cluster_sender);
                let new_connection = NewConnection::ToPeer(peer_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }


    // Removes all client and peer connections and ends their corresponding tasks.
    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        if let Some(connection) = self.coordinator_connection.take() {
            connection.close();
        }

        for peer_connection in self.peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.close();
            }
        }
        for _ in 0..self.peers.len() {
            self.peer_connections.push(None);
        }
    }

    #[inline]
    fn cluster_id_to_idx(&self, id: NodeId) -> Option<usize> {
        self.peers.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToCoordinator(CoordinatorConnection),
}

struct PeerConnection {
    peer_id: NodeId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ClusterMessage>,
}

impl PeerConnection {
    pub fn new(
        peer_id: NodeId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(NodeId, ClusterMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_cluster_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((peer_id, m)).await {
                                break;
                            };
                        }
                        Err(err) => {
                            error!("Error deserializing message: {:?}", err);
                        }
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to node {peer_id}: {err}");
                        break;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to node {peer_id}: {err}");
                    break;
                }
            }
            info!("Connection to node {peer_id} closed");
        });
        PeerConnection {
            peer_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ClusterMessage,
    ) -> Result<(), mpsc::error::SendError<ClusterMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}


struct CoordinatorConnection {
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ShardMessage>,
}

impl CoordinatorConnection {
    pub fn new(
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<CoordinatorMessage>,
    ) -> Self {
        let (reader, mut writer) = frame_shard_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => incoming_messages.send(m).await.unwrap(),
                        Err(err) => error!("Error deserializing message: {:?}", err),
                    }
                }
            }
        });
        // Writer Actor
        let (message_tx, mut message_rx) = mpsc::unbounded_channel();
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to coordinator: {err}");
                        error!("Killing connection to coordinator");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to coordinator: {err}");
                    error!("Killing connection to coordinator");
                    return;
                }
            }
        });
        CoordinatorConnection {
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ShardMessage,
    ) -> Result<(), mpsc::error::SendError<ShardMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}
