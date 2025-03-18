use futures::{SinkExt, StreamExt};
use async_trait::async_trait;
use log::*;
use omnipaxos_sql::common::{
    messages::*,
    sql::{ClientId, NodeId, ShardId},
    utils::*,
};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use std::{collections::HashMap, str::FromStr};
use tokio::{select, sync::mpsc::{Sender, UnboundedSender}};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::mpsc::Receiver,
};
use tokio::{sync::mpsc, task::JoinHandle};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use omnipaxos_sql::server::configs::OmniPaxosCoordinatorConfig;
use serde::{Serialize, Deserialize};
use crate::network::NetworkTrait;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TesterMessage {
    ClientMessage(ClientMessage),
    ClusterMessage(ClusterMessage, NodeId),
    Disconnect(NodeId),
    Reconnect(NodeId),
}

pub struct NetworkTest {
    peers: Vec<NodeId>,
    shards: Vec<ShardId>,
    peer_connections: Vec<Option<PeerConnection>>,
    tester_connection: Option<TesterConnection>,
    shard_connections: Vec<Option<ShardConnection>>,
    batch_size: usize,
    tester_message_sender: Sender<TesterMessage>,
    cluster_message_sender: Sender<(NodeId, ClusterMessage)>,
    shard_message_sender: Sender<(ShardId, ShardMessage)>,
    cluster_messages: Receiver<(NodeId, ClusterMessage)>,
    tester_messages: Receiver<TesterMessage>,
    shard_messages: Receiver<(ShardId, ShardMessage)>,
    disconnected: HashMap<NodeId, (Vec<ClusterMessage>, Vec<ClusterMessage>)>,
}

fn get_peer_addrs(config: OmniPaxosCoordinatorConfig) -> (SocketAddr, Vec<SocketAddr>) {
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

fn get_shard_addrs(config: OmniPaxosCoordinatorConfig) -> Vec<SocketAddr> {
    config
        .local
        .shard_addrs
        .into_iter()
        .map(|addr_str| match addr_str.to_socket_addrs() {
            Ok(mut addrs) => addrs.next().unwrap(),
            Err(e) => panic!("Address {addr_str} is invalid: {e}"),
        })
        .collect()
}

#[async_trait]
impl NetworkTrait for NetworkTest {

    fn send_to_cluster(&mut self, to: NodeId, msg: ClusterMessage) {
        if let Some((sending, _)) = self.disconnected.get_mut(&to) {
            sending.push(msg);
        }
        else {
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
    }

    fn send_to_client(&mut self, _: ClientId, msg: ServerMessage) {
        match &mut self.tester_connection {
            Some(connection) => {
                if let Err(err) = connection.send(msg) {
                    warn!("Couldn't send msg to tester: {err}");
                    self.tester_connection = None;
                }
            }
            None => warn!("Not connected to tester"),
        }
    }

    fn send_to_shard(&mut self, to: ShardId, msg: CoordinatorMessage) {
        match self.shard_id_to_idx(to) {
            Some(idx) => match &mut self.shard_connections[idx] {
                Some(ref mut connection) => {
                    if let Err(err) = connection.send(msg) {
                        warn!("Couldn't send msg to shard {to}: {err}");
                        self.shard_connections[idx] = None;
                    }
                }
                None => warn!("Not connected to shard {to}"),
            },
            None => error!("Sending to unexpected shard {to}"),
        }
    }

    async fn recv_many(
        &mut self,
        cluster_msg_buf: &mut Vec<(NodeId, ClusterMessage)>,
        client_msg_buf: &mut Vec<(ClientId, ClientMessage)>,
        shard_msg_buf: &mut Vec<(ShardId, ShardMessage)>,
        batch_size: usize,
    ) {
        let mut timeout_interval = tokio::time::interval(Duration::from_millis(5));

        while cluster_msg_buf.len() < batch_size || client_msg_buf.len() < batch_size {
            select! {
                Some((id, msg)) = self.cluster_messages.recv(), if cluster_msg_buf.len() < batch_size => {
                    if let Some((_, receiving)) = self.disconnected.get_mut(&id){
                        receiving.push(msg);
                    }
                    else {
                        cluster_msg_buf.push((id, msg));
                    }
                }
                Some(msg) = self.shard_messages.recv(), if shard_msg_buf.len() < batch_size => {
                    shard_msg_buf.push(msg);
                }
                Some(msg) = self.tester_messages.recv(), if (client_msg_buf.len() < batch_size && cluster_msg_buf.len() < batch_size && shard_msg_buf.len() < batch_size) => {
                    match msg {
                        TesterMessage::ClusterMessage(cls_msg, id) => {
                            if let Some((_, receiving)) = self.disconnected.get_mut(&id){
                                receiving.push(cls_msg);
                            }
                            else {
                                cluster_msg_buf.push((id, cls_msg));
                            }
                        },
                        TesterMessage::ClientMessage(cli_msg) => {
                            client_msg_buf.push((1, cli_msg));
                        },
                        TesterMessage::Disconnect(node_id) => {
                            self.disconnected.insert(node_id, (Vec::with_capacity(100), Vec::with_capacity(100)));
                        },
                        TesterMessage::Reconnect(node_id) => {
                            if let Some((sending, received)) = self.disconnected.remove(&node_id) {
                                for message in received {
                                    cluster_msg_buf.push((node_id, message));
                                }
                                for message in sending {
                                    self.send_to_cluster(node_id, message);
                                }
                            }
                        },
                    };
                }
                _ = timeout_interval.tick() => break,
            }
        }
    }
}


impl NetworkTest {
    pub async fn new(config: OmniPaxosCoordinatorConfig, batch_size: usize) -> Self {
        let (listen_address, node_addresses) = get_peer_addrs(config.clone());
        let shard_addresses = get_shard_addrs(config.clone());
        let id = config.local.server_id;
        let peer_addresses: Vec<(NodeId, SocketAddr)> = config
            .cluster
            .nodes
            .into_iter()
            .zip(node_addresses.into_iter())
            .filter(|(node_id, _addr)| *node_id != id)
            .collect();
        let shard_addresses: Vec<(ShardId, SocketAddr)> = config
            .local
            .shards
            .into_iter()
            .zip(shard_addresses.into_iter())
            .collect();
        let mut cluster_connections = vec![];
        cluster_connections.resize_with(peer_addresses.len(), Default::default);
        let mut shard_connections = vec![];
        shard_connections.resize_with(shard_addresses.len(), Default::default);
        let (cluster_message_sender, cluster_messages) = tokio::sync::mpsc::channel(batch_size);
        let (shard_message_sender, shard_messages) = tokio::sync::mpsc::channel(batch_size);
        let (tester_message_sender, tester_messages) = tokio::sync::mpsc::channel(batch_size);
        let mut network = Self {
            peers: peer_addresses.iter().map(|(id, _)| *id).collect(),
            shards: shard_addresses.iter().map(|(id, _)| *id).collect(),
            peer_connections: cluster_connections,
            tester_connection: None,
            shard_connections: shard_connections,
            batch_size,
            tester_message_sender,
            cluster_message_sender,
            shard_message_sender,
            cluster_messages,
            tester_messages,
            shard_messages,
            disconnected: HashMap::new(),
        };
        network
            .initialize_connections(id, peer_addresses, shard_addresses, listen_address)
            .await;
        network
    }

    async fn initialize_connections(
        &mut self,
        id: NodeId,
        peers: Vec<(NodeId, SocketAddr)>,
        shards: Vec<(ShardId, SocketAddr)>,
        listen_address: SocketAddr,
    ) {
        let (connection_sink, mut connection_source) = mpsc::channel(30);
        let listener_handle =
            self.spawn_connection_listener(connection_sink.clone(), listen_address);
        self.spawn_peer_connectors(connection_sink.clone(), id, peers);
        self.spawn_shard_connectors(connection_sink.clone(), shards);
        while let Some(new_connection) = connection_source.recv().await {
            match new_connection {
                NewConnection::ToPeer(connection) => {
                    let peer_idx = self.cluster_id_to_idx(connection.peer_id).unwrap();
                    self.peer_connections[peer_idx] = Some(connection);
                }
                NewConnection::ToShard(connection) => {
                    let shard_idx = self.shard_id_to_idx(connection.shard_id).unwrap();
                    self.shard_connections[shard_idx] = Some(connection);
                }
                NewConnection::ToTester(connection) => {
                    self.tester_connection = Some(connection)
                }
            }
            let tester_connected = self.tester_connection.is_some();
            let all_shards_connected = self.shard_connections.iter().all(|c| c.is_some());
            let all_cluster_connected = self.peer_connections.iter().all(|c| c.is_some());
            if tester_connected && all_cluster_connected && all_shards_connected {
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
        let tester_sender = self.tester_message_sender.clone();
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
                            tester_sender.clone(),
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
        tester_message_sender: Sender<TesterMessage>,
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
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                info!("Identified connection from tester");
                let underlying_stream = registration_connection.into_inner().into_inner();
                NewConnection::ToTester(TesterConnection::new(
                    underlying_stream,
                    batch_size,
                    tester_message_sender,
                ))
            }
            Some(Ok(RegistrationMessage::CoordinatorRegister)) => {
                // Handled on the shard side
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
                            error!("Establishing connection to node {peer} failed: {err}")
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

    fn spawn_shard_connectors(
        &self,
        connection_sender: Sender<NewConnection>,
        shards: Vec<(ShardId, SocketAddr)>,
    ) {
        for (shard, shard_address) in shards {
            let reconnect_delay = Duration::from_secs(1);
            let mut reconnect_interval = tokio::time::interval(reconnect_delay);
            let shard_sender = self.shard_message_sender.clone();
            let connection_sender = connection_sender.clone();
            let batch_size = self.batch_size;
            tokio::spawn(async move {
                // Establish connection
                let shard_connection = loop {
                    reconnect_interval.tick().await;
                    match TcpStream::connect(shard_address).await {
                        Ok(connection) => {
                            info!("New connection to shard {shard}");
                            connection.set_nodelay(true).unwrap();
                            break connection;
                        }
                        Err(err) => {
                            error!("Establishing connection to node {shard} failed: {err}")
                        }
                    }
                };
                // Send handshake
                let mut registration_connection = frame_registration_connection(shard_connection);
                let handshake = RegistrationMessage::CoordinatorRegister;
                if let Err(err) = registration_connection.send(handshake).await {
                    error!("Error sending handshake to {shard}: {err}");
                    return;
                }
                let underlying_stream = registration_connection.into_inner().into_inner();
                // Create connection actor
                let shard_actor =
                    ShardConnection::new(shard, underlying_stream, batch_size, shard_sender);
                let new_connection = NewConnection::ToShard(shard_actor);
                connection_sender.send(new_connection).await.unwrap();
            });
        }
    }

    // Removes all client, peer, and shard connections and ends their corresponding tasks.
    #[allow(dead_code)]
    pub fn shutdown(&mut self) {
        if let Some(connection) = self.tester_connection.take() {
            connection.close();
        }
        for peer_connection in self.peer_connections.drain(..) {
            if let Some(connection) = peer_connection {
                connection.close();
            }
        }
        for shard_connection in self.shard_connections.drain(..) {
            if let Some(connection) = shard_connection {
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

    #[inline]
    fn shard_id_to_idx(&self, id: ShardId) -> Option<usize> {
        self.shards.iter().position(|&p| p == id)
    }
}

enum NewConnection {
    ToPeer(PeerConnection),
    ToTester(TesterConnection),
    ToShard(ShardConnection),
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

struct ShardConnection {
    shard_id: ShardId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<CoordinatorMessage>,
}

impl ShardConnection {
    pub fn new(
        shard_id: ShardId,
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<(ShardId, ShardMessage)>,
    ) -> Self {
        let (reader, mut writer) = frame_coordinator_connection(connection);
        // Reader Actor
        let reader_task = tokio::spawn(async move {
            let mut buf_reader = reader.ready_chunks(batch_size);
            while let Some(messages) = buf_reader.next().await {
                for msg in messages {
                    match msg {
                        Ok(m) => {
                            if let Err(_) = incoming_messages.send((shard_id, m)).await {
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
                        error!("Couldn't send message to shard {shard_id}: {err}");
                        break;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to shard {shard_id}: {err}");
                    break;
                }
            }
            info!("Connection to node {shard_id} closed");
        });
        ShardConnection {
            shard_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: CoordinatorMessage,
    ) -> Result<(), mpsc::error::SendError<CoordinatorMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

struct TesterConnection {
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: UnboundedSender<ServerMessage>,
}

impl TesterConnection {
    pub fn new(
        connection: TcpStream,
        batch_size: usize,
        incoming_messages: Sender<TesterMessage>,
    ) -> Self {
        let (reader, mut writer) = frame_to_tester_connection(connection);
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
                        error!("Couldn't send message to tester: {err}");
                        error!("Killing connection to tester");
                        return;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to tester: {err}");
                    error!("Killing connection to tester");
                    return;
                }
            }
        });
        TesterConnection {
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub fn send(
        &mut self,
        msg: ServerMessage,
    ) -> Result<(), mpsc::error::SendError<ServerMessage>> {
        self.outgoing_messages.send(msg)
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

pub type FromTesterConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    TesterMessage,
    (),
    Bincode<TesterMessage, ()>,
>;

pub type ToTesterConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ServerMessage,
    Bincode<(), ServerMessage>,
>;

pub fn frame_to_tester_connection(
    stream: TcpStream,
) -> (FromTesterConnection, ToTesterConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromTesterConnection::new(stream, Bincode::default()),
        ToTesterConnection::new(sink, Bincode::default()),
    )
}
