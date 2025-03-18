use futures::{SinkExt, StreamExt};
use log::*;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio_serde::{formats::Bincode, Framed};
use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};
use omnipaxos_sql::common::{messages::*, sql::{NodeId, ShardId}};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::sync::mpsc::{self, channel};
use tokio::task::JoinHandle;
use tokio::{net::TcpStream, sync::mpsc::Receiver};
use tokio::{sync::mpsc::Sender, time::interval};
use serde::{Serialize, Deserialize};


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum TesterMessage {
    ClientMessage(ClientMessage),
    ClusterMessage(ClusterMessage, NodeId),
    Disconnect(NodeId),
    Reconnect(NodeId),
}

pub struct Network {
    server_connections: Vec<Option<ServerConnection>>,
    server_message_sender: Sender<ServerMessage>,
    pub server_messages: Receiver<ServerMessage>,
    batch_size: usize,
    n_shards: u64,
}

const RETRY_SERVER_CONNECTION_TIMEOUT: Duration = Duration::from_secs(1);

impl Network {
    pub async fn new(servers: Vec<(NodeId, ShardId, String)>, batch_size: usize, n_shards: u64) -> Self {
        let mut server_connections = vec![];
        server_connections.resize_with(servers.len(), Default::default);
        let (server_message_sender, server_messages) = channel(batch_size);
        let mut network = Self {
            server_connections,
            batch_size,
            server_message_sender,
            server_messages,
            n_shards,
        };
        network.initialize_connections(servers).await;
        network
    }

    async fn initialize_connections(&mut self, servers: Vec<(NodeId, ShardId, String)>) {
        info!("Establishing server connections");
        let mut connection_tasks = Vec::with_capacity(servers.len());
        for (server_id, shard_id,  server_addr_str) in &servers {
            let server_address = server_addr_str
                .to_socket_addrs()
                .expect("Unable to resolve server IP")
                .next()
                .unwrap();
            let task = tokio::spawn(Self::get_server_connection(*server_id, *shard_id, server_address));
            connection_tasks.push(task);
        }
        let finished_tasks = futures::future::join_all(connection_tasks).await;
        for (i, result) in finished_tasks.into_iter().enumerate() {
            match result {
                Ok((from_server_conn, to_server_conn)) => {
                    let connected_server_id = servers[i].0;
                    info!("Connected to server {connected_server_id}");
                    let server_connection = ServerConnection::new(
                        connected_server_id,
                        from_server_conn,
                        to_server_conn,
                        self.batch_size,
                        self.server_message_sender.clone(),
                    );
                    self.server_connections[i] = Some(server_connection)
                }
                Err(err) => {
                    let failed_server = servers[i].0;
                    panic!("Unable to establish connection to server {failed_server}: {err}")
                }
            }
        }
    }

    async fn get_server_connection(
        server_id: NodeId,
        shard_id: ShardId,
        server_address: SocketAddr,
    ) -> (FromServerConnection, ToServerConnection) {
        let mut retry_connection = interval(RETRY_SERVER_CONNECTION_TIMEOUT);
        loop {
            retry_connection.tick().await;
            match TcpStream::connect(server_address).await {
                Ok(stream) => {
                    stream.set_nodelay(true).unwrap();
                    let mut registration_connection = frame_registration_connection(stream);
                    registration_connection
                        .send(RegistrationMessage::ClientRegister)
                        .await
                        .expect("Couldn't send registration to server");
                    let underlying_stream = registration_connection.into_inner().into_inner();
                    break frame_tester_connection(underlying_stream);
                }
                Err(e) => error!("Unable to connect to server {server_id} {shard_id}: {e}"),
            }
        }
    }

    pub async fn send(&mut self, to: NodeId, shard: ShardId, msg: TesterMessage) {
        match self.server_connections.get_mut(((to - 1) * (self.n_shards + 1) + shard) as usize) {
            Some(connection_slot) => match connection_slot {
                Some(connection) => {
                    if let Err(err) = connection.send(msg).await {
                        warn!("Couldn't send msg to server {to}: {err}");
                        self.server_connections[to as usize] = None;
                    }
                }
                None => error!("Not connected to server {to}"),
            },
            None => error!("Sending to unexpected server {to}"),
        }
    }

    // Removes all server connections and ends their corresponding tasks
    pub fn shutdown(&mut self) {
        let connection_count = self.server_connections.len();
        for server_connection in self.server_connections.drain(..) {
            if let Some(connection) = server_connection {
                connection.close();
            }
        }
        for _ in 0..connection_count {
            self.server_connections.push(None);
        }
    }
}

struct ServerConnection {
    // server_id: NodeId,
    reader_task: JoinHandle<()>,
    writer_task: JoinHandle<()>,
    outgoing_messages: Sender<TesterMessage>,
}

impl ServerConnection {
    pub fn new(
        server_id: NodeId,
        reader: FromServerConnection,
        mut writer: ToServerConnection,
        batch_size: usize,
        incoming_messages: Sender<ServerMessage>,
    ) -> Self {
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
        let (message_tx, mut message_rx) = mpsc::channel(batch_size);
        let writer_task = tokio::spawn(async move {
            let mut buffer = Vec::with_capacity(batch_size);
            while message_rx.recv_many(&mut buffer, batch_size).await != 0 {
                for msg in buffer.drain(..) {
                    if let Err(err) = writer.feed(msg).await {
                        error!("Couldn't send message to server {server_id}: {err}");
                        break;
                    }
                }
                if let Err(err) = writer.flush().await {
                    error!("Couldn't send message to server {server_id}: {err}");
                    break;
                }
            }
            info!("Connection to server {server_id} closed");
        });
        ServerConnection {
            // server_id,
            reader_task,
            writer_task,
            outgoing_messages: message_tx,
        }
    }

    pub async fn send(
        &mut self,
        msg: TesterMessage,
    ) -> Result<(), mpsc::error::SendError<TesterMessage>> {
        self.outgoing_messages.send(msg).await
    }

    fn close(self) {
        self.reader_task.abort();
        self.writer_task.abort();
    }
}

pub type RegistrationConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    RegistrationMessage,
    RegistrationMessage,
    Bincode<RegistrationMessage, RegistrationMessage>,
>;

pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Bincode::default())
}

pub type FromServerConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ServerMessage,
    (),
    Bincode<ServerMessage, ()>,
>;

pub type ToServerConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    TesterMessage,
    Bincode<(), TesterMessage>,
>;

pub fn frame_tester_connection(
    stream: TcpStream,
) -> (FromServerConnection, ToServerConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromServerConnection::new(stream, Bincode::default()),
        ToServerConnection::new(sink, Bincode::default()),
    )
}