pub mod messages {
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
    use serde::{Deserialize, Serialize};

    use super::{
        sql::{Command, CommandId, SqlCommand},
        utils::Timestamp,
    };

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        NodeRegister2(NodeId),
        NodeRegister3(NodeId),
        ClientRegister,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(OmniPaxosMessage<Command>),
        LeaderStartSignal(Timestamp),
        ReadRequest(NodeId, NodeId, CommandId, SqlCommand),
        ReadResponse(NodeId, CommandId, Option<String>),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClientMessage {
        Handle(CommandId, SqlCommand),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ServerMessage {
        Answer(CommandId, Option<String>),
        StartSignal(Timestamp),
    }

    impl ServerMessage {
        pub fn command_id(&self) -> (CommandId, Option<String>) {
            match self {
                ServerMessage::Answer(id, s) => (*id, s.clone()),
                ServerMessage::StartSignal(_) => unimplemented!(),
            }
        }
    }
}

pub const TABLE_NAME: &str = "test_table";
pub mod sql {
    // use omnipaxos::{macros::Entry, storage::Snapshot};
    use crate::common::TABLE_NAME;
    use omnipaxos::macros::Entry;
    use serde::{Deserialize, Serialize};

    pub type CommandId = usize;
    pub type ClientId = u64;
    pub type NodeId = omnipaxos::util::NodeId;
    pub type InstanceId = NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub sql_cmd: SqlCommand,
        pub phase: Option<Phase>,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum Phase {
        Prepare,
        Commit,
        Abort,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct SqlCommand {
        pub query_type: QueryType,
        pub table: String,
        pub columns: Vec<(String, String)>, // this is column name, type
        pub values: Option<Vec<String>>,
        pub conditions: Option<String>,
        pub consistency: Option<Consistency>,
    }

    impl SqlCommand {
        pub fn create_table_cmd() -> Self {
            Self {
                query_type: QueryType::Create,
                table: TABLE_NAME.to_string(),
                columns: vec![
                    ("id".to_string(), "serial".to_string()),
                    ("key".to_string(), "text".to_string()),
                    ("value".to_string(), "text".to_string()),
                ],
                values: None,
                conditions: None,
                consistency: None,
            }
        }
        pub fn insert_cmd(client_id: String, key: String) -> Self {
            Self {
                query_type: QueryType::Insert,
                table: TABLE_NAME.to_string(),
                columns: vec![
                    ("key".to_string(), "text".to_string()),
                    ("value".to_string(), "text".to_string()),
                ],
                values: Some(vec![key.clone(), format!("{}_value_{}", client_id, key)]),
                conditions: None,
                consistency: None,
            }
        }

        pub fn select_cmd(key: String, consistency: Consistency) -> Self {
            Self {
                query_type: QueryType::Select,
                table: TABLE_NAME.to_string(),
                columns: vec![("value".to_string(), "text".to_string())],
                values: None,
                conditions: Some(format!("key = '{}'", key)),
                consistency: Some(consistency),
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Copy)]
    pub enum QueryType {
        Select,
        Insert,
        Update,
        Delete,
        Create,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Consistency {
        Leader,
        Local,
        Linearizable,
    }

    //#[derive(Clone, Debug, Serialize, Deserialize)]
    //pub struct KVSnapshot {
    //snapshotted: HashMap<String, String>,
    //deleted_keys: Vec<String>,
    //}

    //impl Snapshot<Command> for KVSnapshot {
    //fn create(entries: &[Command]) -> Self {
    //let mut snapshotted = HashMap::new();
    //let mut deleted_keys: Vec<String> = Vec::new();
    //for e in entries {
    //match &e.kv_cmd {
    //KVCommand::Put(key, value) => {
    //snapshotted.insert(key.clone(), value.clone());
    //}
    //KVCommand::Delete(key) => {
    //if snapshotted.remove(key).is_none() {
    //// key was not in the snapshot
    //deleted_keys.push(key.clone());
    //}
    //}
    //KVCommand::Get(_) => (),
    //}
    //}
    //// remove keys that were put back
    //deleted_keys.retain(|k| !snapshotted.contains_key(k));
    //Self {
    //snapshotted,
    //deleted_keys,
    //}
    //}

    //fn merge(&mut self, delta: Self) {
    //for (k, v) in delta.snapshotted {
    //self.snapshotted.insert(k, v);
    //}
    //for k in delta.deleted_keys {
    //self.snapshotted.remove(&k);
    //}
    //self.deleted_keys.clear();
    //}

    //fn use_snapshots() -> bool {
    //true
    //}
    //}
}

pub mod utils {
    use super::messages::*;
    use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::TcpStream;
    use tokio_serde::{formats::Bincode, Framed};
    use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

    pub type Timestamp = i64;

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

    pub type FromNodeConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClusterMessage,
        (),
        Bincode<ClusterMessage, ()>,
    >;
    pub type ToNodeConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClusterMessage,
        Bincode<(), ClusterMessage>,
    >;

    pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromNodeConnection::new(stream, Bincode::default()),
            ToNodeConnection::new(sink, Bincode::default()),
        )
    }

    // pub type ServerConnection = Framed<
    //     CodecFramed<TcpStream, LengthDelimitedCodec>,
    //     ServerMessage,
    //     ClientMessage,
    //     Bincode<ServerMessage, ClientMessage>,
    // >;

    pub type FromServerConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ServerMessage,
        (),
        Bincode<ServerMessage, ()>,
    >;

    pub type ToServerConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ClientMessage,
        Bincode<(), ClientMessage>,
    >;

    pub type FromClientConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ClientMessage,
        (),
        Bincode<ClientMessage, ()>,
    >;

    pub type ToClientConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ServerMessage,
        Bincode<(), ServerMessage>,
    >;

    pub fn frame_clients_connection(
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

    // pub fn frame_clients_connection(stream: TcpStream) -> ServerConnection {
    //     let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    //     Framed::new(length_delimited, Bincode::default())
    // }

    pub fn frame_servers_connection(
        stream: TcpStream,
    ) -> (FromClientConnection, ToClientConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromClientConnection::new(stream, Bincode::default()),
            ToClientConnection::new(sink, Bincode::default()),
        )
    }
}
