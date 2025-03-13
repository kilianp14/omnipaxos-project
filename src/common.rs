

pub mod messages {
    use omnipaxos::{messages::Message as OmniPaxosMessage, util::NodeId};
    use serde::{Deserialize, Serialize};
    use crate::common::sql::ClientId;

    use super::{
        sql::{Command, CommandId, SqlCommand},
        utils::Timestamp,
    };

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum RegistrationMessage {
        NodeRegister(NodeId),
        ClientRegister,
        CoordinatorRegister,
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ClusterMessage {
        OmniPaxosMessage(OmniPaxosMessage<Command>),
        LeaderStartSignal(Timestamp),
        ReadRequest(ClientId, NodeId, CommandId, SqlCommand),
        ReadResponse(ClientId, CommandId, Option<String>),
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

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum CoordinatorMessage {
        Prepare(CommandId, SqlCommand),
        Commit(CommandId),
        Abort(CommandId),
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub enum ShardMessage {
        Ack(ClientId, CommandId),
        Nack(ClientId, CommandId),
        Answer(ClientId, CommandId, Option<String>),
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
    pub type ShardId = u64;
    pub type NodeId = omnipaxos::util::NodeId;
    pub type InstanceId = NodeId;

    #[derive(Debug, Clone, Entry, Serialize, Deserialize)]
    pub struct Command {
        pub client_id: ClientId,
        pub coordinator_id: NodeId,
        pub id: CommandId,
        pub sql_cmd: SqlCommand,
        pub phase: Phase,
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
        pub keys: Option<Vec<String>>,
        pub values: Option<Vec<String>>,
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
                keys: None,
                values: None,
                consistency: None,
            }
        }
        pub fn insert_cmd(keys: Vec<String>, values: Vec<String>) -> Self {
            Self {
                query_type: QueryType::Insert,
                table: TABLE_NAME.to_string(),
                columns: vec![
                    ("key".to_string(), "text".to_string()),
                    ("value".to_string(), "text".to_string()),
                ],
                keys: Some(keys),
                values: Some(values),
                consistency: None,
            }
        }

        pub fn select_cmd(keys: Vec<String>, consistency: Consistency) -> Self {
            Self {
                query_type: QueryType::Select,
                table: TABLE_NAME.to_string(),
                columns: vec![("value".to_string(), "text".to_string())],
                keys: Some(keys),
                values: None,
                consistency: Some(consistency),
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize, Copy)]
    pub enum QueryType {
        Select,
        Insert,
        Create,
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Consistency {
        Leader,
        Local,
        Linearizable,
    }
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

    pub type FromCoordinatorConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        CoordinatorMessage,
        (),
        Bincode<CoordinatorMessage, ()>,
    >;

    pub type ToCoordinatorConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        ShardMessage,
        Bincode<(), ShardMessage>,
    >;

    pub type FromShardConnection = Framed<
        FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
        ShardMessage,
        (),
        Bincode<ShardMessage, ()>,
    >;

    pub type ToShardConnection = Framed<
        FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
        (),
        CoordinatorMessage,
        Bincode<(), CoordinatorMessage>,
    >;

    pub fn frame_shard_connection(
        stream: TcpStream,
    ) -> (FromCoordinatorConnection, ToCoordinatorConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromCoordinatorConnection::new(stream, Bincode::default()),
            ToCoordinatorConnection::new(sink, Bincode::default()),
        )
    }

    pub fn frame_coordinator_connection(
        stream: TcpStream,
    ) -> (FromShardConnection, ToShardConnection) {
        let (reader, writer) = stream.into_split();
        let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
        let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
        (
            FromShardConnection::new(stream, Bincode::default()),
            ToShardConnection::new(sink, Bincode::default()),
        )
    }
}
