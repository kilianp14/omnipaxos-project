use omnipaxos_sql::common::{sql::{QueryType, SqlCommand, CommandId, ShardId, NodeId}, messages::DatabaseError};
use sqlx::{Executor, PgPool, Row};
use uuid::Uuid;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(base_url: &str) -> Self {
        let default_pool = PgPool::connect(base_url)
            .await
            .expect("Failed to connect to PostgreSQL");

        // Generate a unique database name
        let db_name = format!(
            "omnipaxos_tempdb_{}",
            Uuid::new_v4().to_string().replace("-", "_")
        );

        // Create a new temporary database
        let create_db_query = format!("CREATE DATABASE {}", db_name);
        default_pool
            .execute(create_db_query.as_str())
            .await
            .expect("Failed to create temp database");

        // Construct new database URL
        let temp_db_url = format!("{}/{}", base_url, db_name);

        // Connect to the new temporary database
        let temp_pool = PgPool::connect(&temp_db_url)
            .await
            .expect("Failed to connect to temp database");
        Database { pool: temp_pool }
    }

    pub async fn commit_command(&self, transaction_id: CommandId, node_id: NodeId, shard_id: ShardId) -> Result<String, DatabaseError> {
        let commit_query = format!("COMMIT PREPARED '{}_{}_{}'", transaction_id, node_id, shard_id);
        sqlx::query(&commit_query).execute(&self.pool).await?;
    
        Ok(format!("Committed Transaction {}", transaction_id))
    }
    
    pub async fn abort_command(&self, transaction_id: CommandId, node_id: NodeId, shard_id: ShardId) -> Result<String, DatabaseError> {
        let abort_query = format!("ROLLBACK PREPARED '{}_{}_{}'", transaction_id, node_id, shard_id);
        sqlx::query(&abort_query).execute(&self.pool).await?;
    
        Ok(format!("Aborted Transaction {}", transaction_id))
    }

    pub async fn prepare_command(
        &self, command: SqlCommand, transaction_id: CommandId, node_id: NodeId, shard_id: ShardId
    ) -> Result<String, DatabaseError> {
        // Begin the transaction
        sqlx::query("BEGIN").execute(&self.pool).await?;

        let result = self.execute_command(command).await;
        match result {
            Ok(message) => {
                // Node and Shard id is needed here because otherwise we use the same global transaction id for all databases
                // This causes issues if running in a local cluster
                let prepare_query = format!("PREPARE TRANSACTION '{}_{}_{}'", transaction_id, node_id, shard_id);
                sqlx::query(&prepare_query).execute(&self.pool).await?;
            
                Ok(format!("{}", message))
            },
            Err(err) => Err(err)
        }
    }

    pub async fn execute_command(&self, command: SqlCommand) -> Result<String, DatabaseError> {
        match command.query_type {
            QueryType::Select => self.handle_select(command).await,
            QueryType::Insert => self.handle_insert(command).await,
            QueryType::Create => self.handle_create(command).await,
        }
    }

    async fn handle_select(&self, command: SqlCommand) -> Result<String, DatabaseError> {
        if command.keys.is_none() || command.keys.as_ref().unwrap().is_empty() {
            return Err(DatabaseError{message: "No keys provided".to_string()});
        }
    
        let columns: String = if command.columns.is_empty() {
            "*".to_string()
        } else {
            command
                .columns
                .iter()
                .map(|(col, _)| format!("\"{}\"", col))
                .collect::<Vec<String>>()
                .join(", ")
        };
        
        let query_str = format!(
            "SELECT {} FROM \"{}\" WHERE \"{}\" = ANY($1)",
            columns, command.table, command.columns[0].0
        );
    
        let rows = sqlx::query(&query_str)
            .bind(command.keys.unwrap())
            .fetch_all(&self.pool)
            .await?;
    
        if rows.is_empty() {
            return Ok("".to_string());
        }

        let result = rows
            .iter()
            .map(|row| {
                let values: Vec<String> = command.columns.iter().enumerate().map(|(i, (_, col_type))| {
                    match col_type.as_str() {
                        "text" | "varchar" | "char" => row.get::<String, _>(i),
                        "int4" | "int8" | "bigint" | "integer" => row.get::<i64, _>(i).to_string(),
                        "float4" | "float8" | "real" | "double precision" => row.get::<f64, _>(i).to_string(),
                        "bool" => row.get::<bool, _>(i).to_string(),
                        _ => "UNKNOWN".to_string(),
                    }
                }).collect();
                values.join(", ") // Join columns with a comma
            })
            .collect::<Vec<String>>()
            .join("; ");
    
        Ok(result)
    }
    
    async fn handle_insert(&self, command: SqlCommand) -> Result<String, DatabaseError> {
        let values = match command.values.clone() {
            Some(v) if !v.is_empty() => v,
            _ => return Err(DatabaseError{message: "No values provided for insertion.".to_string()}),
        };
        let keys = match command.keys.clone() {
            Some(v) if !v.is_empty() => v,
            _ => return Err(DatabaseError{message: "No keys provided for insertion.".to_string()}),
        };
        if keys.len() != values.len() {
            return Err(DatabaseError{message: "Different amount of keys and values".to_string()})
        }
    
        let columns: Vec<String> = command
            .columns
            .iter()
            .map(|(col, _)| format!("\"{}\"", col))
            .collect();
        let columns_str = columns.join(", ");
    
        let insert_query = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            command.table,
            columns_str,
            (0..=values[0].len())
                .map(|i| format!("${}", i + 1))
                .collect::<Vec<String>>()
                .join(", ")
        );
        
        for (key, row) in keys.iter().zip(values.iter()) {
            let mut query = sqlx::query(&insert_query);
            query = query.bind(key);
            for value in row {
                query = query.bind(value);
            }
            query.execute(&self.pool).await?;
        }
        Ok(format!("Inserted {} rows", values.len()))
    }
    
    async fn handle_create(&self, command: SqlCommand) -> Result<String, DatabaseError> {
        if command.columns.is_empty() {
            return Err(DatabaseError{message: "No columns provided.".to_string()});
        }
    
        let primary_key = &command.columns[0].0;
    
        let columns_definitions: Vec<String> = command
            .columns
            .iter()
            .map(|(name, dtype)| format!("\"{}\" {}", name, dtype))
            .collect();
    
        let query_str = format!(
            "CREATE TABLE IF NOT EXISTS \"{}\" ({} , PRIMARY KEY (\"{}\"))",
            command.table,
            columns_definitions.join(", "),
            primary_key
        );
    
        sqlx::query(&query_str).execute(&self.pool).await?;
    
        Ok(format!("Created table: {}", command.table))
    }
}
