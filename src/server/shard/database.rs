use omnipaxos_sql::common::{sql::{QueryType, SqlCommand, CommandId}, messages::DatabaseError};
use sqlx::{Executor, PgPool};
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

    pub async fn commit_command(&self, transaction_id: CommandId) -> Result<String, DatabaseError> {
        let commit_query = format!("COMMIT PREPARED '{}'", transaction_id);
        sqlx::query(&commit_query).execute(&self.pool).await?;
    
        Ok(format!("Committed Transaction {}", transaction_id))
    }
    
    pub async fn abort_command(&self, transaction_id: CommandId) -> Result<String, DatabaseError> {
        let abort_query = format!("ROLLBACK PREPARED '{}'", transaction_id);
        sqlx::query(&abort_query).execute(&self.pool).await?;
    
        Ok(format!("Aborted Transaction {}", transaction_id))
    }

    pub async fn prepare_command(&self, command: SqlCommand, id:CommandId) -> Result<String, DatabaseError> {
        match command.query_type {
            QueryType::Select => self.handle_select(command).await,
            QueryType::Insert => self.handle_insert(command, id).await,
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
            "SELECT {} FROM \"{}\" WHERE \"key\" = ANY($1)",
            columns, command.table
        );
    
        let rows: Vec<(String,)> = sqlx::query_as(&query_str)
            .bind(command.keys.unwrap())
            .fetch_all(&self.pool)
            .await?;
    
        if rows.is_empty() {
            return Err(DatabaseError{message: "Rows not found".to_string()});
        }
    
        let result = rows
            .into_iter()
            .map(|(value,)| value)
            .collect::<Vec<String>>()
            .join(", ");
    
        Ok(result)
    }
    
    async fn handle_insert(&self, command: SqlCommand, id: CommandId) -> Result<String, DatabaseError> {
        let values = match command.values.clone() {
            Some(v) if !v.is_empty() => v,
            _ => return Err(DatabaseError{message: "No values provided for insertion.".to_string()}),
        };
    
        let columns: Vec<String> = command
            .columns
            .iter()
            .map(|(col, _)| format!("\"{}\"", col))
            .collect();
        let columns_str = columns.join(", ");
    
        // Begin the transaction
        sqlx::query("BEGIN").execute(&self.pool).await?;
    
        let insert_query = format!(
            "INSERT INTO \"{}\" ({}) VALUES ({})",
            command.table,
            columns_str,
            values[0]
                .iter()
                .enumerate()
                .map(|(i, _)| format!("${}", i + 1))
                .collect::<Vec<String>>()
                .join(", ")
        );
    
        for row in values.iter() {
            sqlx::query(&insert_query)
                .bind(row.clone())
                .execute(&self.pool)
                .await?;
        }
    
        // Prepare the transaction (but do not commit yet)
        let prepare_query = format!("PREPARE TRANSACTION '{}'", id);
        sqlx::query(&prepare_query).execute(&self.pool).await?;
    
        Ok(format!("Prepared transaction '{}', awaiting commit.", id))
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
