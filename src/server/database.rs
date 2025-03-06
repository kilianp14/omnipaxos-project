use omnipaxos_sql::common::sql::{Phase, QueryType, SqlCommand, CommandId};
use sqlx::{postgres::PgQueryResult, query, query_as, Executor, PgPool};
use uuid::Uuid;

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(base_url: &str) -> Self {
        // TODO: Should we create/initialize the database in a separate script?
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

    pub async fn commit_command(&self, transaction_id: CommandId) -> Option<String> {
        let commit_query = format!("COMMIT PREPARED '{}'", transaction_id);
        let response = query(&commit_query).execute(&self.pool).await.ok();
        match response {
            Some(res) => Some(format!("Committed Transaction {}", transaction_id)),
            None => Some(format!("Failed to commit Transaction {}", transaction_id)),
        }
    }

    pub async fn abort_command(&self, transaction_id: CommandId) -> Option<String> {
        let abort_query = format!("ROLLBACK PREPARED '{}'", transaction_id);
        let response = query(&abort_query).execute(&self.pool).await.ok();
        match response {
            Some(res) => Some(format!("Aborted Transaction {}", transaction_id)),
            None => Some(format!("Failed to abort Transaction {}", transaction_id)),
        }
    }

    pub async fn prepare_command(&self, command: SqlCommand, id:CommandId) -> Option<String> {
        match command.query_type {
            QueryType::Select => self.handle_select(command).await,
            QueryType::Insert => self.handle_insert(command, id).await,
            QueryType::Update => self.handle_update(command, id).await,
            QueryType::Delete => self.handle_delete(command, id).await,
            QueryType::Create => self.handle_create(command, id).await,
        }
    }

    async fn handle_select(&self, command: SqlCommand) -> Option<String> {
        let columns = command
            .columns
            .into_iter()
            .map(|(col, _)| col)
            .collect::<Vec<String>>()
            .join(", ");
        let condition = command.conditions.unwrap_or("TRUE".to_string());
        let query_str = format!(
            "SELECT {} FROM {} WHERE {}",
            columns, command.table, condition
        );

        let rows: Option<Vec<(String,)>> = query_as(&query_str).fetch_all(&self.pool).await.ok();

        match rows {
            Some(values) => {
                let result: String = values
                    .into_iter()
                    .map(|(s,)| s)
                    .collect::<Vec<String>>()
                    .join(", ");
                Some(result)
            }
            None => None,
        }
    }

    async fn handle_insert(&self, command: SqlCommand, id:CommandId) -> Option<String> {
        let columns = command
            .columns
            .into_iter()
            .map(|(col, _)| col)
            .collect::<Vec<String>>()
            .join(", ");
        let values: String = command
            .values?
            .iter()
            .map(|i| format!("'{}'", i))
            .collect::<Vec<String>>()
            .join(", ");
        let query_str = format!(
            "BEGIN; INSERT INTO {} ({}) VALUES ({}) RETURNING id; PREPARE TRANSACTION '{}';",
            command.table, columns, values, id
        );

        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        match result {
            Some(res) => Some(format!("Inserted {} rows", res.rows_affected())),
            None => Some(format!("Failed to insert row with query {}", query_str)),
        }
    }

    async fn handle_update(&self, command: SqlCommand, id:CommandId) -> Option<String> {
        // FYI this is probably not required for this project.
        let assignments: Vec<String> = command
            .columns
            .into_iter()
            .zip(command.values?)
            .map(|((col, _), val)| format!("{} = '{}'", col, val))
            .collect();

        let set_clause = assignments.join(", ");
        let condition = command.conditions.unwrap_or("TRUE".to_string());
        let query_str = format!(
            "BEGIN; UPDATE {} SET {} WHERE {} RETURNING id; PREPARE TRANSACTION '{}';",
            command.table, set_clause, condition, id
        );

        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        match result {
            Some(res) => Some(format!("Updated {} rows", res.rows_affected())),
            None => None,
        }
    }

    async fn handle_delete(&self, command: SqlCommand, id:CommandId) -> Option<String> {
        let condition = command.conditions.unwrap_or("TRUE".to_string());
        let query_str = format!(
            "BEGIN; DELETE FROM {} WHERE {} RETURNING id; PREPARE TRANSACTION '{}';",
            command.table, condition, id
        );

        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        match result {
            Some(res) => Some(format!("Updated {} rows", res.rows_affected())),
            None => None,
        }
    }

    async fn handle_create(&self, command: SqlCommand, id:CommandId) -> Option<String> {
        let columns_definitions: Vec<String> = command
            .columns
            .iter()
            .map(|(name, dtype)| format!("{} {}", name, dtype))
            .collect();

        let query_str = format!(
            "BEGIN; CREATE TABLE IF NOT EXISTS {} ({}); PREPARE TRANSACTION '{}';",
            command.table,
            columns_definitions.join(", "),
            id
        );

        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        match result {
            Some(_) => Some(format!("Table {} rows", command.table)),
            None => None,
        }
    }
}
