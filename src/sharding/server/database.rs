use log::info;
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
        let keys = command.keys.unwrap_or_default();
        let keys_str = keys
            .into_iter()
            .map(|key| format!("'{}'", key))
            .collect::<Vec<String>>()
            .join(", ");
        let query_str = format!(
            "SELECT {} FROM {} WHERE key IN ({})",
            columns, command.table, keys_str
        );



        let rows: Option<Vec<(String,)>> = query_as(&query_str).fetch_all(&self.pool).await.ok();
        
        // info!("Query: {}", query_str);
        // info!("Rows: {:?}", rows);

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
        let columns = command.clone()
            .columns
            .into_iter()
            .map(|(col, _)| col)
            .collect::<Vec<String>>()
            .join(", ");
        let values: String = command.clone()
            .values?
            .iter()
            .map(|i| format!("{}", i))
            .collect::<Vec<String>>()
            .join(", ");

        let query_str = "BEGIN";
        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        // Execute your insert.
        for value in command.values.unwrap_or_default() {
            let insert_query = format!(
                "INSERT INTO {} ({}) VALUES {}",
                command.table, columns, value
            );
            let result: Option<PgQueryResult> = query(&insert_query).execute(&self.pool).await.ok();
            if result.is_none() {
                return Some(format!("Failed to insert row with query {}", insert_query));
            }
        }
        // Prepare the transaction to make it pending.
        let prepare_query = format!("PREPARE TRANSACTION '{}'", id);
        let result: Option<PgQueryResult> = query(&prepare_query).execute(&self.pool).await.ok();

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

        let query_str = "BEGIN";
        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        let set_clause = assignments.join(", ");
        let keys = command.keys.unwrap_or_default();
        let keys_str = keys
            .into_iter()
            .map(|key| format!("'{}'", key))
            .collect::<Vec<String>>()
            .join(", ");
        let query_str = format!(
            "UPDATE {} SET {} WHERE key IN ({}) RETURNING id;",
            command.table, set_clause, keys_str
        );
        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        // Prepare the transaction to make it pending.
        let prepare_query = format!("PREPARE TRANSACTION '{}'", id);
        let result: Option<PgQueryResult> = query(&prepare_query).execute(&self.pool).await.ok();

        match result {
            Some(res) => Some(format!("Updated {} rows", res.rows_affected())),
            None => None,
        }
    }

    async fn handle_delete(&self, command: SqlCommand, id:CommandId) -> Option<String> {
        let query_str = "BEGIN";
        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        let keys = command.keys.unwrap_or_default();
        let keys_str = keys
            .into_iter()
            .map(|key| format!("'{}'", key))
            .collect::<Vec<String>>()
            .join(", ");
        let query_str = format!(
            "DELETE FROM {} WHERE key IN ({}) RETURNING id;",
            command.table, keys_str
        );

        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();

        // Prepare the transaction to make it pending.
        let prepare_query = format!("PREPARE TRANSACTION '{}'", id);
        let result: Option<PgQueryResult> = query(&prepare_query).execute(&self.pool).await.ok();

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
            "CREATE TABLE IF NOT EXISTS {} ({})",
            command.table,
            columns_definitions.join(", ")
        );

        let result: Option<PgQueryResult> = query(&query_str).execute(&self.pool).await.ok();
        
        // Test insert values into all databases so we can do some cross shard reads
        let insert_values: Vec<String> = (0..=20)
            .map(|i| format!("({}, 'pre_written_{}')", i, i))
            .collect();

        let insert_query = format!(
            "INSERT INTO {} (key, value) VALUES {}",
            command.table,
            insert_values.join(", ")
        );

        let result: Option<PgQueryResult> = query(&insert_query).execute(&self.pool).await.ok();

        // Prepare the transaction to make it pending.
        let prepare_query = format!("PREPARE TRANSACTION '{}'", id);
        let result: Option<PgQueryResult> = query(&prepare_query).execute(&self.pool).await.ok();

        match result {
            Some(_) => Some(format!("Table {} rows", command.table)),
            None => None,
        }
    }
}
