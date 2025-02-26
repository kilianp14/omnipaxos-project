use omnipaxos_sql::common::sql::{SqlCommand, QueryType};
use sqlx::{postgres::PgQueryResult, query, query_as, PgPool};

pub struct Database {
    pool: PgPool,
}

impl Database {
    pub async fn new(database_url: &str) -> Self {
        let pool = PgPool::connect(database_url).await.expect("Failed to connect to database");
        Database { pool }
    }

    pub async fn handle_command(&self, command: SqlCommand) -> Option<String> {
        match command.query_type {
            QueryType::Select => self.handle_select(command).await,
            QueryType::Insert => self.handle_insert(command).await,
            QueryType::Update => self.handle_update(command).await,
            QueryType::Delete => self.handle_delete(command).await,
            QueryType::Create => self.handle_create(command).await,
        }
    }

    async fn handle_select(&self, command: SqlCommand) -> Option<String> {
        let columns = command.columns.into_iter().map(|(col,_)|col).collect::<Vec<String>>().join(", ");
        let condition = command.conditions.unwrap_or("TRUE".to_string());
        let query_str = format!("SELECT {} FROM {} WHERE {}", columns, command.table, condition);
        
        let rows: Option<Vec<(String,)>> = query_as(&query_str)
            .fetch_all(&self.pool)
            .await
            .ok();
        
        match rows {
            Some(values) => {
                let result: String = values.into_iter()
                    .map(|(s,)| s)
                    .collect::<Vec<String>>()
                    .join(", ");
                Some(result)
            },
            None => None,
        }
    }

    async fn handle_insert(&self, command: SqlCommand) -> Option<String> {
        let columns = command.columns.into_iter().map(|(col,_)|col).collect::<Vec<String>>().join(", ");
        let values = command.values?.join(", ");
        let query_str = format!("INSERT INTO {} ({}) VALUES ({}) RETURNING id", command.table, columns, values);
        
        let result: Option<PgQueryResult> = query(&query_str)
            .execute(&self.pool)
            .await
            .ok();
        
        match result {
            Some(res) => Some(format!("Inserted {} rows", res.rows_affected())),
            None => None,
        }
    }

    async fn handle_update(&self, command: SqlCommand) -> Option<String> {
        let assignments: Vec<String> = command.columns.into_iter()
            .zip(command.values?)
            .map(|((col, _), val)| format!("{} = '{}'", col, val))
            .collect();
        
        let set_clause = assignments.join(", ");
        let condition = command.conditions.unwrap_or("TRUE".to_string());
        let query_str = format!("UPDATE {} SET {} WHERE {} RETURNING id", command.table, set_clause, condition);
        
        let result: Option<PgQueryResult> = query(&query_str)
            .execute(&self.pool)
            .await
            .ok();
        
        match result {
            Some(res) => Some(format!("Updated {} rows", res.rows_affected())),
            None => None,
        }
    }

    async fn handle_delete(&self, command: SqlCommand) -> Option<String> {
        let condition = command.conditions.unwrap_or("TRUE".to_string());
        let query_str = format!("DELETE FROM {} WHERE {} RETURNING id", command.table, condition);
        
        let result: Option<PgQueryResult> = query(&query_str)
            .execute(&self.pool)
            .await
            .ok();
        
        match result {
            Some(res) => Some(format!("Updated {} rows", res.rows_affected())),
            None => None,
        }
    }

    async fn handle_create(&self, command: SqlCommand) -> Option<String> {
        let columns_definitions: Vec<String> = command.columns
            .iter()
            .map(|(name, dtype)| format!("{} {}", name, dtype))
            .collect();
        
        let query_str = format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            command.table,
            columns_definitions.join(", ")
        );

        let result: Option<PgQueryResult> = query(&query_str)
            .execute(&self.pool)
            .await
            .ok();
        
        match result {
            Some(_) => Some(format!("Table {} rows", command.table)),
            None => None,
        }
    }    
}
