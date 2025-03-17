use std::{fs::File, io::Write};

use crate::configs::ClientConfig;
use chrono::Utc;
use csv::Writer;
use omnipaxos_sql::common::sql::{Consistency, SqlCommand};
use omnipaxos_sql::common::{
    sql::{CommandId, QueryType},
    utils::Timestamp,
};
use serde::Serialize;

#[derive(Debug, Serialize, Clone)]
struct RequestData {
    command_id: CommandId,
    request_time: Timestamp,
    response_time: Option<Timestamp>,
    consistency: Option<Consistency>,
    query_type: QueryType,
    request_value: Option<String>,
    response_value: Option<String>,
}

pub struct ClientData {
    request_data: Vec<RequestData>,
    response_count: usize,
}

impl ClientData {
    pub fn new() -> Self {
        ClientData {
            request_data: Vec::new(),
            response_count: 0,
        }
    }

    pub fn new_request(&mut self, command: SqlCommand, command_id: CommandId) {
        let values :Option<String> = match command.query_type {
            QueryType::Insert => {
                if let Some(vals) = command.values.clone() {
                    Some(vals.iter().map(|v| v.join(",")).collect::<Vec<String>>().join("."))
                }
                else {None}
            },
            QueryType::Select => {
                if let Some(keys) = command.keys.clone() {
                    Some(keys.iter().map(|key| key.to_string()).collect::<Vec<String>>().join("."))
                }
                else {None}
            },
            _ => None,
        };
        let data = RequestData {
            request_time: Utc::now().timestamp_millis(),
            query_type: command.query_type,
            command_id,
            request_value: values,
            consistency: command.consistency,
            response_value: None,
            response_time: None,
        };
        self.request_data.push(data);
    }

    pub fn new_response(&mut self, command_id: CommandId, response: String) {
        let response_time = Utc::now().timestamp_millis();
        self.request_data[command_id].response_time = Some(response_time);
        self.request_data[command_id].response_value = Some(response);
        self.response_count += 1;
    }

    pub fn response_count(&self) -> usize {
        self.response_count
    }

    pub fn request_count(&self) -> usize {
        self.request_data.len()
    }

    pub fn save_summary(&self, config: ClientConfig) -> Result<(), std::io::Error> {
        let config_json = serde_json::to_string_pretty(&config)?;
        let mut summary_file = File::create(config.summary_filepath)?;
        summary_file.write_all(config_json.as_bytes())?;
        summary_file.flush()?;
        Ok(())
    }

    pub fn to_csv(&self, file_path: String) -> Result<(), std::io::Error> {
        let file = File::create(file_path)?;
        let mut writer = Writer::from_writer(file);
        for data in &self.request_data {
            writer.serialize(data)?;
        }
        writer.flush()?;
        Ok(())
    }
}