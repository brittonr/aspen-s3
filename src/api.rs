//! Storage API traits for S3 backend
//!
//! This module defines the traits that any storage backend must implement
//! to support the S3 API. This allows for different implementations:
//! - In-memory (for testing)
//! - Raft-backed (not recommended for blob data)
//! - iroh-blobs (recommended for content-addressed storage)
//! - Direct filesystem
//! - External S3

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt;

/// Errors that can occur in the key-value store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KeyValueStoreError {
    NotFound { key: String },
    AlreadyExists { key: String },
    Failed { reason: String },
    NotInitialized,
}

impl fmt::Display for KeyValueStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound { key } => write!(f, "Key not found: {}", key),
            Self::AlreadyExists { key } => write!(f, "Key already exists: {}", key),
            Self::Failed { reason } => write!(f, "Operation failed: {}", reason),
            Self::NotInitialized => write!(f, "Store not initialized"),
        }
    }
}

impl std::error::Error for KeyValueStoreError {}

/// Write commands for the KV store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WriteCommand {
    Set { key: String, value: Vec<u8> },
    SetMulti { pairs: Vec<(String, Vec<u8>)> },
    Delete { key: String },
    DeleteMulti { keys: Vec<String> },
}

/// Write request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    pub command: WriteCommand,
}

/// Write result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteResult {
    pub success: bool,
}

/// Read request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadRequest {
    pub key: String,
}

/// Read result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadResult {
    pub value: Option<Vec<u8>>,
}

/// Delete request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRequest {
    pub key: String,
}

/// Delete result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteResult {
    pub deleted: bool,
}

/// Scan request for prefix-based queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanRequest {
    pub prefix: String,
    pub start_after: Option<String>,
    pub limit: Option<u32>,
}

/// Scan result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScanResult {
    pub items: Vec<(String, Vec<u8>)>,
    pub has_more: bool,
}

/// Trait for key-value storage operations
#[async_trait]
pub trait KeyValueStore: Send + Sync {
    async fn write(&self, request: WriteRequest) -> Result<WriteResult, KeyValueStoreError>;
    async fn read(&self, request: ReadRequest) -> Result<ReadResult, KeyValueStoreError>;
    async fn delete(&self, request: DeleteRequest) -> Result<DeleteResult, KeyValueStoreError>;
    async fn scan(&self, request: ScanRequest) -> Result<ScanResult, KeyValueStoreError>;
}

/// Simple in-memory KV store for testing
pub struct InMemoryKeyValueStore {
    data: std::sync::Arc<tokio::sync::RwLock<std::collections::BTreeMap<String, Vec<u8>>>>,
}

impl InMemoryKeyValueStore {
    pub fn new() -> Self {
        Self {
            data: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::BTreeMap::new())),
        }
    }
}

impl Default for InMemoryKeyValueStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl KeyValueStore for InMemoryKeyValueStore {
    async fn write(&self, request: WriteRequest) -> Result<WriteResult, KeyValueStoreError> {
        let mut data = self.data.write().await;
        match request.command {
            WriteCommand::Set { key, value } => {
                data.insert(key, value);
                Ok(WriteResult { success: true })
            }
            WriteCommand::SetMulti { pairs } => {
                for (key, value) in pairs {
                    data.insert(key, value);
                }
                Ok(WriteResult { success: true })
            }
            WriteCommand::Delete { key } => {
                data.remove(&key);
                Ok(WriteResult { success: true })
            }
            WriteCommand::DeleteMulti { keys } => {
                for key in keys {
                    data.remove(&key);
                }
                Ok(WriteResult { success: true })
            }
        }
    }

    async fn read(&self, request: ReadRequest) -> Result<ReadResult, KeyValueStoreError> {
        let data = self.data.read().await;
        Ok(ReadResult {
            value: data.get(&request.key).cloned(),
        })
    }

    async fn delete(&self, request: DeleteRequest) -> Result<DeleteResult, KeyValueStoreError> {
        let mut data = self.data.write().await;
        let deleted = data.remove(&request.key).is_some();
        Ok(DeleteResult { deleted })
    }

    async fn scan(&self, request: ScanRequest) -> Result<ScanResult, KeyValueStoreError> {
        let data = self.data.read().await;
        let limit = request.limit.unwrap_or(1000) as usize;

        let items: Vec<(String, Vec<u8>)> = data
            .range(request.prefix.clone()..)
            .take_while(|(k, _)| k.starts_with(&request.prefix))
            .skip_while(|(k, _)| {
                if let Some(ref start) = request.start_after {
                    k <= start
                } else {
                    false
                }
            })
            .take(limit + 1)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        let has_more = items.len() > limit;
        let items = if has_more {
            items.into_iter().take(limit).collect()
        } else {
            items
        };

        Ok(ScanResult { items, has_more })
    }
}