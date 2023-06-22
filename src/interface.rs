use crate::keys::Key;
use std::{any::Any, time::Duration};

pub type StorageResult<T> = std::result::Result<T, StorageError>;
#[derive(Debug)]
pub enum StorageError {
    KeyNotFound {
        key: Key,
        start_key: Key,
        end_key: Key,
    },
    Timeout(Duration),
    Canceled,
    /// Errors that can not be handled by a coproceszsor plugin but should instead be returned to the client.
    /// If such an error appears, plugins can run some cleanup code and return early from the/// request. The error will be passed to the client and the client might sretry the request.
    Other(Box<dyn Any>),
}

impl From<sled::Error> for StorageError {
    fn from(e: sled::Error) -> StorageError {
        StorageError::Other(Box::new(e))
    }
}
