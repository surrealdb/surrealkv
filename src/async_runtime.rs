use std::{error::Error, future::Future, result::Result};
pub trait JoinHandle<T>: Future<Output = Result<T, Box<dyn Error>>> + Send {}

pub trait TaskSpawner: Send + Sync + 'static {
    fn spawn<F>(&self, f: F) -> Box<dyn JoinHandle<F::Output> + Unpin>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

#[cfg(feature = "tokio")]
pub mod tokio;
