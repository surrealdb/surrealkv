use std::{fmt::Display, future::Future};

#[derive(Debug)]
pub struct JoinError;

impl Display for JoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("JoinError")
    }
}

pub trait JoinHandle<T>: Future<Output = std::result::Result<T, JoinError>> + Send {}

pub trait TaskSpawner: Send + Sync + 'static {
    fn spawn<F>(f: F) -> impl JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

pub mod tokio;
