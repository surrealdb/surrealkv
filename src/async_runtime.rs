use std::{
    fmt::{Display, Formatter},
    future::Future,
    pin::Pin,
    result::Result,
};

#[derive(Debug)]
pub struct JoinError;

impl Display for JoinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("JoinError")
    }
}

pub trait JoinHandle<T>: Future<Output = Result<T, JoinError>> + Send {}

pub trait TaskSpawner: Copy + Send + Sync + 'static {
    fn spawn<F>(self, f: F) -> Pin<Box<dyn JoinHandle<F::Output>>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static;
}

#[cfg(feature = "tokio")]
pub mod tokio;
