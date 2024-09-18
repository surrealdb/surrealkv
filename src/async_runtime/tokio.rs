use std::{
    future::Future,
    pin::{pin, Pin},
    result::Result,
    task::{Context, Poll},
};

use super::{JoinError, JoinHandle, TaskSpawner};

#[derive(Copy, Clone, Default)]
pub struct TokioSpawner;

impl TaskSpawner for TokioSpawner {
    fn spawn<F>(self, f: F) -> Pin<Box<dyn JoinHandle<F::Output>>>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Box::pin(TokioJoinHandle(tokio::spawn(f)))
    }
}

pub struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for TokioJoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        pin!(&mut self.0).poll(cx).map_err(|_| JoinError)
    }
}

impl<T: Send> JoinHandle<T> for TokioJoinHandle<T> {}
