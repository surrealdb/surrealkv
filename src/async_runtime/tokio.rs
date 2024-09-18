use std::{
    error::Error,
    future::Future,
    pin::{pin, Pin},
    result::Result,
    task::{Context, Poll},
};

use super::{JoinHandle, TaskSpawner};

#[derive(Copy, Clone, Default)]
pub struct TokioSpawner;

impl TaskSpawner for TokioSpawner {
    fn spawn<F>(&self, f: F) -> Box<dyn JoinHandle<F::Output> + Unpin>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        Box::new(TokioJoinHandle(tokio::spawn(f)))
    }
}

pub struct TokioJoinHandle<T>(tokio::task::JoinHandle<T>);

impl<T> Future for TokioJoinHandle<T> {
    type Output = Result<T, Box<dyn Error>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        pin!(&mut self.0).poll(cx).map_err(|e| Box::new(e).into())
    }
}

impl<T: Send> JoinHandle<T> for TokioJoinHandle<T> {}
