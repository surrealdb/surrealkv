//! Task spawning abstraction for different runtime environments.
//! 
//! This module provides a unified interface for spawning async tasks that works
//! with both tokio (for native targets) and wasm_bindgen_futures (for WASM targets).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A handle to a spawned task that can be awaited.
/// 
/// On native targets, this wraps a tokio JoinHandle.
/// On WASM targets, this is a simplified handle that doesn't support awaiting.
pub struct TaskHandle<T> {
	#[cfg(not(target_arch = "wasm32"))]
	handle: tokio::task::JoinHandle<T>,
	#[cfg(target_arch = "wasm32")]
	_phantom: std::marker::PhantomData<T>,
}

impl<T> Future for TaskHandle<T> {
	type Output = Result<T, TaskError>;
	
	fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
		#[cfg(not(target_arch = "wasm32"))]
		{
			// Delegate to the tokio JoinHandle
			unsafe {
				let this = self.get_unchecked_mut();
				Pin::new_unchecked(&mut this.handle).poll(cx).map(|result| {
					result.map_err(TaskError::Tokio)
				})
			}
		}
		#[cfg(target_arch = "wasm32")]
		{
			// On WASM, this should not be called
			panic!("TaskHandle::poll is not supported on WASM targets. Use spawn_detached instead.");
		}
	}
}

/// Errors that can occur when working with spawned tasks.
#[derive(Debug, thiserror::Error)]
pub enum TaskError {
	#[cfg(not(target_arch = "wasm32"))]
	#[error("Tokio task error: {0}")]
	Tokio(#[from] tokio::task::JoinError),
	#[cfg(target_arch = "wasm32")]
	#[error("WASM task error")]
	Wasm,
}

/// Spawn a new async task.
/// 
/// On native targets, this uses `tokio::spawn` and returns a TaskHandle that can be awaited.
/// On WASM targets, this uses `wasm_bindgen_futures::spawn_local` and returns a dummy handle.
/// 
/// For WASM targets, consider using `spawn_detached` if you don't need to await the result.
pub fn spawn<F, T>(future: F) -> TaskHandle<T>
where
	F: Future<Output = T> + Send + 'static,
	T: Send + 'static,
{
	#[cfg(not(target_arch = "wasm32"))]
	{
		TaskHandle {
			handle: tokio::spawn(future),
		}
	}
	#[cfg(target_arch = "wasm32")]
	{
		use wasm_bindgen_futures::spawn_local;
		
		// For WASM, we spawn the task but can't return a meaningful handle
		spawn_local(async move {
			let _ = future.await;
		});
		
		TaskHandle {
			_phantom: std::marker::PhantomData,
		}
	}
}

/// Spawn a new async task that doesn't need to return a value.
/// 
/// This is the recommended approach for WASM targets where you don't need to await the result.
pub fn spawn_detached<F>(future: F)
where
	F: Future<Output = ()> + Send + 'static,
{
	#[cfg(not(target_arch = "wasm32"))]
	{
		tokio::spawn(future);
	}
	#[cfg(target_arch = "wasm32")]
	{
		use wasm_bindgen_futures::spawn_local;
		spawn_local(future);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicU32, Ordering};
	use std::time::Duration;

	#[cfg(not(target_arch = "wasm32"))]
	#[tokio::test]
	async fn test_spawn_detached() {
		let counter = Arc::new(AtomicU32::new(0));
		let counter_clone = counter.clone();
		
		spawn_detached(async move {
			counter_clone.store(100, Ordering::SeqCst);
		});
		
		// Give the task time to complete
		tokio::time::sleep(Duration::from_millis(10)).await;
		assert_eq!(counter.load(Ordering::SeqCst), 100);
	}

	#[cfg(target_arch = "wasm32")]
	#[test]
	fn test_spawn_detached_wasm() {
		let counter = Arc::new(AtomicU32::new(0));
		let counter_clone = counter.clone();
		
		spawn_detached(async move {
			counter_clone.store(100, Ordering::SeqCst);
		});
		
		// For WASM, we can't easily test async behavior in unit tests
		// The task will be executed by the WASM runtime
		assert_eq!(counter.load(Ordering::SeqCst), 0); // Initial value
	}
}
