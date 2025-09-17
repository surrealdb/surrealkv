use std::future::Future;

// Task spawning abstraction for different runtime environments.
//
// This function provides a unified interface for spawning async tasks that works
// with both tokio (for native targets) and wasm_bindgen_futures (for WASM
// targets).
pub fn spawn<F>(future: F)
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
