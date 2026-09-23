pub mod generator;
pub mod harness;
pub mod model;

#[cfg(test)]
mod tests {
	use super::harness::SimRunner;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_dst_differential_seeds() {
		// Run deterministic simulation across multiple distinct random seeds
		for seed in [1, 42, 1337, 2026, 99999] {
			let mut runner = SimRunner::new();
			runner.run(seed, 200).await;
			runner.close().await;
		}
	}
}
