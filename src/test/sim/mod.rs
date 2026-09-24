pub mod generator;
pub mod harness;
pub mod model;

#[cfg(test)]
mod tests {
	use super::harness::SimRunner;
	use test_log::test;

	#[test(tokio::test)]
	async fn test_dst_differential_seeds() {
		// Read environment variables or default to a robust testing set
		let steps: usize =
			std::env::var("SURREALKV_SIM_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(500);

		let seed_count: usize =
			std::env::var("SURREALKV_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(10);

		let base_seeds: Vec<u64> =
			vec![1, 42, 1337, 2026, 99999, 777777, 1234567, 3141592, 2718281, 8888888];
		let seeds: Vec<u64> = if seed_count <= base_seeds.len() {
			base_seeds[..seed_count].to_vec()
		} else {
			let mut extended = base_seeds;
			for i in 10..seed_count {
				extended.push((i as u64).wrapping_mul(6364136223846793005).wrapping_add(1));
			}
			extended
		};

		for seed in seeds {
			let mut runner = SimRunner::new();
			runner.run(seed, steps).await;
			runner.close().await;
		}
	}
}
