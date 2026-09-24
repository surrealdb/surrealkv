use std::time::Instant;
use surrealkv_sim::SimRunner;

#[tokio::main]
async fn main() {
	let steps: usize =
		std::env::var("SURREALKV_SIM_STEPS").ok().and_then(|s| s.parse().ok()).unwrap_or(5000);

	let seed_count: usize =
		std::env::var("SURREALKV_SIM_SEEDS").ok().and_then(|s| s.parse().ok()).unwrap_or(500);

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

	let parallelism = std::thread::available_parallelism().map_or(4, |n| n.get()).min(64);
	println!(
		"Starting SurrealKV Deterministic Simulation: {} seeds, {} steps/seed ({} total ops), parallelism: {}",
		seeds.len(),
		steps,
		seeds.len() * steps,
		parallelism
	);

	let start = Instant::now();
	let mut join_set = tokio::task::JoinSet::new();

	for seed in seeds {
		while join_set.len() >= parallelism {
			if let Some(res) = join_set.join_next().await {
				res.unwrap();
			}
		}

		join_set.spawn(async move {
			let mut runner = SimRunner::new();
			runner.run(seed, steps).await;
			runner.close().await;
		});
	}

	while let Some(res) = join_set.join_next().await {
		res.unwrap();
	}

	let elapsed = start.elapsed();
	println!("Simulation completed successfully in {elapsed:?} with 0 divergences!");
}
