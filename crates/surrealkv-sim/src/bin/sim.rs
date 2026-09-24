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
	let total_seeds = seeds.len();
	let mut completed = 0;
	let report_interval = (total_seeds / 10).max(100);

	for seed in seeds {
		while join_set.len() >= parallelism {
			if let Some(res) = join_set.join_next().await {
				res.unwrap();
				completed += 1;
				if completed % report_interval == 0 {
					let el = start.elapsed();
					let pct = (completed as f64 / total_seeds as f64) * 100.0;
					let ops_sec = (completed * steps) as f64 / el.as_secs_f64();
					println!(
						"Progress: {completed}/{total_seeds} seeds ({pct:.1}%), {el:.1?} elapsed, {ops_sec:.0} ops/sec"
					);
				}
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
		completed += 1;
		if completed % report_interval == 0 && completed < total_seeds {
			let el = start.elapsed();
			let pct = (completed as f64 / total_seeds as f64) * 100.0;
			let ops_sec = (completed * steps) as f64 / el.as_secs_f64();
			println!(
				"Progress: {completed}/{total_seeds} seeds ({pct:.1}%), {el:.1?} elapsed, {ops_sec:.0} ops/sec"
			);
		}
	}

	let elapsed = start.elapsed();
	println!("Simulation completed successfully in {elapsed:?} with 0 divergences!");
}
