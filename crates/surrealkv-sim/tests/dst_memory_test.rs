//! The differential test of `dst_test`, over the memory file system, with a crash for every
//! restart.
//!
//! A restart here is `simulate_crash`: the database is stopped without a close and the memory is
//! mounted as one of the ways of crashing left it. The recovered database has to be the last state
//! that was made durable or the state after a later commit, and the model is rewound to it.

use surrealkv_sim::SimRunner;
use test_log::test;

#[test(tokio::test(flavor = "multi_thread"))]
async fn test_dst_differential_seeds_over_memory_with_crashes() {
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
	let mut join_set = tokio::task::JoinSet::new();
	let mut crashes = (0usize, 0usize);

	for seed in seeds {
		while join_set.len() >= parallelism {
			if let Some(res) = join_set.join_next().await {
				let (crashed, lost) = res.unwrap();
				crashes = (crashes.0 + crashed, crashes.1 + lost);
			}
		}

		join_set.spawn(async move {
			let mut runner = SimRunner::in_memory();
			runner.run(seed, steps).await;
			let crashes = runner.crashes();
			runner.close().await;
			crashes
		});
	}

	while let Some(res) = join_set.join_next().await {
		let (crashed, lost) = res.unwrap();
		crashes = (crashes.0 + crashed, crashes.1 + lost);
	}
	eprintln!("{} crashes, {} acknowledged commits lost", crashes.0, crashes.1);
	assert!(crashes.0 > 0, "the workloads crash");
	assert!(
		steps < 500 || crashes.1 > 0,
		"some crash lost a commit that was not synced, or the crashes are not crashes"
	);
}
