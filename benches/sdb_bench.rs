mod sdb_benches;

use criterion::{criterion_group, criterion_main, Criterion};

fn bench(c: &mut Criterion) {
    sdb_benches::benchmark_group(c);
}

criterion_group!(benches, bench);

criterion_main!(benches);
