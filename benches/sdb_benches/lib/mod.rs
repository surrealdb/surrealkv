use criterion::{Criterion, Throughput};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use surrealkv::Options;
use surrealkv::Store as Datastore;
use tempdir::TempDir;

mod routines;

static DB: OnceLock<Arc<Datastore>> = OnceLock::new();

fn create_temp_directory() -> TempDir {
    TempDir::new("test").unwrap()
}

pub(super) async fn init() {
    let mut opts = Options::new();
    opts.dir = create_temp_directory().path().to_path_buf();
    let ds = Datastore::new(opts).expect("should create store");

    let _ = DB.set(Arc::new(ds));
}

pub(super) fn benchmark_group(c: &mut Criterion) {
    let num_ops = *super::NUM_OPS;
    let runtime = super::rt();

    runtime.block_on(async { init().await });

    let mut group = c.benchmark_group("surrealkv");

    group.measurement_time(Duration::from_secs(*super::DURATION_SECS));
    group.sample_size(*super::SAMPLE_SIZE);
    group.throughput(Throughput::Elements(1));

    group.bench_function("creates", |b| {
        routines::bench_routine(
            b,
            DB.get().unwrap().clone(),
            routines::Create::new(super::rt()),
            num_ops,
        )
    });
    group.finish();
}
