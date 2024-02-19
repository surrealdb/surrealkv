use std::sync::Arc;

use surrealkv::Store as Datastore;
use tokio::{runtime::Runtime, task::JoinSet};

pub struct Create {
    runtime: &'static Runtime,
}

impl Create {
    pub fn new(runtime: &'static Runtime) -> Self {
        Self { runtime }
    }
}

impl super::Routine for Create {
    fn setup(&self, _ds: Arc<Datastore>, _num_ops: usize) {
        self.runtime.block_on(async {});
    }

    fn run(&self, ds: Arc<Datastore>, num_ops: usize) {
        self.runtime.block_on(async {
            // Spawn one task for each operation
            let mut tasks = JoinSet::default();
            for _ in 0..num_ops {
                let ds = ds.clone();

                tasks.spawn_on(
                    async move {
                        {
                            let mut txn = ds.begin().unwrap();
                            let key = nanoid::nanoid!();
                            let value = nanoid::nanoid!();
                            txn.set(key.as_bytes(), value.as_bytes()).unwrap();
                            txn.commit().await.expect("[setup] create record failed")
                        };
                    },
                    self.runtime.handle(),
                );
            }

            while let Some(task) = tasks.join_next().await {
                task.unwrap();
            }
        });
    }

    fn cleanup(&self, _ds: Arc<Datastore>, _num_ops: usize) {
        self.runtime.block_on(async {});
    }
}
