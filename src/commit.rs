use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use tokio::sync::{oneshot, Mutex, Notify, Semaphore};

use crate::{
    batch::Batch,
    error::{Error, Result},
};

// pub(super) static SKV_COMMIT_POOL: OnceLock<affinitypool::Threadpool> = OnceLock::new();

// pub(super) fn commit_pool() -> &'static affinitypool::Threadpool {
//     SKV_COMMIT_POOL.get_or_init(|| {
//         affinitypool::Builder::new()
//             .thread_name("surrealkv-commitpool")
//             .thread_stack_size(5 * 1024 * 1024)
//             .thread_per_core(true)
//             .worker_threads(32)
//             .build()
//     })
// }

// Trait for commit operations
pub trait CommitEnv: Send + Sync + 'static {
    // Write batch to WAL (synchronous operation)
    fn write(&self, batch: &Batch, seq_num: u64, sync_wal: bool) -> Result<()>;

    // Apply batch to memtable (can be called concurrently)
    fn apply(&self, batch: &Batch, seq_num: u64) -> Result<()>;
}

// Represents a batch in the commit pipeline
struct CommitBatch {
    // The batch data
    batch: Batch,
    // Assigned sequence number
    seq_num: AtomicU64,
    // Whether batch has been applied
    applied: AtomicBool,
    // Whether to sync WAL
    sync_wal: bool,
    // Channel to notify completion - using Mutex for safe interior mutability
    complete_tx: Mutex<Option<oneshot::Sender<Result<()>>>>,
}

impl CommitBatch {
    fn new(batch: Batch, sync_wal: bool) -> (Arc<Self>, oneshot::Receiver<Result<()>>) {
        let (tx, rx) = oneshot::channel();
        let commit = Arc::new(Self {
            batch,
            seq_num: AtomicU64::new(0),
            applied: AtomicBool::new(false),
            sync_wal,
            complete_tx: Mutex::new(Some(tx)),
        });
        (commit, rx)
    }

    fn set_seq_num(&self, seq: u64) {
        self.seq_num.store(seq, Ordering::Release);
    }

    fn get_seq_num(&self) -> u64 {
        self.seq_num.load(Ordering::Acquire)
    }

    fn mark_applied(&self) {
        self.applied.store(true, Ordering::Release);
    }

    fn is_applied(&self) -> bool {
        self.applied.load(Ordering::Acquire)
    }

    async fn complete(&self, result: Result<()>) {
        if let Some(tx) = self.complete_tx.lock().await.take() {
            let _ = tx.send(result);
        }
    }
}

// Manages sequence number publishing with proper ordering
struct PublishManager {
    // Ordered queue of batches pending publication
    pending: Mutex<std::collections::BTreeMap<u64, Arc<CommitBatch>>>,
    // Notification for new work
    notify: Notify,
    // Current visible sequence number
    visible_seq_num: Arc<AtomicU64>,
    // Shutdown flag
    shutdown: AtomicBool,
}

impl PublishManager {
    fn new(visible_seq_num: Arc<AtomicU64>) -> Self {
        Self {
            pending: Mutex::new(std::collections::BTreeMap::new()),
            notify: Notify::new(),
            visible_seq_num,
            shutdown: AtomicBool::new(false),
        }
    }

    async fn add_batch(&self, batch: Arc<CommitBatch>) {
        let seq_num = batch.get_seq_num();
        self.pending.lock().await.insert(seq_num, batch);
        self.notify.notify_one();
    }

    fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.notify.notify_one();
    }

    async fn run(&self) {
        loop {
            // Try to publish as many sequential batches as possible
            loop {
                let next_batch = {
                    let mut pending = self.pending.lock().await;
                    let current_visible = self.visible_seq_num.load(Ordering::Acquire);
                    let next_seq = if current_visible == 0 {
                        1
                    } else {
                        current_visible + 1
                    };

                    // Check if we have the next batch and it's applied
                    if let Some(batch) = pending.get(&next_seq) {
                        if batch.is_applied() {
                            pending.remove(&next_seq)
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                };
                if let Some(batch) = next_batch {
                    let new_visible = batch.get_seq_num() + batch.batch.count() as u64 - 1;
                    self.visible_seq_num.store(new_visible, Ordering::Release);

                    batch.complete(Ok(())).await;
                } else {
                    break;
                }
            }

            // Check for shutdown
            if self.shutdown.load(Ordering::Acquire) {
                let is_empty = self.pending.lock().await.is_empty();
                if is_empty {
                    break;
                }
            }

            // Wait for notification or timeout
            tokio::select! {
                _ = self.notify.notified() => {},
                _ = tokio::time::sleep(Duration::from_millis(10)) => {
                    // Periodic check
                }
            }
        }
    }
}

// The commit pipeline
pub struct CommitPipeline {
    // Environment for operations
    env: Arc<dyn CommitEnv>,
    // Next sequence number
    log_seq_num: AtomicU64,
    // Visible sequence number
    visible_seq_num: Arc<AtomicU64>,
    // Write mutex for serializing WAL writes
    write_mutex: Arc<Mutex<()>>,
    // Publish manager
    publish_manager: Arc<PublishManager>,
    // Publish manager task handle
    publish_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    // Semaphores for concurrency control
    commit_sem: Arc<Semaphore>,
    sync_sem: Arc<Semaphore>,
    // Shutdown flag
    shutdown: AtomicBool,
}

const MAX_CONCURRENT_COMMITS: usize = 31;

impl CommitPipeline {
    pub fn new(env: Arc<dyn CommitEnv>) -> Arc<Self> {
        let visible_seq_num = Arc::new(AtomicU64::new(0));
        let publish_manager = Arc::new(PublishManager::new(visible_seq_num.clone()));

        // Start the publish manager task
        let manager = publish_manager.clone();
        let publish_task = tokio::spawn(async move {
            manager.run().await;
        });

        Arc::new(Self {
            env,
            log_seq_num: AtomicU64::new(1),
            visible_seq_num,
            write_mutex: Arc::new(Mutex::new(())),
            publish_manager,
            publish_task: Mutex::new(Some(publish_task)),
            commit_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_COMMITS)),
            sync_sem: Arc::new(Semaphore::new(MAX_CONCURRENT_COMMITS)),
            shutdown: AtomicBool::new(false),
        })
    }

    pub fn set_seq_num(&self, seq_num: u64) {
        if seq_num > 0 {
            self.visible_seq_num.store(seq_num, Ordering::Release);
            self.log_seq_num.store(seq_num, Ordering::Release);
        }
    }

    pub async fn commit(&self, batch: Batch, sync_wal: bool) -> Result<()> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(Error::PipelineStall);
        }

        if batch.is_empty() {
            return Ok(());
        }

        // Acquire permits
        let _commit_permit = self
            .commit_sem
            .acquire()
            .await
            .map_err(|_| Error::PipelineStall)?;

        let _sync_permit = if sync_wal {
            Some(
                self.sync_sem
                    .acquire()
                    .await
                    .map_err(|_| Error::PipelineStall)?,
            )
        } else {
            None
        };

        // Create commit batch
        let (commit_batch, complete_rx) = CommitBatch::new(batch, sync_wal);

        // Phase 1: Prepare (assign sequence number and write to WAL)
        let starting_seq_num = self.prepare(commit_batch.clone()).await?;

        // Phase 2: Apply to memtable (concurrent)
        let env = self.env.clone();
        let apply_batch = commit_batch.clone();

        // let apply_handle = commit_pool().spawn(move || env.apply(&batch_clone.batch));

        // let apply_handle = tokio::spawn(async move {
        //     // Run apply in blocking context since it might be CPU intensive
        //     tokio::task::spawn_blocking(move || env.apply(&batch_clone.batch))
        //         .await
        //         .unwrap()
        // });

        let apply_handle = tokio::spawn(async move {
            env.apply(&apply_batch.batch, starting_seq_num)
            // // Run apply in blocking context since it might be CPU intensive
            // tokio::task::spawn_blocking(move || env.apply(&batch_clone.batch))
            //     .await
            //     .unwrap()
        });

        // Wait for apply to complete
        match apply_handle.await {
            Ok(res) => match res {
                Ok(_) => {
                    commit_batch.mark_applied();
                }
                Err(e) => {
                    let err = Error::CommitFail(e.to_string());
                    commit_batch.complete(Err(err.clone())).await;
                    return Err(err);
                }
            },
            Err(_) => {
                commit_batch.mark_applied();
                let err = Error::PipelineStall;
                commit_batch.complete(Err(err.clone())).await;
                return Err(err);
            }
        }

        // Phase 3: Publish
        self.publish_manager.add_batch(commit_batch).await;

        // Wait for completion
        complete_rx.await.map_err(|_| Error::PipelineStall)?
    }

    async fn prepare(&self, commit_batch: Arc<CommitBatch>) -> Result<u64> {
        // Serialize WAL writes
        let _guard = self.write_mutex.lock().await;

        // Assign sequence number
        let count = commit_batch.batch.count() as u64;

        let seq_num = self.log_seq_num.fetch_add(count, Ordering::SeqCst);

        // println!("Assigned seq_num {} to batch (count={})", seq_num, count);

        commit_batch.set_seq_num(seq_num);

        // Write to WAL in blocking context
        let env = self.env.clone();
        let sync_wal = commit_batch.sync_wal;

        // commit_pool().spawn(move || env.write(&batch_for_wal, sync_wal)).await

        // tokio::task::spawn_blocking(move || env.write(&batch_for_wal, sync_wal))
        //     .await
        //     .map_err(|_| Error::PipelineStall)?

        env.write(&commit_batch.batch, seq_num, sync_wal)?;
        // let _ = tokio::spawn(async move { env.write(&commit_batch.batch, seq_num, sync_wal) })
        //     .await
        //     .map_err(|_| Error::PipelineStall)?;
        // tokio::task::spawn(move || env.write(&batch_for_wal, sync_wal))
        //     .await
        //     .map_err(|_| Error::PipelineStall)?
        Ok(seq_num)
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        // Shutdown publish manager
        self.publish_manager.shutdown();

        // Wait for publish task to complete
        if let Some(task) = self.publish_task.lock().await.take() {
            let _ = task.await;
        }
    }

    pub fn get_visible_seq_num(&self) -> u64 {
        self.visible_seq_num.load(Ordering::Acquire)
    }
}

impl Drop for CommitPipeline {
    fn drop(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Release);
        self.publish_manager.shutdown();
    }
}

#[cfg(test)]
mod tests {
    use crate::sstable::InternalKeyKind;

    use super::*;

    struct MockEnv;

    impl CommitEnv for MockEnv {
        fn write(&self, _batch: &Batch, _seq_num: u64, _sync_wal: bool) -> Result<()> {
            Ok(())
        }

        fn apply(&self, _batch: &Batch, _seq_num: u64) -> Result<()> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_single_commit() {
        let pipeline = CommitPipeline::new(Arc::new(MockEnv));

        let mut batch = Batch::new();
        batch
            .add_record(InternalKeyKind::Set, b"key1", Some(b"value1"))
            .unwrap();
        println!("Batch count: {}", batch.count());

        let result = pipeline.commit(batch, false).await;
        assert!(result.is_ok(), "Single commit failed: {:?}", result);

        // Wait a bit for publishing
        tokio::time::sleep(Duration::from_millis(50)).await;

        let visible = pipeline.get_visible_seq_num();
        println!("Visible seq num after single commit: {}", visible);
        assert_eq!(
            visible, 1,
            "Expected visible=1 after one commit with count=1 (highest seq num used)"
        );

        pipeline.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_sequential_commits() {
        let pipeline = CommitPipeline::new(Arc::new(MockEnv));
        println!(
            "initial visible seq num: {}",
            pipeline.get_visible_seq_num()
        );

        // First test sequential commits to verify basic functionality
        for i in 0..5 {
            let mut batch = Batch::new();
            batch
                .add_record(
                    InternalKeyKind::Set,
                    &format!("key{}", i).into_bytes(),
                    Some(&[1, 2, 3]),
                )
                .unwrap();
            // println!("Batch {} count: {}", i, batch.count());
            let result = pipeline.commit(batch, false).await;
            assert!(
                result.is_ok(),
                "Sequential commit {} failed: {:?}",
                i,
                result
            );
            println!(
                "visible seq num after commit {}: {}",
                i,
                pipeline.get_visible_seq_num()
            );
        }

        println!("Final visible seq num: {}", pipeline.get_visible_seq_num());
        assert_eq!(pipeline.get_visible_seq_num(), 5);

        pipeline.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_concurrent_commits() {
        let pipeline = CommitPipeline::new(Arc::new(MockEnv));

        let mut handles = vec![];
        for i in 0..10 {
            let pipeline = pipeline.clone();
            let handle = tokio::spawn(async move {
                let mut batch = Batch::new();
                batch
                    .add_record(
                        InternalKeyKind::Set,
                        &format!("key{}", i).into_bytes(),
                        Some(&[1, 2, 3]),
                    )
                    .unwrap();
                pipeline.commit(batch, false).await
            });
            handles.push(handle);
        }

        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Commit {} failed: {:?}", i, result);
        }

        // Give time for all publishing to complete
        let start = std::time::Instant::now();
        while pipeline.get_visible_seq_num() < 10 && start.elapsed() < Duration::from_secs(5) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Verify sequence numbers are published correctly
        assert_eq!(
            pipeline.get_visible_seq_num(),
            10,
            "Not all batches were published"
        );

        // Shutdown the pipeline
        pipeline.shutdown().await;
    }

    struct DelayedMockEnv;

    impl CommitEnv for DelayedMockEnv {
        fn write(&self, _batch: &Batch, _seq_num: u64, _sync_wal: bool) -> Result<()> {
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_micros(100) {
                std::hint::spin_loop();
            }
            Ok(())
        }

        fn apply(&self, _batch: &Batch, _seq_num: u64) -> Result<()> {
            let start = std::time::Instant::now();
            while start.elapsed() < Duration::from_micros(50) {
                std::hint::spin_loop();
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_concurrent_commits_with_delays() {
        let pipeline = CommitPipeline::new(Arc::new(DelayedMockEnv));

        let mut handles = vec![];
        for i in 0..5 {
            let pipeline = pipeline.clone();
            let handle = tokio::spawn(async move {
                let mut batch = Batch::new();
                batch
                    .add_record(
                        InternalKeyKind::Set,
                        &format!("key{}", i).into_bytes(),
                        Some(&[1, 2, 3]),
                    )
                    .unwrap();
                pipeline.commit(batch, false).await
            });
            handles.push(handle);
        }

        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Verify sequence numbers are published correctly
        assert_eq!(pipeline.get_visible_seq_num(), 5);

        // Shutdown the pipeline
        pipeline.shutdown().await;
    }
}
