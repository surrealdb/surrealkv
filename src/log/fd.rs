use std::path::PathBuf;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use crossbeam_queue::ArrayQueue;

use crate::log::{Error, Options, Result, Segment};

/// Pool of segment readers for a specific segment
pub struct SegmentReaderPool {
    readers: ArrayQueue<Segment>,
    dir: PathBuf,
    id: u64,
    opts: Options,
    pool_size: usize,
    active_readers: AtomicUsize,
}

impl SegmentReaderPool {
    pub fn new(dir: PathBuf, id: u64, opts: Options, pool_size: usize) -> Result<Self> {
        Ok(Self {
            readers: ArrayQueue::new(pool_size),
            dir,
            id,
            opts,
            pool_size,
            active_readers: AtomicUsize::new(0),
        })
    }

    pub fn acquire_reader(self: &Arc<Self>) -> Result<PooledReader> {
        // Attempt to acquire an existing reader from the queue
        if let Some(segment) = self.readers.pop() {
            return Ok(PooledReader {
                segment: Some(segment),
                pool: Arc::clone(self),
            });
        }

        // If no readers are available, try to create a new one
        let mut current = self.active_readers.load(Ordering::Acquire);
        loop {
            if current >= self.pool_size {
                return Err(Error::NoAvailableReaders);
            }

            match self.active_readers.compare_exchange_weak(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    let segment = match Segment::open(&self.dir, self.id, &self.opts, true) {
                        Ok(segment) => segment,
                        Err(e) => {
                            // Rollback the active reader count on failure
                            self.active_readers.fetch_sub(1, Ordering::AcqRel);
                            return Err(e);
                        }
                    };
                    return Ok(PooledReader {
                        segment: Some(segment),
                        pool: Arc::clone(self),
                    });
                }
                Err(actual) => current = actual,
            }
        }
    }

    fn return_reader(&self, reader: Segment) {
        match self.readers.push(reader) {
            Ok(()) => {} // Successfully returned to pool
            Err(reader) => {
                // Queue is full, drop the reader and decrement counter
                drop(reader);
                self.active_readers.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }
}

pub struct PooledReader {
    pub segment: Option<Segment>,
    pool: Arc<SegmentReaderPool>,
}

impl Drop for PooledReader {
    fn drop(&mut self) {
        if let Some(reader) = self.segment.take() {
            self.pool.return_reader(reader);
        }
    }
}
