use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::{collections::VecDeque, sync::atomic::AtomicUsize};

use parking_lot::Mutex;

use crate::log::{Error, Options, Result, Segment};

/// Pool of segment readers for a specific segment
pub struct SegmentReaderPool {
    readers: Mutex<VecDeque<Segment>>,
    dir: PathBuf,
    id: u64,
    opts: Options,
    pool_size: usize,
    active_readers: AtomicUsize,
}

impl SegmentReaderPool {
    pub fn new(dir: PathBuf, id: u64, opts: Options, pool_size: usize) -> Result<Self> {
        let readers = VecDeque::with_capacity(pool_size);

        Ok(Self {
            readers: Mutex::new(readers),
            dir,
            id,
            opts,
            pool_size,
            active_readers: AtomicUsize::new(0),
        })
    }

    pub fn acquire_reader(self: &Arc<Self>) -> Result<PooledReader> {
        let mut readers = self.readers.lock();
        let segment = match readers.pop_front() {
            Some(reader) => reader,
            None => {
                let mut current = self.active_readers.load(Ordering::Acquire);
                loop {
                    if current >= self.pool_size {
                        return Err(Error::NoAvailableReaders);
                    }

                    // Try to update the value atomically
                    match self.active_readers.compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            match Segment::open(&self.dir, self.id, &self.opts, true) {
                                Ok(segment) => break segment,
                                Err(e) => {
                                    // Decrement counter on failure
                                    self.active_readers.fetch_sub(1, Ordering::AcqRel);
                                    return Err(e);
                                }
                            }
                        }
                        Err(actual) => current = actual,
                    }
                }
            }
        };

        Ok(PooledReader {
            segment: Some(segment),
            pool: Arc::clone(self),
        })
    }

    fn return_reader(&self, reader: Segment) {
        let mut readers = self.readers.lock();
        if readers.len() < self.pool_size {
            readers.push_back(reader);
        } else {
            // If we can't reuse this reader, we need to decrement active_readers
            // The reader will be dropped automatically when it goes out of scope
            self.active_readers.fetch_sub(1, Ordering::AcqRel);
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
