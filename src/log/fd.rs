use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::{collections::VecDeque, sync::atomic::AtomicUsize};

use parking_lot::Mutex;

use crate::log::{Options, Result, Segment};

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

    pub fn acquire_reader(&self) -> Result<PooledReader> {
        let mut readers = self.readers.lock();
        let segment = match readers.pop_front() {
            Some(reader) => reader,
            None if self.active_readers.load(Ordering::Acquire) < self.pool_size => {
                // Create new reader if under pool size limit
                self.active_readers.fetch_add(1, Ordering::AcqRel);
                Segment::open(&self.dir, self.id, &self.opts, true)?
            }
            None => return Err(super::Error::NoAvailableReaders),
        };

        Ok(PooledReader {
            segment: Some(segment),
            pool: self,
        })
    }

    pub fn return_reader(&self, reader: Segment) {
        let mut readers = self.readers.lock();
        if readers.len() < self.pool_size {
            readers.push_back(reader);
        } else {
            // Reader will be dropped and active_readers decremented
            self.active_readers.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

pub struct PooledReader<'a> {
    pub segment: Option<Segment>,
    pool: &'a SegmentReaderPool,
}

impl<'a> Drop for PooledReader<'a> {
    fn drop(&mut self) {
        if let Some(reader) = self.segment.take() {
            self.pool.return_reader(reader);
        }
    }
}
