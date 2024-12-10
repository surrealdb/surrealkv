use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use dashmap::DashMap;
use parking_lot::Mutex;

use surrealkv::storage::log::{Error, Options, Result, Segment};

pub struct SysFile {
    file: Mutex<Segment>,
    in_use: AtomicBool,
}

impl SysFile {
    fn new(dir: &Path, id: u64, opts: Arc<Options>) -> Result<Self> {
        Ok(SysFile {
            file: Mutex::new(Segment::open(dir, id, &opts, true)?),
            in_use: AtomicBool::new(false),
        })
    }

    fn read_at(&self, buf: &mut [u8], off: u64) -> Result<usize> {
        self.file.lock().read_at(buf, off)
    }
}

struct FileHandle {
    segment_id: u64,
    dir: PathBuf,
    file_descriptors: Vec<Arc<SysFile>>,
    max_fds: usize,
    opts: Arc<Options>,
}

impl FileHandle {
    fn new(dir: &Path, segment_id: u64, opts: Arc<Options>, max_fds: usize) -> Result<Self> {
        let file = SysFile::new(dir, segment_id, opts.clone())?.into();
        Ok(FileHandle {
            segment_id,
            dir: dir.to_path_buf(),
            file_descriptors: vec![(file)],
            max_fds,
            opts,
        })
    }

    fn get_fd(&mut self) -> Result<Arc<SysFile>> {
        // Try to find and mark an unused file descriptor atomically
        for fd in &self.file_descriptors {
            // Try to atomically swap from false to true
            // Only succeeds if it was actually false
            if !fd.in_use.load(Ordering::Relaxed)
                && !fd
                    .in_use
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                    .is_err()
            {
                return Ok(fd.clone());
            }
        }

        // No unused file descriptor found, create new one if under limit
        if self.file_descriptors.len() < self.max_fds {
            let new_file: Arc<SysFile> =
                SysFile::new(&self.dir, self.segment_id, self.opts.clone())?.into();
            new_file.in_use.store(true, Ordering::SeqCst);
            self.file_descriptors.push(new_file);
            Ok(self.file_descriptors.last().unwrap().clone())
        } else {
            Err(io::Error::new(io::ErrorKind::Other, "Maximum file descriptors reached").into())
        }
    }

    fn release_fd(&mut self, fd: Arc<SysFile>) {
        if let Some(fd) = self
            .file_descriptors
            .iter_mut()
            .find(|sf| Arc::as_ptr(sf) == Arc::as_ptr(&fd))
        {
            fd.in_use.store(false, Ordering::SeqCst);
        }
    }
}

pub struct FileDescriptorTable {
    table: DashMap<u64, Arc<Mutex<FileHandle>>>,
    max_fds_per_file: usize,
}

impl FileDescriptorTable {
    pub fn new(max_fds_per_file: usize) -> Self {
        FileDescriptorTable {
            table: DashMap::new(),
            max_fds_per_file,
        }
    }

    pub fn insert_segment(&self, dir: &Path, segment_id: u64, opts: Arc<Options>) -> Result<()> {
        let file_handle = FileHandle::new(dir, segment_id, opts.clone(), self.max_fds_per_file)?;
        self.table
            .insert(segment_id, Arc::new(Mutex::new(file_handle)));
        Ok(())
    }

    pub fn read_at(&self, segment_id: u64, buf: &mut [u8], offset: u64) -> Result<usize> {
        self.table
            .get(&segment_id)
            .ok_or(Error::SegmentNotFound)
            .and_then(|handle| {
                let mut handle = handle.lock();
                let file = handle.get_fd()?;
                let result = file.read_at(buf, offset);
                handle.release_fd(file);
                result
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tempdir::TempDir;
    use tokio::task;

    fn assert_send_sync<T: Send + Sync>() {}
    #[test]
    fn test_send_sync() {
        assert_send_sync::<FileDescriptorTable>();
    }

    fn create_segment_file(temp_dir: &TempDir, id: u64) -> Arc<Options> {
        // Create segment options and open a segment
        let opts = Arc::new(Options::default());
        let mut segment =
            Segment::open(temp_dir.path(), id, &opts, false).expect("should create segment");
        let r = segment.append(&[0, 1, 2, 3]);
        assert_eq!(r, Ok(0));
        segment.close().unwrap();
        opts
    }

    #[tokio::test]
    async fn test_basic_operations() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        let table = FileDescriptorTable::new(10);
        let segment_id = 1;

        let opts = create_segment_file(&temp_dir, segment_id);

        table
            .insert_segment(temp_dir.path(), segment_id, opts)
            .unwrap();

        let mut buffer = [0; 4];
        table.read_at(segment_id, &mut buffer, 0).unwrap();

        assert_eq!(&buffer, &[0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");
        let table = Arc::new(FileDescriptorTable::new(10));
        let segment_id = 1;
        let opts = create_segment_file(&temp_dir, segment_id);

        table
            .insert_segment(temp_dir.path(), segment_id, opts)
            .unwrap();

        let mut handles = vec![];

        for _ in 0..10 {
            let table_clone = table.clone();
            handles.push(task::spawn(async move {
                let mut buffer = [0; 4];
                table_clone.read_at(segment_id, &mut buffer, 0).unwrap();
                assert_eq!(&buffer, &[0, 1, 2, 3]);
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }
}
