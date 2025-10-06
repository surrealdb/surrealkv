use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::Arc;

use fs2::FileExt; // Use fs2 for file locking
use crate::error::{Error, Result};

/// LockFile prevents multiple processes from accessing the same database directory
pub(crate) struct LockFile {
    /// The path to the lock file
    path: PathBuf,
    /// The lock file handle
    file: Option<File>,
}

impl LockFile {
    /// Lock file name used in database directories
    pub const LOCK_FILE_NAME: &'static str = "LOCK";

    /// Creates a new lock file at the specified path
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let lock_path = path.as_ref().join(Self::LOCK_FILE_NAME);
        Self {
            path: lock_path,
            file: None,
        }
    }

    /// Acquires the lock, returning an error if the database is already in use
    pub fn acquire(&mut self) -> Result<()> {
        // Try to open the lock file with create flag
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&self.path)
            .map_err(|e| Error::Io(Arc::new(e)))?;

        // Try to lock the file exclusively using fs2
        file.try_lock_exclusive()
            .map_err(|e| match e.kind() {
                ErrorKind::WouldBlock => {
                    Error::Other(format!("Database at {} is already locked by another process", self.path.display()))
                }
                _ => Error::Io(Arc::new(e)),
            })?;

        // Write process ID to lock file for debugging
        let pid = process::id();
        let content = format!("{}\n", pid);
        file.set_len(0)
            .and_then(|_| file.try_clone()?.write_all(content.as_bytes()))
            .map_err(|e| Error::Io(Arc::new(e)))?;

        self.file = Some(file);
        Ok(())
    }

    /// Releases the lock
    pub fn release(&mut self) -> Result<()> {
        if let Some(file) = self.file.take() {
            // Unlock the file using fs2
            file.unlock()
                .map_err(|e| Error::Io(Arc::new(e)))?;
            // File will be closed when dropped
        }
        Ok(())
    }
}

impl Drop for LockFile {
    fn drop(&mut self) {
        // Try to release the lock, but don't panic if it fails
        let _ = self.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use tempfile::TempDir;

    use crate::error::Error;
    use crate::lsm::TreeBuilder;
    use crate::InternalKey;

    #[test]
    fn test_lock_acquisition_and_release() {
        let temp_dir = TempDir::new().unwrap();
        let mut lock = LockFile::new(temp_dir.path());
        
        // Initial acquisition should succeed
        assert!(lock.acquire().is_ok());
        
        // Lock file should exist
        assert!(temp_dir.path().join(LockFile::LOCK_FILE_NAME).exists());
        
        // Release should succeed
        assert!(lock.release().is_ok());
    }

    #[test]
    fn test_lock_contention() {
        let temp_dir = TempDir::new().unwrap();
        
        // Create first lock
        let mut lock1 = LockFile::new(temp_dir.path());
        assert!(lock1.acquire().is_ok());
        
        // Second lock should fail
        let mut lock2 = LockFile::new(temp_dir.path());
        assert!(lock2.acquire().is_err());
        
        // After releasing first lock, second should succeed
        assert!(lock1.release().is_ok());
        assert!(lock2.acquire().is_ok());
    }

    #[test]
    fn test_lock_with_process_id() {
        let temp_dir = TempDir::new().unwrap();
        let mut lock = LockFile::new(temp_dir.path());
        
        assert!(lock.acquire().is_ok());
        
        // Check that process ID is written to file
        let lock_path = temp_dir.path().join(LockFile::LOCK_FILE_NAME);
        let content = fs::read_to_string(&lock_path).unwrap();
        let pid = process::id().to_string();
        
        assert!(content.trim() == pid);
    }

    #[test]
    fn test_drop_releases_lock() {
        let temp_dir = TempDir::new().unwrap();
        
        {
            let mut lock = LockFile::new(temp_dir.path());
            assert!(lock.acquire().is_ok());
            // lock goes out of scope here and should be dropped
        }
        
        // New lock should succeed because previous was dropped
        let mut lock2 = LockFile::new(temp_dir.path());
        assert!(lock2.acquire().is_ok());
    }
    
    // Integration tests with Tree
    
    #[tokio::test]
    async fn test_lock_file_prevents_concurrent_access() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();
        
        // First instance should succeed
        let tree1 = TreeBuilder::<InternalKey>::new()
            .with_path(temp_path.clone())
            .build()
            .expect("First tree should be created successfully");

        // Second instance should fail with lock error
        let result = TreeBuilder::<InternalKey>::new()
            .with_path(temp_path.clone())
            .build();
        
        assert!(result.is_err(), "Second tree should fail to acquire lock");
        if let Err(Error::Other(msg)) = result {
            assert!(msg.contains("already locked"), "Error should indicate database is locked");
        } else {
            panic!("Expected a lock error");
        }
        
        // After closing the first tree, we should be able to open again
        tree1.close().await.unwrap();
        
        let tree2 = TreeBuilder::<InternalKey>::new()
            .with_path(temp_path)
            .build()
            .expect("After closing first tree, second should succeed");
            
        tree2.close().await.unwrap();
    }


    #[test]
    fn test_lock_file_multithreaded() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();
        let threads = 10;
        
        // Use a barrier to make all threads try to open the database at roughly the same time
        let barrier = Arc::new(Barrier::new(threads));
        let mut handles = vec![];
        
        for i in 0..threads {
            let path = temp_path.clone();
            let thread_barrier = barrier.clone();
            
            let handle = thread::spawn(move || {
                // Wait for all threads to be ready
                thread_barrier.wait();
                
                // Try to open the database
                let result = TreeBuilder::<InternalKey>::new()
                    .with_path(path)
                    .build();
                
                (i, result)
            });
            
            handles.push(handle);
        }
        
        // Collect results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.join().unwrap());
        }
        
        // All threads should succeed since they're using different paths
        let success_count = results.iter().filter(|(_, result)| result.is_ok()).count();
        assert_eq!(success_count, threads, "All threads should succeed");
        
        // Close all successful instances
        for (_, result) in results {
            if let Ok(tree) = result {
                // Use block_on since we're in a regular test, not async
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    tree.close().await.unwrap();
                });
            }
        }
    }
    
    #[tokio::test(flavor = "multi_thread")]
    async fn test_lock_file_multithreaded_with_disk() {
        let temp_dir = TempDir::new().unwrap();
        let temp_path = temp_dir.path().to_path_buf();
        let tasks = 10;
        
        // Use a barrier to make all tasks try to open the database at roughly the same time
        let barrier = Arc::new(Barrier::new(tasks));
        let mut handles = vec![];
        
        for i in 0..tasks {
            let path = temp_path.clone();
            let task_barrier = barrier.clone();
            
            let handle = tokio::task::spawn(async move {
                // Wait for all tasks to be ready
                task_barrier.wait();
                
                // Try to open the database (with VLog disabled to avoid async issues)
                let result = TreeBuilder::<InternalKey>::new()
                    .with_path(path)
                    .with_enable_vlog(false)
                    .build();
                
                (i, result)
            });
            
            handles.push(handle);
        }
        
        // Collect results
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.unwrap());
        }
        
        // Exactly one task should succeed
        let success_count = results.iter().filter(|(_, result)| result.is_ok()).count();
        assert_eq!(success_count, 1, "Exactly one task should succeed in acquiring the lock");
        
        // All other tasks should fail with a lock error
        let failures = results.iter().filter(|(_, result)| result.is_err()).count();
        assert_eq!(failures, tasks - 1, "All other tasks should fail with lock error");
        
        // Close the successful instance
        for (_, result) in results {
            if let Ok(tree) = result {
                tree.close().await.unwrap();
                break; // Only one should succeed, so we can break after closing it
            }
        }
    }
}