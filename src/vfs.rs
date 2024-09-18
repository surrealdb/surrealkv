pub use std::fs::{File, OpenOptions};
use std::{
    fs::{Metadata, Permissions, ReadDir},
    io,
    path::Path,
};

pub trait FileSystem: Send + Sync + 'static {
    fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<()>;
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadDir>;
    fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File>;
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata>;
    fn copy<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<u64>;
    fn set_permissions<P: AsRef<Path>>(&self, path: P, perm: Permissions) -> io::Result<()>;
    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<File>;
    fn copy_dir_all(&self, src: &Path, dst: &Path) -> io::Result<()> {
        if !dst.exists() {
            self.create_dir_all(dst)?;
        }
        for entry in self.read_dir(src)? {
            let entry = entry?;
            let ty = entry.file_type()?;
            if ty.is_dir() {
                self.copy_dir_all(&entry.path(), &dst.join(entry.file_name()))?;
            } else {
                self.copy(entry.path(), dst.join(entry.file_name()))?;
            }
        }
        Ok(())
    }
}

pub struct Dummy;
impl FileSystem for Dummy {
    fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<()> {
        std::fs::rename(from, to)
    }
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::remove_file(path)
    }
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadDir> {
        std::fs::read_dir(path)
    }
    fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::remove_dir_all(path)
    }
    fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()> {
        std::fs::create_dir_all(path)
    }
    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        std::fs::File::open(path)
    }
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata> {
        std::fs::metadata(path)
    }
    fn copy<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<u64> {
        std::fs::copy(from, to)
    }
    fn set_permissions<P: AsRef<Path>>(&self, path: P, perm: Permissions) -> io::Result<()> {
        std::fs::set_permissions(path, perm)
    }
    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<File> {
        std::fs::File::create(path)
    }
}
