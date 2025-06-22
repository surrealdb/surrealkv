pub use std::fs::File as File2;
use std::{
    fs::{Metadata, Permissions, ReadDir},
    io::{self, Read, Seek, Write},
    path::Path,
};

// pub struct OpenOptions;

pub trait File: Send + Sync + Read + Write + Seek + Sized + 'static {
    fn sync_all(&self) -> io::Result<()>;
    fn try_clone(&self) -> io::Result<Self>;
}

impl File for std::fs::File {
    fn sync_all(&self) -> io::Result<()> {
        std::fs::File::sync_all(self)
    }
    fn try_clone(&self) -> io::Result<Self> {
        std::fs::File::try_clone(self)
    }
}

pub trait FileSystem: Send + Sync + Sized + 'static {
    type File: File;

    fn rename<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<()>;
    fn remove_file<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn read_dir<P: AsRef<Path>>(&self, path: P) -> io::Result<ReadDir>;
    fn remove_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn create_dir_all<P: AsRef<Path>>(&self, path: P) -> io::Result<()>;
    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File>;
    fn metadata<P: AsRef<Path>>(&self, path: P) -> io::Result<Metadata>;
    fn copy<P: AsRef<Path>, Q: AsRef<Path>>(&self, from: P, to: Q) -> io::Result<u64>;
    fn set_permissions<P: AsRef<Path>>(&self, path: P, perm: Permissions) -> io::Result<()>;
    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File>;
    fn read_to_string<P: AsRef<Path>>(&self, path: P) -> io::Result<String>;
    fn open_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        option: &OpenOptions<Self>,
    ) -> io::Result<Self::File>;

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

#[derive(Clone, Debug, Copy)]
pub struct OpenOptions<'a, V: FileSystem> {
    parent: &'a V,
    read: bool,
    write: bool,
    append: bool,
    truncate: bool,
    create: bool,
}

unsafe impl<'a, V: FileSystem> Send for OpenOptions<'a, V> {}
unsafe impl<'a, V: FileSystem> Sync for OpenOptions<'a, V> {}

impl<'a, V: FileSystem> OpenOptions<'a, V> {
    pub fn new(parent: &'a V) -> Self {
        OpenOptions {
            parent,
            read: false,
            write: false,
            append: false,
            truncate: false,
            create: false,
        }
    }

    pub fn read(&mut self, read: bool) -> &mut Self {
        self.read = read;
        self
    }

    pub fn write(&mut self, write: bool) -> &mut Self {
        self.write = write;
        self
    }

    pub fn append(&mut self, append: bool) -> &mut Self {
        self.append = append;
        self
    }
    pub fn truncate(&mut self, truncate: bool) -> &mut Self {
        self.truncate = truncate;
        self
    }
    pub fn create(&mut self, create: bool) -> &mut Self {
        self.create = create;
        self
    }

    pub fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<V::File> {
        self.parent.open_with_options(path, self)
    }
}

pub struct Dummy;
impl FileSystem for Dummy {
    type File = std::fs::File;

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
    fn open<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
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
    fn create<P: AsRef<Path>>(&self, path: P) -> io::Result<Self::File> {
        std::fs::File::create(path)
    }
    fn read_to_string<P: AsRef<Path>>(&self, path: P) -> io::Result<String> {
        std::fs::read_to_string(path)
    }

    fn open_with_options<P: AsRef<Path>>(
        &self,
        path: P,
        OpenOptions {
            read,
            write,
            append,
            truncate,
            create,
            ..
        }: &OpenOptions<Self>,
    ) -> io::Result<Self::File> {
        std::fs::OpenOptions::new()
            .read(*read)
            .write(*write)
            .append(*append)
            .truncate(*truncate)
            .create(*create)
            .open(path)
    }
}
