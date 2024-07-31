mod aol;
pub use aol::Aol;

use ahash::{HashMap, HashMapExt};
use std::fmt;
use std::fs::File;
use std::fs::{read_dir, OpenOptions};
use std::io::BufReader;
use std::io::{self, BufRead, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::PoisonError;

/// The `READ_BUF_SIZE` constant represents the size of a buffer for disk reads from
/// the append-only log.
const READ_BUF_SIZE: usize = 32 * 1024;

/// The magic value for identifying file headers.
///
/// The `MAGIC` constant represents a magic value used in file headers to uniquely identify the
/// format or purpose of the file. It's a hexadecimal value that serves as a signature for file
/// identification and verification.
const MAGIC: u64 = 0xFB98F92D;

/// The version number of the file format.
///
/// The `VERSION` constant represents the version number of the file format used in append-only log
/// segments. It provides information about the structure and layout of the data within the segment.
const VERSION: u64 = 1;

/// Default file mode for newly created files, represented in octal format.
/// The default mode is set to read and write permissions for the owner,
/// and read-only permissions for the group and others.
const DEFAULT_FILE_MODE: u32 = 0o644;

/// Default compression format used for writing data.
/// The data will be compressed using the `DefaultCompression` algorithm.
const DEFAULT_COMPRESSION_FORMAT: CompressionFormat = CompressionFormat::NoCompression;

/// Default compression level used for compression.
/// The data will be compressed using the `DefaultCompression` level.
const DEFAULT_COMPRESSION_LEVEL: CompressionLevel = CompressionLevel::BestSpeed;

/// Default maximum size of the segment file.
const DEFAULT_FILE_SIZE: u64 = 4096 * 256 * 20; // 20mb

/// Default maximum number of open files allowed.
const DEFAULT_MAX_OPEN_FILES: usize = 16;

/// Constants for key names used in the file header.
const KEY_MAGIC: &str = "magic";
const KEY_VERSION: &str = "version";
const KEY_SEGMENT_ID: &str = "segment_id";
const KEY_COMPRESSION_FORMAT: &str = "compression_format";
const KEY_COMPRESSION_LEVEL: &str = "compression_level";
const KEY_MAX_FILE_SIZE: &str = "max_file_size";
const KEY_ADDITIONAL_METADATA: &str = "additional_metadata";

// Enum to represent different compression formats
#[derive(Clone)]
pub enum CompressionFormat {
    NoCompression = 0,
}

impl CompressionFormat {
    fn as_u64(&self) -> u64 {
        match self {
            CompressionFormat::NoCompression => 0,
        }
    }
}

// Enum to represent different compression levels
#[derive(Clone)]
pub enum CompressionLevel {
    BestSpeed = 0,
}

impl CompressionLevel {
    fn as_u64(&self) -> u64 {
        match *self {
            CompressionLevel::BestSpeed => 0,
        }
    }
}

/// Represents options for configuring a segment in a write-ahead log.
///
/// The `Options` struct provides a way to customize various aspects of a write-ahead log segment,
/// such as the file mode, compression settings, metadata, and extension. These options are used
/// when creating a new segment to tailor its behavior according to application requirements.
#[derive(Clone)]
pub struct Options {
    /// The permission mode for creating directories.
    ///
    /// If specified, this option sets the permission mode for creating directories. It determines
    /// the access rights for creating new directories. If not specified, the default directory
    /// creation mode will be used.
    pub(crate) dir_mode: Option<u32>,

    /// The file mode to set for the segment file.
    ///
    /// If specified, this option sets the permission mode for the segment file. It determines who
    /// can read, write, and execute the file. If not specified, the default file mode will be used.
    pub(crate) file_mode: Option<u32>,

    /// The compression format to apply to the segment's data.
    ///
    /// If specified, this option sets the compression format that will be used to compress the
    /// data written to the segment. Compression can help save storage space but might introduce
    /// some overhead in terms of CPU usage during read and write operations.
    pub(crate) compression_format: Option<CompressionFormat>,

    /// The compression level to use with the selected compression format.
    ///
    /// This option specifies the compression level that will be applied when compressing the data.
    /// Higher levels usually provide better compression ratios but require more computational
    /// resources. If not specified, a default compression level will be used.
    pub(crate) compression_level: Option<CompressionLevel>,

    /// The metadata associated with the segment.
    ///
    /// This option allows you to attach metadata to the segment. Metadata can be useful for storing
    /// additional information about the segment's contents or usage. If not specified, no metadata
    /// will be associated with the segment.
    pub(crate) metadata: Option<Metadata>,

    /// The extension to use for the segment file.
    ///
    /// If specified, this option sets the extension for the segment file. The extension is used
    /// when creating the segment file on disk. If not specified, a default extension might be used.
    pub(crate) file_extension: Option<String>,

    /// The maximum size of the segment file.
    ///
    /// If specified, this option sets the maximum size that the segment file is allowed to reach.
    /// Once the file reaches this size, additional writes will be prevented. If not specified,
    /// there is no maximum size limit for the file.
    ///
    /// This is used by aol to cycle segments when the max file size is reached.
    pub(crate) max_file_size: u64,

    /// The maximum number of open files allowed.
    ///
    /// If specified, this option sets the maximum number of open files allowed.
    ///
    /// This is used by aol to initialize the segment cache.
    pub(crate) max_open_files: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            dir_mode: Some(0o750),                                // default directory mode
            file_mode: Some(DEFAULT_FILE_MODE),                   // default file mode
            compression_format: Some(DEFAULT_COMPRESSION_FORMAT), // default compression format
            compression_level: Some(DEFAULT_COMPRESSION_LEVEL),   // default compression level
            metadata: None,                                       // default metadata
            file_extension: None,                                 // default extension
            max_file_size: DEFAULT_FILE_SIZE,                     // default max file size (20mb)
            max_open_files: DEFAULT_MAX_OPEN_FILES,
        }
    }
}

impl Options {
    pub fn validate(&self) -> Result<()> {
        if self.max_file_size == 0 {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::InvalidInput,
                "invalid max_file_size",
            )));
        }

        Ok(())
    }

    #[allow(dead_code)]
    pub fn with_file_mode(mut self, file_mode: u32) -> Self {
        self.file_mode = Some(file_mode);
        self
    }

    #[allow(dead_code)]
    pub fn with_compression_format(mut self, compression_format: CompressionFormat) -> Self {
        self.compression_format = Some(compression_format);
        self
    }

    #[allow(dead_code)]
    pub fn with_compression_level(mut self, compression_level: CompressionLevel) -> Self {
        self.compression_level = Some(compression_level);
        self
    }

    #[allow(dead_code)]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    #[allow(dead_code)]
    pub fn with_file_extension(mut self, extension: String) -> Self {
        self.file_extension = Some(extension);
        self
    }

    #[allow(dead_code)]
    pub fn with_max_file_size(mut self, max_file_size: u64) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    #[allow(dead_code)]
    pub fn with_dir_mode(mut self, dir_mode: u32) -> Self {
        self.dir_mode = Some(dir_mode);
        self
    }
}

/// Represents metadata associated with a file.
///
/// The `Metadata` struct defines a container for storing key-value pairs of metadata. This metadata
/// can be used to hold additional information about a file. The data is stored in a hash map where
/// each key is a string and each value is a vector of bytes.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct Metadata {
    /// The map holding key-value pairs of metadata.
    ///
    /// The `data` field is a hash map that allows associating arbitrary data with descriptive keys.
    /// This can be used, for example, to store additional information about the file as a header.
    data: HashMap<String, Vec<u8>>,
}

impl Metadata {
    // Constructor for Metadata, reads data from bytes if provided
    pub(crate) fn new(b: Option<Vec<u8>>) -> Self {
        let mut metadata = Metadata {
            data: HashMap::new(),
        };

        // If bytes are provided, read data from them using read_from
        if let Some(b) = b {
            metadata.read_from(&mut &b[..]).unwrap();
        }

        metadata
    }

    /// Creates a new `Metadata` instance with file header information.
    ///
    /// This method creates a new `Metadata` instance and sets the key-value pairs corresponding to
    /// the file header information, such as magic number, version, segment ID, compression format,
    /// and compression level.
    ///
    /// # Parameters
    ///
    /// - `id`: The segment ID value.
    /// - `cf`: The compression format value.
    /// - `cl`: The compression level value.
    ///
    /// # Returns
    ///
    /// Returns a `Metadata` instance containing the file header information.
    fn new_file_header(id: u64, opts: &Options) -> Result<Self> {
        let mut buf = Metadata::new(None);

        // Set file header key-value pairs using constants
        let cf = opts
            .compression_format
            .as_ref()
            .unwrap_or(&CompressionFormat::NoCompression);
        let cl = opts
            .compression_level
            .as_ref()
            .unwrap_or(&CompressionLevel::BestSpeed);

        buf.put_uint(KEY_MAGIC, MAGIC);
        buf.put_uint(KEY_VERSION, VERSION);
        buf.put_uint(KEY_SEGMENT_ID, id);
        buf.put_uint(KEY_COMPRESSION_FORMAT, cf.as_u64());
        buf.put_uint(KEY_COMPRESSION_LEVEL, cl.as_u64());
        buf.put_uint(KEY_MAX_FILE_SIZE, opts.max_file_size);
        if let Some(md) = opts.metadata.as_ref() {
            buf.put(KEY_ADDITIONAL_METADATA, &md.to_bytes()?);
        }

        Ok(buf)
    }

    // Returns the serialized bytes representation of Metadata
    pub(crate) fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut b = Vec::new();
        self.write_to(&mut b)?;
        Ok(b)
    }

    // Reads Metadata from a given reader
    pub(crate) fn read_from<R: Read>(&mut self, reader: &mut R) -> Result<()> {
        let mut len_buf = [0; 4];
        reader.read_exact(&mut len_buf)?; // Read 4 bytes for the length
        let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length

        // Loop to read key-value pairs from the reader
        for _i in 0..len {
            let k = read_field(reader)?; // Read key
            let v = read_field(reader)?; // Read value
            self.data.insert(String::from_utf8_lossy(&k).to_string(), v);
        }

        Ok(())
    }

    // Writes Metadata to a given writer
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<()> {
        let mut len_buf = [0; 4];
        len_buf.copy_from_slice(&(self.data.len() as u32).to_be_bytes());
        writer.write_all(&len_buf)?; // Write length

        // Loop to write key-value pairs to the writer
        for (key, value) in &self.data {
            write_field(key.as_bytes(), writer)?; // Write key
            write_field(value, writer)?; // Write value
        }

        Ok(())
    }

    // Puts an integer value with a given key
    pub(crate) fn put_uint(&mut self, key: &str, n: u64) {
        let b = n.to_be_bytes(); // Convert integer to big-endian bytes
        self.put(key, &b); // Call the generic put method
    }

    // Gets an integer value associated with a given key
    pub(crate) fn get_uint(&self, key: &str) -> Result<u64> {
        // // Use the generic get method to retrieve bytes
        let value_bytes = self.get(key).ok_or(Error::IO(IOError::new(
            io::ErrorKind::NotFound,
            "Key not found",
        )))?;

        let bytes: &[u8] = value_bytes.as_slice();
        let value_bytes = bytes.as_ref().try_into().map_err(|_| {
            Error::IO(IOError::new(
                io::ErrorKind::InvalidData,
                "Failed to convert bytes to u64",
            ))
        })?;

        let int_value = u64::from_be_bytes(value_bytes);
        Ok(int_value)
    }

    // Generic method to put a key-value pair into the data HashMap
    pub(crate) fn put(&mut self, key: &str, value: &[u8]) {
        self.data.insert(key.to_string(), value.to_vec());
    }

    // Generic method to get the value associated with a given key
    pub(crate) fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }
}

// Reads a field from the given reader
pub(crate) fn read_field<R: Read>(reader: &mut R) -> Result<Vec<u8>> {
    let mut len_buf = [0; 4];
    reader.read_exact(&mut len_buf)?; // Read 4 bytes for the length
    let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length
    let mut fb = vec![0u8; len];
    reader.read_exact(&mut fb)?; // Read the actual field bytes
    Ok(fb)
}

// Writes a field to the given writer
pub(crate) fn write_field<W: Write>(b: &[u8], writer: &mut W) -> Result<()> {
    let mut len_buf = [0; 4];
    len_buf.copy_from_slice(&(b.len() as u32).to_be_bytes());
    writer.write_all(&len_buf)?; // Write 4 bytes for the length
    writer.write_all(b)?; // Write the actual field bytes
    Ok(())
}

pub(crate) fn read_file_header(file: &mut File) -> Result<Vec<u8>> {
    // Read the header using read_field
    read_field(file)
}

fn write_file_header(file: &mut File, id: u64, opts: &Options) -> Result<usize> {
    // Create a buffer to hold the header
    let mut buf = Vec::new();

    // Write the header using write_field
    let meta = Metadata::new_file_header(id, opts)?;
    write_field(&meta.to_bytes()?, &mut buf)?;

    // Write header to the file
    file.write_all(&buf)?;

    // Sync data to disk and flush metadata
    file.sync_all()?;

    Ok(buf.len())
}

fn validate_file_header(header: &[u8], id: u64, opts: &Options) -> Result<()> {
    validate_magic_version(header)?;
    validate_segment_id(header, id)?;
    validate_compression(header, opts)?;
    validate_metadata(header, opts)?;

    Ok(())
}

fn validate_metadata(header: &[u8], opts: &Options) -> Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;
    let additional_md = meta.get(KEY_ADDITIONAL_METADATA);

    match (additional_md, &opts.metadata) {
        (Some(md), Some(expected_md)) if *md != expected_md.to_bytes()? => Err(Error::IO(
            IOError::new(io::ErrorKind::InvalidData, "Corrupted metadata"),
        )),
        (None, Some(_)) | (Some(_), None) => Err(Error::IO(IOError::new(
            io::ErrorKind::InvalidData,
            "Invalid metadata",
        ))),
        _ => Ok(()),
    }
}

fn validate_magic_version(header: &[u8]) -> Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;

    let magic = meta.get_uint(KEY_MAGIC)?;
    let version = meta.get_uint(KEY_VERSION)?;

    if magic != MAGIC || version != VERSION {
        return Err(Error::IO(IOError::new(
            io::ErrorKind::InvalidData,
            "Invalid header data",
        )));
    }

    Ok(())
}

fn validate_segment_id(header: &[u8], id: u64) -> Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;

    let segment_id = meta.get_uint(KEY_SEGMENT_ID)?;

    if segment_id != id {
        return Err(Error::IO(IOError::new(
            io::ErrorKind::InvalidData,
            "Invalid segment ID",
        )));
    }

    Ok(())
}

fn validate_compression(header: &[u8], opts: &Options) -> Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;

    let cf = meta.get_uint(KEY_COMPRESSION_FORMAT)?;
    let cl = meta.get_uint(KEY_COMPRESSION_LEVEL)?;

    if let Some(expected_cf) = &opts.compression_format {
        if cf != expected_cf.as_u64() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::InvalidData,
                "Invalid compression format",
            )));
        }
    }

    if let Some(expected_cl) = &opts.compression_level {
        if cl != expected_cl.as_u64() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::InvalidData,
                "Invalid compression level",
            )));
        }
    }

    Ok(())
}

fn parse_segment_name(name: &str) -> Result<(u64, Option<String>)> {
    let parts: Vec<&str> = name.split('.').collect();

    if parts.is_empty() {
        return Err(Error::IO(IOError::new(
            io::ErrorKind::InvalidInput,
            "Invalid segment name format",
        )));
    }

    let index = parts[0].parse();
    if let Ok(index) = index {
        if parts.len() == 1 {
            return Ok((index, None));
        } else if parts.len() == 2 {
            return Ok((index, Some(parts[1].to_string())));
        }
    }

    Err(Error::IO(IOError::new(
        io::ErrorKind::InvalidInput,
        "Invalid segment name format",
    )))
}

pub(crate) fn segment_name(index: u64, ext: &str) -> String {
    if ext.is_empty() {
        return format!("{:020}", index);
    }
    format!("{:020}.{}", index, ext)
}

/// Gets the range of segment IDs present in the specified directory.
///
/// This function returns a tuple containing the minimum and maximum segment IDs
/// found in the directory. If no segments are found, the tuple will contain (0, 0).
fn get_segment_range(dir: &Path) -> Result<(u64, u64)> {
    let refs = list_segment_ids(dir)?;
    if refs.is_empty() {
        return Ok((0, 0));
    }
    Ok((refs[0], refs[refs.len() - 1]))
}

/// Lists the segment IDs found in the specified directory.
///
/// This function reads the names of segment files in the directory and extracts the segment IDs.
/// The segment IDs are returned as a sorted vector. If no segment files are found, an empty
/// vector is returned.
fn list_segment_ids(dir: &Path) -> Result<Vec<u64>> {
    let mut refs: Vec<u64> = Vec::new();
    let entries = read_dir(dir)?;

    for entry in entries {
        let file = entry?;

        // Check if the entry is a file
        if std::fs::metadata(file.path())?.is_file() {
            let fn_name = file.file_name();
            let fn_str = fn_name.to_string_lossy();
            let (index, _) = parse_segment_name(&fn_str)?;
            refs.push(index);
        }
    }

    refs.sort();

    Ok(refs)
}

#[derive(Debug)]
pub(crate) struct SegmentRef {
    /// The path where the segment file is located.
    pub(crate) file_path: PathBuf,
    /// The base offset of the file.
    pub(crate) file_header_offset: u64,
    /// The unique identifier of the segment.
    pub(crate) id: u64,
}

impl SegmentRef {
    /// Creates a vector of SegmentRef instances by reading segments in the specified directory.
    pub fn read_segments_from_directory(directory_path: &Path) -> Result<Vec<SegmentRef>> {
        let mut segment_refs = Vec::new();

        // Read the directory and iterate through its entries
        let files = read_dir(directory_path)?;
        for file in files {
            let entry = file?;
            if entry.file_type()?.is_file() {
                let file_path = entry.path();
                let fn_name = entry.file_name();
                let fn_str = fn_name.to_string_lossy();
                let (index, _) = parse_segment_name(&fn_str)?;

                let mut file = OpenOptions::new().read(true).open(&file_path)?;
                let header = read_file_header(&mut file)?;
                validate_magic_version(&header)?;
                validate_segment_id(&header, index)?;

                // Create a SegmentRef instance
                let segment_ref = SegmentRef {
                    file_path,
                    file_header_offset: (4 + header.len()) as u64, // You need to set the correct offset here
                    id: index,
                };

                segment_refs.push(segment_ref);
            }
        }

        segment_refs.sort_by(|a, b| a.id.cmp(&b.id));

        Ok(segment_refs)
    }
}

/*
    Represents a segment in aan append-only (or write-ahead) log.

    A `Segment` represents a portion of thean append-only (or write-ahead) log. It holds information about the file
    that stores the log entries, as well as details related to the segment's data and state.

    A segment header is stored at the beginning of the segment file. It has the following format:

    File Header

     0      1      2      3      4      5      6      7      8
     +------+------+------+------+------+------+------+------+
     | Magic                                                 |
     +------+------+------+------+------+------+------+------+
     | Version                                               |
     +------+------+------+------+------+------+------+------+
     | SegmentID                                             |
     +------+------+------+------+------+------+------+------+
     | Compression                                           |
     +------+------+------+------+------+------+------+------+
     | Compression Level                                     |
     +------+------+------+------+------+------+------+------+
     | Metadata                                              |
     .                                                       |
     .                                                       |
     .                                                       |
     +------+------+------+------+------+------+------+------+
*/
pub(crate) struct Segment {
    /// The unique identifier of the segment.
    pub(crate) id: u64,

    #[allow(dead_code)]
    /// The path where the segment file is located.
    pub(crate) file_path: PathBuf,

    /// The underlying file for storing the segment's data.
    file: File,

    /// The base offset of the file.
    pub(crate) file_header_offset: u64,

    /// The current offset within the file.
    file_offset: u64,

    #[allow(dead_code)]
    /// The maximum size of the segment file.
    pub(crate) file_size: u64,

    /// A flag indicating whether the segment is closed or not.
    closed: bool,
}

impl Segment {
    pub(crate) fn open(dir: &Path, id: u64, opts: &Options) -> Result<Self> {
        // Ensure the options are valid
        opts.validate()?;

        // Build the file path using the segment name and extension
        let extension = opts.file_extension.as_deref().unwrap_or("");
        let file_name = segment_name(id, extension);
        let file_path = dir.join(&file_name);
        let file_path_exists = file_path.exists();
        let file_path_is_file = file_path.is_file();

        // Open the file with the specified options
        let mut file = Self::open_file(&file_path, opts)?;

        // Initialize the file header offset
        let mut file_header_offset = 0;

        // If the file already exists
        if file_path_exists && file_path_is_file {
            // Handle existing file
            let header = read_file_header(&mut file)?;
            validate_file_header(&header, id, opts)?;

            file_header_offset += 4 + header.len();
            let (index, _) = parse_segment_name(&file_name)?;
            if index != id {
                return Err(Error::IO(IOError::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid segment id",
                )));
            }
        } else {
            // Write new file header
            let header_len = write_file_header(&mut file, id, opts)?;
            file_header_offset += header_len;
        }

        // Seek to the end of the file to get the file offset
        let file_offset = file.seek(io::SeekFrom::End(0))?;

        // Initialize and return the Segment
        Ok(Segment {
            file,
            file_header_offset: file_header_offset as u64,
            file_offset: file_offset - file_header_offset as u64,
            file_path,
            id,
            closed: false,
            file_size: opts.max_file_size,
        })
    }

    fn open_file(file_path: &Path, opts: &Options) -> Result<File> {
        let mut open_options = OpenOptions::new();
        open_options.read(true).append(true);

        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            if let Some(file_mode) = opts.file_mode {
                open_options.mode(file_mode);
            }
        }

        if !file_path.exists() {
            open_options.create(true); // Create the file if it doesn't exist
        }

        let file = open_options.open(file_path)?;

        Ok(file)
    }

    // Flushes the current block to disk.
    // This method also synchronize file metadata to the filesystem
    // hence it is a bit slower than fdatasync (sync_data).
    pub(crate) fn sync(&mut self) -> Result<()> {
        if self.closed {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "Segment is closed",
            )));
        }

        self.file.sync_all()?;
        Ok(())
    }

    pub(crate) fn close(&mut self) -> Result<()> {
        self.sync()?;
        self.closed = true;
        Ok(())
    }

    // Returns the current offset within the segment.
    pub(crate) fn offset(&self) -> u64 {
        self.file_offset
    }

    /// Appends data to the segment.
    ///
    /// # Parameters
    ///
    /// - `rec`: The data to be appended.
    ///
    /// # Returns
    ///
    /// Returns the offset.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is closed.
    pub(crate) fn append(&mut self, rec: &[u8]) -> Result<u64> {
        // If the segment is closed, return an error
        if self.closed {
            return Err(Error::SegmentClosed);
        }

        if rec.is_empty() {
            return Err(Error::EmptyBuffer);
        }

        let offset = self.offset();

        // write_all does atomic writes to the file (in this case the os buffer)
        self.file.write_all(rec)?;
        self.file_offset += rec.len() as u64;

        Ok(offset)
    }

    /// Reads data from the segment at the specified offset from the underlying file.
    /// The read data is then copied into the provided byte slice `bs`.
    ///
    /// # Parameters
    ///
    /// - `bs`: A byte slice to store the read data.
    /// - `off`: The offset from which to start reading.
    ///
    /// # Returns
    ///
    /// Returns the number of bytes read and any encountered error.
    ///
    /// # Errors
    ///
    /// Returns an error if the provided offset is negative or if there is an I/O error
    /// during reading.
    pub(crate) fn read_at(&self, bs: &mut [u8], off: u64) -> Result<usize> {
        if self.closed {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "Segment is closed",
            )));
        }

        if off > self.offset() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::Other,
                "Offset beyond current position",
            )));
        }

        // Read from the file
        let actual_read_offset = self.file_header_offset + off;
        let bytes_read;

        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            bytes_read = self.file.read_at(bs, actual_read_offset)?;
        }
        #[cfg(not(unix))]
        {
            let mut file = &self.file;
            file.seek(SeekFrom::Start(self.file_header_offset + off))?;
            bytes_read = file.read(bs)?;
        }

        Ok(bytes_read)
    }
}

impl Drop for Segment {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.close().ok();
    }
}

/// Result returning Error
pub type Result<T> = std::result::Result<T, Error>;

/// Custom error type for the storage module
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    Corruption(CorruptionError), // New variant for CorruptionError
    SegmentClosed,
    EmptyBuffer,
    Eof,
    IO(IOError),
    Poison(String),
    RecordTooLarge,
    SegmentNotFound,
}

// Implementation of Display trait for Error
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Corruption(err) => write!(f, "Corruption error: {}", err),
            Error::SegmentClosed => write!(f, "Segment is closed"),
            Error::EmptyBuffer => write!(f, "Buffer is empty"),
            Error::IO(err) => write!(f, "IO error: {}", err),
            Error::Eof => write!(f, "EOF"),
            Error::Poison(msg) => write!(f, "Lock Poison: {}", msg),
            Error::RecordTooLarge => write!(
                f,
                "Record is too large to fit in a segment. Increase max segment size"
            ),
            Error::SegmentNotFound => write!(f, "Segment not found"),
        }
    }
}

// Implementation of Error trait for Error
impl std::error::Error for Error {}

// Implementation to convert io::Error into Error
impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IO(IOError {
            kind: e.kind(),
            message: e.to_string(),
        })
    }
}

// Implementation to convert PoisonError into Error
impl<T: Sized> From<PoisonError<T>> for Error {
    fn from(e: PoisonError<T>) -> Error {
        Error::Poison(e.to_string())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IOError {
    kind: io::ErrorKind,
    message: String,
}

impl IOError {
    pub(crate) fn new(kind: io::ErrorKind, message: &str) -> Self {
        IOError {
            kind,
            message: message.to_string(),
        }
    }
}

impl fmt::Display for IOError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "kind={}, message={}", self.kind, self.message)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CorruptionError {
    kind: io::ErrorKind,
    message: String,
    pub(crate) segment_id: u64,
    pub(crate) offset: u64,
}

impl CorruptionError {
    pub(crate) fn new(kind: io::ErrorKind, message: &str, segment_id: u64, offset: u64) -> Self {
        CorruptionError {
            kind,
            message: message.to_string(),
            segment_id,
            offset,
        }
    }
}

impl fmt::Display for CorruptionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "kind={}, message={}, segment_id={}, offset={}",
            self.kind, self.message, self.segment_id, self.offset
        )
    }
}

// Implementation of Error trait for CorruptionError
impl std::error::Error for CorruptionError {}

// MultiSegmentReader is a buffered reader that reads in multiples of BLOCK_SIZE.
// It is used by Reader to read from multiple segments. It is used by WAL to
// track multiple segment and offset for corruption detection. Since data is
// written to WAL in multiples of BLOCK_SIZE, non-block aligned segments
// are padded with zeros. This is done to avoid partial reads from the WAL.
pub struct MultiSegmentReader {
    buf: BufReader<File>,      // Buffer for reading from the current segment.
    segments: Vec<SegmentRef>, // List of segments to read from.
    cur: usize,                // Index of current segment in segments.
    off: usize,                // Offset in current segment.
}

impl MultiSegmentReader {
    pub(crate) fn new(segments: Vec<SegmentRef>) -> Result<MultiSegmentReader> {
        if segments.is_empty() {
            return Err(Error::IO(IOError::new(
                io::ErrorKind::InvalidInput,
                "Empty segment list",
            )));
        }

        let cur = 0;
        let off = 0;

        // Open the first segment's file for reading
        let mut file = File::open(&segments[cur].file_path)?;
        file.seek(SeekFrom::Start(segments[cur].file_header_offset))?;

        let buf = BufReader::with_capacity(READ_BUF_SIZE, file);

        Ok(MultiSegmentReader {
            buf,
            segments,
            cur,
            off,
        })
    }

    fn is_eof(&mut self) -> io::Result<bool> {
        let bytes_read = self.buf.fill_buf()?;
        Ok(bytes_read.is_empty())
    }

    fn load_next_segment(&mut self) -> io::Result<()> {
        if self.cur + 1 >= self.segments.len() {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }

        self.cur += 1;
        self.off = 0;

        let next_file = File::open(&self.segments[self.cur].file_path)?;
        let header_offset = self.segments[self.cur].file_header_offset;
        let mut next_buf_reader = BufReader::with_capacity(READ_BUF_SIZE, next_file);
        next_buf_reader.seek(SeekFrom::Start(header_offset))?;

        self.buf = next_buf_reader;

        Ok(())
    }

    pub(crate) fn current_segment_id(&self) -> u64 {
        self.segments[self.cur].id
    }

    pub(crate) fn current_offset(&self) -> usize {
        self.off
    }
}

#[allow(clippy::unused_io_amount)]
impl Read for MultiSegmentReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.cur >= self.segments.len() {
            return Ok(0);
        }

        // Note: This could create a problem when reading a partial block
        // spread over multiple segments. Currently wal do not
        // write partial blocks spanning multiple segments.
        let bytes_read = if !self.is_eof()? {
            self.buf.read(buf)?
        } else {
            self.load_next_segment()?;
            self.buf.read(buf)?
        };
        self.off += bytes_read;
        Ok(bytes_read)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::fs::OpenOptions;
    use std::io::Cursor;
    use std::io::Seek;
    use std::io::Write;
    use tempdir::TempDir;

    #[test]
    fn new_empty() {
        let metadata = Metadata::new(None);
        assert_eq!(metadata.data.len(), 0);
    }

    #[test]
    fn put_and_get_uint() {
        let mut metadata = Metadata::new(None);
        metadata.put_uint("age", 25);
        assert_eq!(metadata.get_uint("age").unwrap(), 25);
    }

    #[test]
    fn bytes_roundtrip() {
        let mut metadata = Metadata::new(None);
        metadata.put_uint("age", 30);
        metadata.put_uint("num", 40);

        let bytes = metadata.to_bytes().unwrap();
        let restored_metadata = Metadata::new(Some(bytes));

        assert_eq!(restored_metadata.get_uint("age").unwrap(), 30);
        assert_eq!(restored_metadata.get_uint("num").unwrap(), 40);
    }

    #[test]
    fn read_reader_field() {
        let data = [0, 0, 0, 5, 65, 66, 67, 68, 69]; // "ABCDE"
        let mut cursor = Cursor::new(&data);
        let result = read_field(&mut cursor).unwrap();
        assert_eq!(result, b"ABCDE");
    }

    #[test]
    fn write_reader_field() {
        let mut output = Vec::new();
        let data = b"XYZ";
        write_field(data, &mut output).unwrap();
        assert_eq!(&output, &[0, 0, 0, 3, 88, 89, 90]); // [0, 0, 0, 3] for length and "XYZ" bytes
    }

    #[test]
    fn metadata_extension() {
        let id = 12345;
        let opts = Options::default();

        // Create a new metadata using new_file_header
        let mut meta = Metadata::new_file_header(id, &opts).unwrap();

        // Create an extended metadata
        let mut extended_meta = Metadata::new(None);
        extended_meta.put_uint("key1", 123);
        extended_meta.put_uint("key2", 456);

        // Serialize and extend the extended metadata using bytes
        let extended_bytes = extended_meta.to_bytes().unwrap();
        meta.read_from(&mut &extended_bytes[..])
            .expect("Failed to read from bytes");

        // Check if keys from existing metadata are present in the extended metadata
        assert_eq!(meta.get_uint(KEY_MAGIC).unwrap(), MAGIC);
        assert_eq!(meta.get_uint(KEY_VERSION).unwrap(), VERSION);
        assert_eq!(meta.get_uint(KEY_SEGMENT_ID).unwrap(), id);
        assert_eq!(
            meta.get_uint(KEY_COMPRESSION_FORMAT).unwrap(),
            CompressionFormat::NoCompression.as_u64()
        );
        assert_eq!(
            meta.get_uint(KEY_COMPRESSION_LEVEL).unwrap(),
            CompressionLevel::BestSpeed.as_u64()
        );

        // Check if keys from the extended metadata are present in the extended metadata
        assert_eq!(meta.get_uint("key1").unwrap(), 123);
        assert_eq!(meta.get_uint("key2").unwrap(), 456);
    }

    #[test]
    fn check_and_validate_file_header() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let mut opts = Options::default();

        // Add optional metadata
        let mut metadata = Metadata::new(None);
        metadata.put_uint("key1", 123);
        metadata.put_uint("key2", 456);
        opts.metadata = Some(metadata);

        // Create a new segment file and write the header
        let segment_path = temp_dir.path().join("00000000000000000000");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(segment_path)
            .expect("should create file");

        let id = 0;
        write_file_header(&mut file, id, &opts).expect("should write header");
        file.seek(io::SeekFrom::Start(0))
            .expect("should seek to start"); // Reset the cursor

        // Read the header from the file
        let header = read_file_header(&mut file).expect("should read header");

        // Validate the file header
        let result = validate_file_header(&header, id, &opts);
        assert!(result.is_ok());
    }

    #[test]
    fn bad_file_header() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let mut opts = Options::default();

        // Create a new segment file and write the header
        let segment_path = temp_dir.path().join("00000000000000000000");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(segment_path)
            .expect("should create file");

        let id = 0;
        write_file_header(&mut file, id, &opts).expect("should write header");
        file.seek(io::SeekFrom::Start(0))
            .expect("should seek to start"); // Reset the cursor

        // Read the header from the file
        let header = read_file_header(&mut file).expect("should read header");

        // Modify the metadata to an unexpected value
        opts.metadata = Some(Metadata::new(None)); // This doesn't match the default metadata

        // Validate the file header, expecting an error due to mismatched compression level
        let result = validate_file_header(&header, id, &opts);
        assert!(result.is_err()); // Header validation should throw an error
    }

    #[test]
    fn segment_name_with_extension() {
        let index = 42;
        let ext = "log";
        let expected = format!("{:020}.{}", index, ext);
        assert_eq!(segment_name(index, ext), expected);
    }

    #[test]
    fn segment_name_without_extension() {
        let index = 42;
        let expected = format!("{:020}", index);
        assert_eq!(segment_name(index, ""), expected);
    }

    #[test]
    fn parse_segment_name_with_extension() {
        let name = "00000000000000000042.log";
        let result = parse_segment_name(name).unwrap();
        assert_eq!(result, (42, Some("log".to_string())));
    }

    #[test]
    fn parse_segment_name_without_extension() {
        let name = "00000000000000000042";
        let result = parse_segment_name(name).unwrap();
        assert_eq!(result, (42, None));
    }

    #[test]
    fn parse_segment_name_invalid_format() {
        let name = "invalid_name";
        let result = parse_segment_name(name);
        assert!(result.is_err());
    }

    #[test]
    fn segments_empty_directory() {
        let temp_dir = create_temp_directory();
        let dir = temp_dir.path().to_path_buf();

        let result = get_segment_range(&dir).unwrap();
        assert_eq!(result, (0, 0));
    }

    #[test]
    fn segments_non_empty_directory() {
        let temp_dir = create_temp_directory();
        let dir = temp_dir.path().to_path_buf();

        create_segment_file(&dir, "00000000000000000001.log");
        create_segment_file(&dir, "00000000000000000003.log");
        create_segment_file(&dir, "00000000000000000002.log");
        create_segment_file(&dir, "00000000000000000004.log");

        let result = get_segment_range(&dir).unwrap();
        assert_eq!(result, (1, 4));
    }

    fn create_temp_directory() -> TempDir {
        TempDir::new("test").unwrap()
    }

    fn create_segment_file(dir: &Path, name: &str) {
        let file_path = dir.join(name);
        let mut file = File::create(file_path).unwrap();
        file.write_all(b"dummy content").unwrap();
    }

    #[test]
    fn aol_append() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        let sz = segment.offset();
        assert_eq!(0, sz);

        // Test appending an empty buffer
        let r = segment.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert_eq!(r, Ok(0));

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(r, Ok(4));

        // Validate offset after appending
        // 4 + 7 = 11
        assert_eq!(segment.offset(), 11);

        // Test syncing segment
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 11);

        // Test reading from segment
        let mut bs = vec![0; 4];
        let n = segment.read_at(&mut bs, 0).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 7];
        let n = segment.read_at(&mut bs, 4).expect("should read");
        assert_eq!(7, n);
        assert_eq!(&[4, 5, 6, 7, 8, 9, 10].to_vec(), &bs[..]);

        // Test reading beyond segment's current size
        let mut bs = vec![0; 14];
        let r = segment.read_at(&mut bs, 11 + 1);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert_eq!(r, Ok(11));

        // Validate offset after appending
        // 11 + 4 = 4100
        assert_eq!(segment.offset(), 11 + 4);

        // Test reading from segment after appending
        let mut bs = vec![0; 4];
        let n = segment.read_at(&mut bs, 11).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[..]);

        // Test syncing segment again
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing again
        assert_eq!(segment.offset(), 15);

        // Test closing segment
        assert!(segment.close().is_ok());
    }

    #[test]
    fn aol_reopen_empty_file() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());

        drop(segment);

        // Reopen segment should pass
        let segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());
    }

    #[test]
    fn segment_reopen() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert_eq!(r, Ok(0));

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(r, Ok(4));

        // Validate offset after appending
        // 4 + 7 = 11
        assert_eq!(segment.offset(), 11);

        // Test syncing segment
        assert!(segment.sync().is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 11);

        // Test closing segment
        assert!(segment.close().is_ok());

        drop(segment);

        // Reopen segment
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(segment.offset(), 11);

        // Test reading from segment
        let mut bs = vec![0; 4];
        let n = segment.read_at(&mut bs, 0).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 7];
        let n = segment.read_at(&mut bs, 4).expect("should read");
        assert_eq!(7, n);
        assert_eq!(&[4, 5, 6, 7, 8, 9, 10].to_vec(), &bs[..]);

        // Test reading beyond segment's current size
        let mut bs = vec![0; 14];
        let r = segment.read_at(&mut bs, READ_BUF_SIZE as u64 + 1);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert_eq!(r, Ok(11));

        // Validate offset after appending
        // 11 + 4 = 4100
        assert_eq!(segment.offset(), 11 + 4);

        // Test reading from segment after appending
        let mut bs = vec![0; 4];
        let n = segment.read_at(&mut bs, 11_u64).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[..]);

        // Test closing segment
        assert!(segment.close().is_ok());

        // Reopen segment
        let segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");
        // Test initial offset
        assert_eq!(segment.offset(), 11 + 4);

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(segment);
        drop(temp_dir);
    }

    #[test]
    fn segment_reopen_file() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());

        drop(segment);

        // Reopen segment should pass
        let segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());
    }

    #[test]
    fn segment_corrupted_metadata() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Close the segment
        segment.close().expect("should close segment");

        // Corrupt the segment's metadata by overwriting the first few bytes
        let segment_path = temp_dir.path().join("00000000000000000000");
        let corrupted_data = vec![0; 4];

        // Open the file for writing before writing to it
        let mut corrupted_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path)
            .expect("should open corrupted file");

        corrupted_file
            .write_all(&corrupted_data)
            .expect("should write corrupted data to file");

        // Attempt to reopen the segment with corrupted metadata
        let reopened_segment = Segment::open(temp_dir.path(), 0, &opts);
        assert!(reopened_segment.is_err()); // Opening should fail due to corrupted metadata
    }

    #[test]
    fn segment_closed_operations() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        // Create a new segment file and open it
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Close the segment
        segment.close().expect("should close segment");

        // Try to perform operations on the closed segment
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_err()); // Appending should fail

        let r = segment.sync();
        assert!(r.is_err()); // Syncing should fail

        let mut bs = vec![0; 11];
        let n = segment.read_at(&mut bs, 0);
        assert!(n.is_err()); // Reading should fail

        // Reopen the closed segment
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should reopen segment");

        // Try to perform operations on the reopened segment
        let r = segment.append(&[4, 5, 6, 7]);
        assert!(r.is_ok()); // Appending should succeed on reopened segment
    }

    #[test]
    fn segment_append_read_append() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        // Create a new segment file and open it
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Append data to the segment
        let append_result = segment.append(&[0, 1, 2, 3]);
        assert!(append_result.is_ok());

        // Read data from a specific position
        let mut buffer = vec![0u8; 2];
        let read_result = segment.read_at(&mut buffer, 0);
        assert!(read_result.is_ok());
        assert_eq!(buffer, vec![0, 1]);

        // Append more data to the segment
        let append_result = segment.append(&[4, 5, 6]);
        assert!(append_result.is_ok());
        segment.sync().expect("should sync segment");

        // Read data from a specific position again
        let mut buffer = vec![0u8; 3];
        let read_result = segment.read_at(&mut buffer, 4);
        assert!(read_result.is_ok());
        assert_eq!(buffer, vec![4, 5, 6]);

        // Close the segment
        segment.close().expect("should close segment");
    }

    #[test]
    fn sync_on_synced_segment() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        let sz = segment.offset();
        assert_eq!(0, sz);

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(r, Ok(0));

        // Validate offset after appending
        assert_eq!(segment.offset(), 11);

        // Test syncing segment
        let r = segment.sync();
        assert!(r.is_ok());
        assert_eq!(segment.offset(), 11);

        let r = segment.sync();
        assert!(r.is_ok());
        assert_eq!(segment.offset(), 11);

        segment.close().expect("should close segment");

        // Reopen segment and validate offset
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        let sz = segment.offset();
        assert_eq!(11, sz);

        let r = segment.sync();
        assert!(r.is_ok());
        assert_eq!(segment.offset(), 11);
    }

    #[test]
    fn test_list_segment_ids() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");
        let dir_path = temp_dir.path();

        // Populate the directory with segment files and some other files
        create_segment_file(dir_path, &segment_name(1, ""));
        create_segment_file(dir_path, &segment_name(2, ""));
        create_segment_file(dir_path, &segment_name(10, ""));

        // Call the function under test
        let segment_ids = list_segment_ids(dir_path).unwrap();

        // Verify the output
        assert_eq!(segment_ids, vec![1, 2, 10]);
    }
}
