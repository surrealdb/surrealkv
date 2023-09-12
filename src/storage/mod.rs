pub mod aol;
pub mod wal;

use std::collections::HashMap;
use std::fs::{read_dir, File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::num::ParseIntError;
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

/// The size of a single block in bytes.
///
/// The `BLOCK_SIZE` constant represents the size of a block used for buffering disk writes in the
/// write-ahead log. It determines the maximum amount of data that can be held in memory before
/// flushing to disk. Larger block sizes can improve write performance but might use more memory.
const BLOCK_SIZE: usize = 4096;

/// Length of the record header in bytes.
///
/// This constant represents the length, in bytes, of the record header structure
/// used in the write-ahead log. The record header contains information
/// about the type of the record, the length of the payload, and a checksum value.
/// The constant is set to 7, reflecting the size of the record header structure.
pub const WAL_RECORD_HEADER_SIZE: usize = 7;

/// The magic value for identifying file headers.
///
/// The `MAGIC` constant represents a magic value used in file headers to uniquely identify the
/// format or purpose of the file. It's a hexadecimal value that serves as a signature for file
/// identification and verification.
const MAGIC: u64 = 0xFB98F92D;

/// The version number of the file format.
///
/// The `VERSION` constant represents the version number of the file format used in write-ahead log
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

/// Constants for key names used in the file header.
const KEY_MAGIC: &str = "magic";
const KEY_VERSION: &str = "version";
const KEY_SEGMENT_ID: &str = "segment_id";
const KEY_COMPRESSION_FORMAT: &str = "compression_format";
const KEY_COMPRESSION_LEVEL: &str = "compression_level";

// Enum to represent different compression formats
#[derive(Clone)]
pub enum CompressionFormat {
    NoCompression = 0,
    Flate = 1,
    GZip = 2,
    ZLib = 3,
}

impl CompressionFormat {
    fn as_u64(&self) -> u64 {
        match self {
            CompressionFormat::NoCompression => 0,
            CompressionFormat::Flate => 1,
            CompressionFormat::GZip => 2,
            CompressionFormat::ZLib => 3,
        }
    }

    fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(CompressionFormat::NoCompression),
            1 => Some(CompressionFormat::Flate),
            2 => Some(CompressionFormat::GZip),
            3 => Some(CompressionFormat::ZLib),
            _ => None,
        }
    }
}

// Enum to represent different compression levels
#[derive(Clone)]
pub enum CompressionLevel {
    BestSpeed = 0,
    BestCompression = 1,
    DefaultCompression = 2,
    HuffmanOnly = 3,
}

impl CompressionLevel {
    fn as_u64(&self) -> u64 {
        match *self {
            CompressionLevel::BestSpeed => 0,
            CompressionLevel::BestCompression => 1,
            CompressionLevel::DefaultCompression => 2,
            CompressionLevel::HuffmanOnly => 3,
        }
    }

    fn from_u64(value: u64) -> Option<Self> {
        match value {
            0 => Some(CompressionLevel::BestSpeed),
            1 => Some(CompressionLevel::BestCompression),
            2 => Some(CompressionLevel::DefaultCompression),
            3 => Some(CompressionLevel::HuffmanOnly),
            _ => None,
        }
    }
}

/// A `Block` is an in-memory buffer that stores data before it is flushed to disk. It is used to
/// batch writes to improve performance by reducing the number of individual disk writes. If the
/// data to be written exceeds the `BLOCK_SIZE`, it will be split and flushed separately. The `Block`
/// keeps track of the allocated space, flushed data, and other details related to the write process.
/// It can also be used to align data to the size of a direct I/O block, if applicable.
///
/// # Type Parameters
///
/// - `BLOCK_SIZE`: The size of the block in bytes.
/// - `RECORD_HEADER_SIZE`: The size of the record header in bytes.
pub(crate) struct Block<const BLOCK_SIZE: usize, const RECORD_HEADER_SIZE: usize> {
    /// The number of bytes currently written in the block.
    written: usize,

    /// The number of bytes that have been flushed to disk.
    flushed: usize,

    /// The buffer that holds the actual data.
    buf: [u8; BLOCK_SIZE],
}

impl<const BLOCK_SIZE: usize, const RECORD_HEADER_SIZE: usize>
    Block<BLOCK_SIZE, RECORD_HEADER_SIZE>
{
    fn new() -> Self {
        Block {
            written: 0,
            flushed: 0,
            buf: [0; BLOCK_SIZE],
        }
    }

    fn remaining(&self) -> usize {
        BLOCK_SIZE - self.written - RECORD_HEADER_SIZE
    }

    fn is_full(&self) -> bool {
        BLOCK_SIZE - self.written <= RECORD_HEADER_SIZE
    }

    fn reset(&mut self) {
        self.buf = [0u8; BLOCK_SIZE];
        self.written = 0;
        self.flushed = 0;
    }

    fn unwritten(&self) -> usize {
        self.written - self.flushed
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
    dir_mode: Option<u32>,

    /// The file mode to set for the segment file.
    ///
    /// If specified, this option sets the permission mode for the segment file. It determines who
    /// can read, write, and execute the file. If not specified, the default file mode will be used.
    file_mode: Option<u32>,

    /// The compression format to apply to the segment's data.
    ///
    /// If specified, this option sets the compression format that will be used to compress the
    /// data written to the segment. Compression can help save storage space but might introduce
    /// some overhead in terms of CPU usage during read and write operations.
    compression_format: Option<CompressionFormat>,

    /// The compression level to use with the selected compression format.
    ///
    /// This option specifies the compression level that will be applied when compressing the data.
    /// Higher levels usually provide better compression ratios but require more computational
    /// resources. If not specified, a default compression level will be used.
    compression_level: Option<CompressionLevel>,

    /// The metadata associated with the segment.
    ///
    /// This option allows you to attach metadata to the segment. Metadata can be useful for storing
    /// additional information about the segment's contents or usage. If not specified, no metadata
    /// will be associated with the segment.
    metadata: Option<Metadata>,

    /// The extension to use for the segment file.
    ///
    /// If specified, this option sets the extension for the segment file. The extension is used
    /// when creating the segment file on disk. If not specified, a default extension might be used.
    extension: Option<String>,

    /// The maximum size of the segment file.
    ///
    /// If specified, this option sets the maximum size that the segment file is allowed to reach.
    /// Once the file reaches this size, additional writes will be prevented. If not specified,
    /// there is no maximum size limit for the file.
    ///
    /// This is used by aol to cycle segments when the max file size is reached.
    max_file_size: u64,

    /// A flag indicating whether the segment is a Write-Ahead Logging (WAL).
    ///
    /// If this flag is set to `true`, the segment's records will be encoded with the WAL header.
    ///
    /// If this flag is set to `false`, Write-Ahead Logging will be disabled, and records are encoded
    /// without the WAL header.
    ///
    /// By default, this flag is set to `false`, indicating that Write-Ahead Logging is not enabled.
    is_wal: bool,
}

impl Options {
    fn default() -> Self {
        Options {
            dir_mode: Some(0o750),                                // default directory mode
            file_mode: Some(DEFAULT_FILE_MODE),                   // default file mode
            compression_format: Some(DEFAULT_COMPRESSION_FORMAT), // default compression format
            compression_level: Some(DEFAULT_COMPRESSION_LEVEL),   // default compression level
            metadata: None,                                       // default metadata
            extension: None,                                      // default extension
            max_file_size: DEFAULT_FILE_SIZE,                     // default max file size (20mb)
            is_wal: false,
        }
    }

    fn new() -> Self {
        Options {
            dir_mode: None,
            file_mode: None,
            compression_format: None,
            compression_level: None,
            metadata: None,
            extension: None,
            max_file_size: 0,
            is_wal: false,
        }
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.max_file_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid max_file_size",
            ));
        }

        Ok(())
    }

    fn with_file_mode(mut self, file_mode: u32) -> Self {
        self.file_mode = Some(file_mode);
        self
    }

    fn with_compression_format(mut self, compression_format: CompressionFormat) -> Self {
        self.compression_format = Some(compression_format);
        self
    }

    fn with_compression_level(mut self, compression_level: CompressionLevel) -> Self {
        self.compression_level = Some(compression_level);
        self
    }

    fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    fn with_extension(mut self, extension: String) -> Self {
        self.extension = Some(extension);
        self
    }

    fn with_max_file_size(mut self, max_file_size: u64) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    fn with_dir_mode(mut self, dir_mode: u32) -> Self {
        self.dir_mode = Some(dir_mode);
        self
    }

    fn with_wal(mut self) -> Self {
        self.is_wal = true;
        self
    }
}

/// Represents metadata associated with a file.
///
/// The `Metadata` struct defines a container for storing key-value pairs of metadata. This metadata
/// can be used to hold additional information about a file. The data is stored in a hash map where
/// each key is a string and each value is a vector of bytes.
#[derive(Clone)]
pub(crate) struct Metadata {
    /// The map holding key-value pairs of metadata.
    ///
    /// The `data` field is a hash map that allows associating arbitrary data with descriptive keys.
    /// This can be used, for example, to store additional information about the file as a header.
    data: HashMap<String, Vec<u8>>,
}

impl Metadata {
    // Constructor for Metadata, reads data from bytes if provided
    fn new(b: Option<Vec<u8>>) -> Self {
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
    fn new_file_header(id: u64, cf: &CompressionFormat, cl: &CompressionLevel) -> Self {
        let mut buf = Metadata::new(None);

        // Set file header key-value pairs using constants
        buf.put_int(KEY_MAGIC, MAGIC);
        buf.put_int(KEY_VERSION, VERSION);
        buf.put_int(KEY_SEGMENT_ID, id);
        buf.put_int(KEY_COMPRESSION_FORMAT, cf.as_u64());
        buf.put_int(KEY_COMPRESSION_LEVEL, cl.as_u64());

        buf
    }

    // Returns the serialized bytes representation of Metadata
    fn bytes(&self) -> Vec<u8> {
        let mut b = Vec::new();
        self.write_to(&mut b).unwrap();
        b
    }

    // Reads Metadata from a given reader
    fn read_from<R: Read>(&mut self, reader: &mut R) -> io::Result<()> {
        let mut len_buf = [0; 4];
        reader.read_exact(&mut len_buf)?; // Read 4 bytes for the length
        let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length

        // Loop to read key-value pairs from the reader
        for _ in 0..len {
            let k = read_field(reader)?; // Read key
            let v = read_field(reader)?; // Read value
            self.data.insert(String::from_utf8_lossy(&k).to_string(), v);
        }

        Ok(())
    }

    // Writes Metadata to a given writer
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
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
    fn put_int(&mut self, key: &str, n: u64) {
        let b = n.to_be_bytes(); // Convert integer to big-endian bytes
        self.put(key, &b); // Call the generic put method
    }

    // Gets an integer value associated with a given key
    fn get_int(&self, key: &str) -> io::Result<u64> {
        // Use the generic get method to retrieve bytes
        let value_bytes = self
            .get(key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "Key not found"))?;

        // Convert bytes to u64
        let int_value = u64::from_be_bytes(value_bytes.as_slice().try_into().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "Failed to convert bytes to u64")
        })?);

        Ok(int_value)
    }

    // Generic method to put a key-value pair into the data HashMap
    fn put(&mut self, key: &str, value: &[u8]) {
        self.data.insert(key.to_string(), value.to_vec());
    }

    // Generic method to get the value associated with a given key
    fn get(&self, key: &str) -> Option<&Vec<u8>> {
        self.data.get(key)
    }
}

/// Enum representing different types of records in a write-ahead log.
///
/// This enumeration defines different types of records that can be used in a write-ahead log.
/// Each record type indicates a particular state of a record in the log. The enum variants
/// represent various stages of a record, including full records, fragments, and special cases.
///
/// # Variants
///
/// - `Empty`: Indicates that the rest of the block is empty.
/// - `Full`: Represents a full record.
/// - `First`: Denotes the first fragment of a record.
/// - `Middle`: Denotes middle fragments of a record.
/// - `Last`: Denotes the final fragment of a record.
#[derive(PartialEq)]
enum RecordType {
    Empty = 0,  // Rest of block is empty.
    Full = 1,   // Full record.
    First = 2,  // First fragment of a record.
    Middle = 3, // Middle fragments of a record.
    Last = 4,   // Final fragment of a record.
}

impl RecordType {
    fn from_u8(value: u8) -> Result<Self, std::io::Error> {
        match value {
            0 => Ok(RecordType::Empty),
            1 => Ok(RecordType::Full),
            2 => Ok(RecordType::First),
            3 => Ok(RecordType::Middle),
            4 => Ok(RecordType::Last),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid Record Type",
            )),
        }
    }
}

/*
    Encodes a record with the provided information into the given buffer.

    It has the following format:

    Record Header

        0      1      2      3      4      5      6      7
        +------+------+------+------+------+------+------+------+------+------+
        | Type |    Length   |         CRC32             |       Payload      |
        +------+------+------+------+------+------+------+------+------+------+
*/
fn encode_record_header(buf: &mut [u8], rec_len: usize, part: &[u8], i: usize) {
    let typ = if i == 0 && part.len() == rec_len {
        RecordType::Full
    } else if part.len() == rec_len {
        RecordType::Last
    } else if i == 0 {
        RecordType::First
    } else {
        RecordType::Middle
    };

    buf[0] = typ as u8;
    let len_part = part.len() as u16;
    buf[1..3].copy_from_slice(&len_part.to_be_bytes());
    // calculate the CRC32 checksum based on the record type and data
    let crc = calculate_crc32(&buf[0..1], part);
    buf[3..7].copy_from_slice(&crc.to_be_bytes());
}

// Reads a field from the given reader
fn read_field<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let mut len_buf = [0; 4];
    reader.read_exact(&mut len_buf)?; // Read 4 bytes for the length
    let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length
    let mut fb = vec![0u8; len];
    reader.read_exact(&mut fb)?; // Read the actual field bytes
    Ok(fb)
}

// Writes a field to the given writer
fn write_field<W: Write>(b: &[u8], writer: &mut W) -> io::Result<()> {
    let mut len_buf = [0; 4];
    len_buf.copy_from_slice(&(b.len() as u32).to_be_bytes());
    writer.write_all(&len_buf)?; // Write 4 bytes for the length
    writer.write_all(b)?; // Write the actual field bytes
    Ok(())
}

fn read_file_header(file: &mut File) -> io::Result<Vec<u8>> {
    // Read the header using read_field
    read_field(file)
}

fn write_file_header(file: &mut File, id: u64, opts: &Options) -> io::Result<usize> {
    // Write the header using write_field
    let mut meta = Metadata::new_file_header(
        id,
        opts.compression_format
            .as_ref()
            .unwrap_or(&DEFAULT_COMPRESSION_FORMAT),
        opts.compression_level
            .as_ref()
            .unwrap_or(&DEFAULT_COMPRESSION_LEVEL),
    );

    if let Some(metadata) = &opts.metadata {
        let buf = metadata.bytes();
        meta.read_from(&mut &buf[..])?;
    }

    let mut header = Vec::new();
    write_field(&meta.bytes(), &mut header)?;

    // Write header to the file
    file.write_all(&header)?;

    // Sync data to disk and flush metadata
    file.sync_all()?;

    Ok(header.len())
}

fn validate_magic_version(header: &[u8]) -> io::Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;

    let magic = meta.get_int(KEY_MAGIC)?;
    let version = meta.get_int(KEY_VERSION)?;

    if magic != MAGIC || version != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid header data",
        ));
    }

    Ok(())
}

fn validate_segment_id(header: &[u8], id: u64) -> io::Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;

    let segment_id = meta.get_int(KEY_SEGMENT_ID)?;

    if segment_id != id {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Invalid segment ID",
        ));
    }

    Ok(())
}

fn validate_compression(header: &[u8], opts: &Options) -> io::Result<()> {
    let mut meta = Metadata::new(None);
    meta.read_from(&mut &header[..])?;

    let cf = meta.get_int(KEY_COMPRESSION_FORMAT)?;
    let cl = meta.get_int(KEY_COMPRESSION_LEVEL)?;

    if let Some(expected_cf) = &opts.compression_format {
        if cf != expected_cf.as_u64() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid compression format",
            ));
        }
    }

    if let Some(expected_cl) = &opts.compression_level {
        if cl != expected_cl.as_u64() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Invalid compression level",
            ));
        }
    }

    Ok(())
}

fn validate_file_header(header: &[u8], id: u64, opts: &Options) -> io::Result<()> {
    validate_magic_version(header)?;
    validate_segment_id(header, id)?;
    validate_compression(header, opts)?;

    Ok(())
}

fn validate_record_type(record_type: &RecordType, i: usize) -> Result<(), io::Error> {
    match record_type {
        RecordType::Full => {
            if i != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected full record: first record expected",
                ));
            }
        }
        RecordType::First => {
            if i != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected first record: no previous records expected",
                ));
            }
        }
        RecordType::Middle => {
            if i == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected middle record: missing previous records",
                ));
            }
        }
        RecordType::Last => {
            if i == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "Unexpected last record: missing previous records",
                ));
            }
        }
        _ => {
            return Err(io::Error::new(io::ErrorKind::Other, "Invalid record type"));
        }
    }

    Ok(())
}

fn calculate_crc32(record_type: &[u8], data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(record_type);
    hasher.update(data);
    hasher.finalize()
}

fn merge_slices(dest: &mut [u8], src: &[u8]) -> usize {
    let min_len = dest.len().min(src.len());

    for (d, s) in dest.iter_mut().zip(src.iter()) {
        *d = *s;
    }

    min_len
}

fn parse_segment_name(name: &str) -> io::Result<(u64, Option<String>)> {
    let parts: Vec<&str> = name.split('.').collect();

    if parts.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Invalid segment name format",
        ));
    }

    let index: Result<u64, ParseIntError> = parts[0].parse();
    if let Ok(index) = index {
        if parts.len() == 1 {
            return Ok((index, None));
        } else if parts.len() == 2 {
            return Ok((index, Some(parts[1].to_string())));
        }
    }

    Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Invalid segment name format",
    ))
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
fn get_segment_range(dir: &Path) -> io::Result<(u64, u64)> {
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
fn list_segment_ids(dir: &Path) -> io::Result<Vec<u64>> {
    let mut refs: Vec<u64> = Vec::new();
    let files = read_dir(dir)?;

    for file in files {
        let entry = file?;
        let fn_name = entry.file_name();
        let fn_str = fn_name.to_string_lossy();
        let (index, _) = parse_segment_name(&fn_str)?;
        refs.push(index);
    }

    refs.sort();

    Ok(refs)
}

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
    pub fn read_segments_from_directory(
        directory_path: &Path,
    ) -> Result<Vec<SegmentRef>, io::Error> {
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
    Represents a segment in a write-ahead log.

    A `Segment` represents a portion of the write-ahead log. It holds information about the file
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

    /// The path where the segment file is located.
    pub(crate) file_path: PathBuf,

    /// The active block for buffering data.
    block: Block<BLOCK_SIZE, WAL_RECORD_HEADER_SIZE>,

    /// The underlying file for storing the segment's data.
    file: File,

    /// The base offset of the file.
    pub(crate) file_header_offset: u64,

    /// The current offset within the file.
    file_offset: u64,

    /// A flag indicating whether the segment is closed or not.
    closed: bool,

    is_wal: bool,
}

impl Segment {
    pub(crate) fn open(dir: &Path, id: u64, opts: &Options) -> io::Result<Self> {
        // Ensure the options are valid
        opts.validate()?;

        // Build the file path using the segment name and extension
        let extension = opts.extension.as_deref().unwrap_or("");
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
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid segment id",
                ));
            }
            // TODO: take id etc from existing header
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
            block: Block::new(),
            is_wal: opts.is_wal,
        })
    }

    fn open_file(file_path: &Path, opts: &Options) -> io::Result<File> {
        let mut open_options = OpenOptions::new();
        open_options.read(true).write(true);

        if let Some(file_mode) = opts.file_mode {
            open_options.mode(file_mode);
        }

        if !file_path.exists() {
            open_options.create(true); // Create the file if it doesn't exist
        }

        open_options.open(file_path)
    }

    // Flushes the current block to disk.
    // This method also synchronize file metadata to the filesystem
    // hence it is a bit slower than fdatasync (sync_data).
    pub(crate) fn sync(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if self.block.written > 0 {
            self.flush_block(true)?;
        }
        self.file.sync_all()
    }

    pub(crate) fn close(&mut self) -> io::Result<()> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }
        self.sync()?;
        self.closed = true;
        Ok(())
    }

    pub(crate) fn flush_block(&mut self, clear: bool) -> io::Result<()> {
        let mut p = &mut self.block;
        let clear = clear || p.is_full();

        // No more data will fit into the block. Clear it and write to disk.
        // The remaining data in the block will be zeroed out.
        if clear {
            p.written = BLOCK_SIZE; // Write till end of block.
        }

        let n = self.file.write(&p.buf[p.flushed..p.written])?;
        p.flushed += n;
        self.file_offset += n as u64;

        // We flushed an entire block, prepare a new one.
        if clear {
            p.reset();
        }

        Ok(())
    }

    // Returns the current offset within the segment.
    pub(crate) fn offset(&self) -> u64 {
        self.file_offset + self.block.unwritten() as u64
    }

    /// Appends data to the segment.
    ///
    /// This method appends the given data to the segment. If the block is full, it is flushed
    /// to disk. The data is written in chunks to the current block until the block is full.
    ///
    /// # Parameters
    ///
    /// - `rec`: The data to be appended.
    ///
    /// # Returns
    ///
    /// Returns the offset, and the number of bytes successfully appended.
    ///
    /// # Errors
    ///
    /// Returns an error if the segment is closed.
    pub(crate) fn append(&mut self, mut rec: &[u8]) -> io::Result<(u64, usize)> {
        // If the segment is closed, return an error
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if rec.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "buf is empty"));
        }

        let offset = self.offset();
        let mut n = 0;
        let mut i = 0;

        while i == 0 || !rec.is_empty() {
            n += self.write_record(&mut rec, i)?;
            i += 1;
        }

        // Write the remaining data to the block
        if self.block.written > 0 {
            self.flush_block(false)?;
        }

        Ok((offset, n))
    }

    fn write_record(&mut self, rec: &mut &[u8], i: usize) -> io::Result<usize> {
        let active_block = &mut self.block;
        let remaining = std::cmp::min(active_block.remaining(), rec.len());
        let partial_record = &rec[..remaining];
        let buf = &mut active_block.buf[active_block.written..];

        if self.is_wal {
            encode_record_header(buf, rec.len(), partial_record, i);
            // Copy the 'partial_record' into the buffer starting from the WAL_RECORD_HEADER_SIZE offset
            merge_slices(&mut buf[WAL_RECORD_HEADER_SIZE..], partial_record);
            active_block.written += partial_record.len() + WAL_RECORD_HEADER_SIZE;
        } else {
            merge_slices(buf, partial_record);
            active_block.written += partial_record.len();
        }

        if active_block.is_full() {
            self.flush_block(true)?;
        }

        *rec = &rec[remaining..];
        Ok(remaining)
    }

    /// Reads data from the segment at the specified offset.
    ///
    /// This method reads data from the segment starting from the given offset. It reads
    /// from the underlying file if the offset is beyond the current block's buffer. The
    /// read data is then copied into the provided byte slice `bs`.
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
    pub(crate) fn read_at(&self, bs: &mut [u8], off: u64) -> io::Result<usize> {
        if self.closed {
            return Err(io::Error::new(io::ErrorKind::Other, "Segment is closed"));
        }

        if off > self.offset() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Offset beyond current position",
            ));
        }

        // Calculate buffer offset
        let mut boff = 0;

        let mut n = 0;
        if off < self.file_offset {
            // Read from the file
            n = self.file.read_at(bs, self.file_header_offset + off)?;
        } else {
            boff = (off - self.file_offset) as usize;
        }

        let pending = bs.len() - n;
        if pending > 0 {
            let available = self.block.unwritten() - boff;
            let read_chunk_size = std::cmp::min(pending, available);

            if read_chunk_size > 0 {
                let buf = &self.block.buf
                    [self.block.written + boff..self.block.written + boff + read_chunk_size];
                merge_slices(bs, buf);
            }

            if read_chunk_size == pending {
                return Ok(n);
            } else {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "Incomplete read",
                ));
            }
        }

        Ok(n)
    }
}

impl Drop for Segment {
    /// Attempt to fsync data on drop, in case we're running without sync.
    fn drop(&mut self) {
        self.close().ok();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::Cursor;
    use std::io::Seek;
    use tempdir::TempDir;

    #[test]
    fn test_new_empty() {
        let metadata = Metadata::new(None);
        assert_eq!(metadata.data.len(), 0);
    }

    #[test]
    fn test_put_and_get_int() {
        let mut metadata = Metadata::new(None);
        metadata.put_int("age", 25);
        assert_eq!(metadata.get_int("age").unwrap(), 25);
    }

    #[test]
    fn test_bytes_roundtrip() {
        let mut metadata = Metadata::new(None);
        metadata.put_int("age", 30);
        metadata.put_int("num", 40);

        let bytes = metadata.bytes();
        let restored_metadata = Metadata::new(Some(bytes));

        assert_eq!(restored_metadata.get_int("age").unwrap(), 30);
        assert_eq!(restored_metadata.get_int("num").unwrap(), 40);
    }

    #[test]
    fn test_read_field() {
        let data = [0, 0, 0, 5, 65, 66, 67, 68, 69]; // "ABCDE"
        let mut cursor = Cursor::new(&data);
        let result = read_field(&mut cursor).unwrap();
        assert_eq!(result, b"ABCDE");
    }

    #[test]
    fn test_write_field() {
        let mut output = Vec::new();
        let data = b"XYZ";
        write_field(data, &mut output).unwrap();
        assert_eq!(&output, &[0, 0, 0, 3, 88, 89, 90]); // [0, 0, 0, 3] for length and "XYZ" bytes
    }

    #[test]
    fn test_metadata_extension() {
        let id = 12345;
        let opts = Options::default();

        // Create a new metadata using new_file_header
        let mut meta = Metadata::new_file_header(
            id,
            opts.compression_format
                .as_ref()
                .unwrap_or(&CompressionFormat::NoCompression),
            opts.compression_level
                .as_ref()
                .unwrap_or(&CompressionLevel::BestSpeed),
        );

        // Create an extended metadata
        let mut extended_meta = Metadata::new(None);
        extended_meta.put_int("key1", 123);
        extended_meta.put_int("key2", 456);

        // Serialize and extend the extended metadata using bytes
        let extended_bytes = extended_meta.bytes();
        meta.read_from(&mut &extended_bytes[..])
            .expect("Failed to read from bytes");

        // Check if keys from existing metadata are present in the extended metadata
        assert_eq!(meta.get_int(KEY_MAGIC).unwrap(), MAGIC);
        assert_eq!(meta.get_int(KEY_VERSION).unwrap(), VERSION);
        assert_eq!(meta.get_int(KEY_SEGMENT_ID).unwrap(), id);
        assert_eq!(
            meta.get_int(KEY_COMPRESSION_FORMAT).unwrap(),
            CompressionFormat::NoCompression.as_u64()
        );
        assert_eq!(
            meta.get_int(KEY_COMPRESSION_LEVEL).unwrap(),
            CompressionLevel::BestSpeed.as_u64()
        );

        // Check if keys from the extended metadata are present in the extended metadata
        assert_eq!(meta.get_int("key1").unwrap(), 123);
        assert_eq!(meta.get_int("key1").unwrap(), 123);
    }

    #[test]
    fn test_validate_file_header() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        // Create a new segment file and write the header
        let segment_path = temp_dir.path().join("00000000000000000000");
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
    fn test_bad_file_header() {
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
            .open(segment_path)
            .expect("should create file");

        let id = 0;
        write_file_header(&mut file, id, &opts).expect("should write header");
        file.seek(io::SeekFrom::Start(0))
            .expect("should seek to start"); // Reset the cursor

        // Read the header from the file
        let header = read_file_header(&mut file).expect("should read header");

        // Modify the compression level to an unexpected value
        opts.compression_level = Some(CompressionLevel::BestCompression); // This doesn't match the default compression level

        // Validate the file header, expecting an error due to mismatched compression level
        let result = validate_file_header(&header, id, &opts);
        assert!(result.is_err()); // Header validation should throw an error
    }

    #[test]
    fn test_segment_name_with_extension() {
        let index = 42;
        let ext = "log";
        let expected = format!("{:020}.{}", index, ext);
        assert_eq!(segment_name(index, ext), expected);
    }

    #[test]
    fn test_segment_name_without_extension() {
        let index = 42;
        let expected = format!("{:020}", index);
        assert_eq!(segment_name(index, ""), expected);
    }

    #[test]
    fn test_parse_segment_name_with_extension() {
        let name = "00000000000000000042.log";
        let result = parse_segment_name(name).unwrap();
        assert_eq!(result, (42, Some("log".to_string())));
    }

    #[test]
    fn test_parse_segment_name_without_extension() {
        let name = "00000000000000000042";
        let result = parse_segment_name(name).unwrap();
        assert_eq!(result, (42, None));
    }

    #[test]
    fn test_parse_segment_name_invalid_format() {
        let name = "invalid_name";
        let result = parse_segment_name(name);
        assert!(result.is_err());
    }

    #[test]
    fn test_segments_empty_directory() {
        let temp_dir = create_temp_directory();
        let dir = temp_dir.path().to_path_buf();

        let result = get_segment_range(&dir).unwrap();
        assert_eq!(result, (0, 0));
    }

    #[test]
    fn test_segments_non_empty_directory() {
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

    fn create_segment_file(dir: &PathBuf, name: &str) {
        let file_path = dir.join(name);
        let mut file = File::create(file_path).unwrap();
        file.write_all(b"dummy content").unwrap();
    }

    #[test]
    fn test_remaining() {
        let block: Block<4096, WAL_RECORD_HEADER_SIZE> = Block {
            written: 100,
            flushed: 0,
            buf: [0; 4096],
        };
        assert_eq!(block.remaining(), 3996 - WAL_RECORD_HEADER_SIZE);

        let block: Block<4096, 0> = Block {
            written: 100,
            flushed: 0,
            buf: [0; 4096],
        };
        assert_eq!(block.remaining(), 3996);
    }

    #[test]
    fn test_is_full() {
        let block: Block<4096, WAL_RECORD_HEADER_SIZE> = Block {
            written: 4096 - WAL_RECORD_HEADER_SIZE,
            flushed: 0,
            buf: [0; 4096],
        };
        assert!(block.is_full());

        let block: Block<4096, 0> = Block {
            written: 4096,
            flushed: 0,
            buf: [0; 4096],
        };
        assert!(block.is_full());
    }

    #[test]
    fn test_reset() {
        let mut block: Block<4096, WAL_RECORD_HEADER_SIZE> = Block {
            written: 100,
            flushed: 0,
            buf: [1; 4096],
        };
        block.reset();
        assert_eq!(block.buf, [0; 4096]);
        assert_eq!(block.written, 0);
        assert_eq!(block.flushed, 0);
    }

    #[test]
    fn test_aol_append() {
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
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().1);

        // Validate offset after appending
        // 4 + 7 = 11
        assert_eq!(segment.offset(), 11);

        // Test syncing segment
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 4096);

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
        let r = segment.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 4 = 4100
        assert_eq!(segment.offset(), 4100);

        // Test reading from segment after appending
        let mut bs = vec![0; 4];
        let n = segment.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[..]);

        // Test syncing segment again
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing again
        assert_eq!(segment.offset(), 4096 * 2);

        // Test closing segment
        assert!(segment.close().is_ok());
    }

    #[test]
    fn test_aol_reopen_empty_file() {
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
    fn test_segment_reopen() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(0, segment.offset());

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().1);

        // Validate offset after appending
        // 4 + 7 = 11
        assert_eq!(segment.offset(), 11);

        // Test syncing segment
        assert!(segment.sync().is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 4096);

        // Test closing segment
        assert!(segment.close().is_ok());

        drop(segment);

        // Reopen segment
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        assert_eq!(segment.offset(), 4096);

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
        let r = segment.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 4 = 4100
        assert_eq!(segment.offset(), 4100);

        // Test reading from segment after appending
        let mut bs = vec![0; 4];
        let n = segment.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(4, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[..]);

        // Test closing segment
        assert!(segment.close().is_ok());

        // Reopen segment
        let segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");
        // Test initial offset
        assert_eq!(segment.offset(), 4096 * 2);

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(segment);
        drop(temp_dir);
    }

    #[test]
    fn test_segment_reopen_file() {
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
    fn test_segment_corrupted_metadata() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options
        let opts = Options::default();

        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Close the segment
        segment.close().expect("should close segment");

        // Corrupt the segment's metadata by overwriting the first few bytes
        let segment_path = temp_dir.path().join("00000000000000000000");
        let mut corrupted_data = vec![0; 4];

        // Open the file for writing before writing to it
        let mut corrupted_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(segment_path)
            .expect("should open corrupted file");

        corrupted_file
            .write_all(&mut corrupted_data)
            .expect("should write corrupted data to file");

        // Attempt to reopen the segment with corrupted metadata
        let reopened_segment = Segment::open(temp_dir.path(), 0, &opts);
        assert!(reopened_segment.is_err()); // Opening should fail due to corrupted metadata
    }

    #[test]
    fn test_segment_closed_operations() {
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
    fn test_wal_append() {
        // Create a temporary directory
        let temp_dir = TempDir::new("test").expect("should create temp dir");

        // Create segment options and open a segment
        let opts = Options::default().with_wal();
        let mut segment = Segment::open(temp_dir.path(), 0, &opts).expect("should create segment");

        // Test initial offset
        let sz = segment.offset();
        assert_eq!(0, sz);

        // Test appending an empty buffer
        let r = segment.append(&[]);
        assert!(r.is_err());

        // Test appending a non-empty buffer
        let r = segment.append(&[0, 1, 2, 3]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Test appending another buffer
        let r = segment.append(&[4, 5, 6, 7, 8, 9, 10]);
        assert!(r.is_ok());
        assert_eq!(7, r.unwrap().1);

        // Validate offset after appending
        // 7 + 4 + 7 + 7 = 25
        assert_eq!(segment.offset(), 25);

        // Test syncing segment
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing
        assert_eq!(segment.offset(), 4096);

        // Test reading from segment
        let mut bs = vec![0; 11];
        let n = segment.read_at(&mut bs, 0).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[0, 1, 2, 3].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test reading another portion of data from segment
        let mut bs = vec![0; 14];
        let n = segment.read_at(&mut bs, 11).expect("should read");
        assert_eq!(14, n);
        assert_eq!(
            &[4, 5, 6, 7, 8, 9, 10].to_vec(),
            &bs[WAL_RECORD_HEADER_SIZE..]
        );

        // Test reading beyond segment's current size
        let mut bs = vec![0; 14];
        let r = segment.read_at(&mut bs, 4097);
        assert!(r.is_err());

        // Test appending another buffer after syncing
        let r = segment.append(&[11, 12, 13, 14]);
        assert!(r.is_ok());
        assert_eq!(4, r.unwrap().1);

        // Validate offset after appending
        // 4096 + 7 + 4 = 4107
        assert_eq!(segment.offset(), 4107);

        // Test reading from segment after appending
        let mut bs = vec![0; 11];
        let n = segment.read_at(&mut bs, 4096).expect("should read");
        assert_eq!(11, n);
        assert_eq!(&[11, 12, 13, 14].to_vec(), &bs[WAL_RECORD_HEADER_SIZE..]);

        // Test syncing segment again
        let r = segment.sync();
        assert!(r.is_ok());

        // Validate offset after syncing again
        assert_eq!(segment.offset(), 4096 * 2);

        // Test closing segment
        assert!(segment.close().is_ok());
    }
}
