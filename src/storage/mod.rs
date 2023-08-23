pub mod aol;
pub mod wal;

use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Read, Write};

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
/// The constant is set to 8, reflecting the size of the record header structure.
pub const RECORD_HEADER_SIZE: usize = 8;

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
}

impl Options {
    fn default() -> Self {
        Options {
            dir_mode: Some(0o750),              // default directory mode
            file_mode: Some(DEFAULT_FILE_MODE), // default file mode
            compression_format: None,           // default compression format
            compression_level: None,            // default compression level
            metadata: None,                     // default metadata
            extension: None,                    // default extension
            max_file_size: 1 << 26,             // default max file size
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
        }
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.max_file_size <= 0 {
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

    // fn get_int(&self, key: &str) -> Option<u64> {
    //     // Use the generic get method to retrieve bytes and convert to u64
    //     self.get(key)
    //         .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
    // }

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
/// Each frame type indicates a particular state of a record in the log. The enum variants
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
    Term = 0,   // Rest of block is empty.
    Full = 1,   // Full record.
    First = 2,  // First fragment of a record.
    Middle = 3, // Middle fragments of a record.
    Last = 4,   // Final fragment of a record.
}

impl RecordType {
    fn from_u8(value: u8) -> Result<Self, std::io::Error> {
        match value {
            0 => Ok(RecordType::Term),
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

/// Encodes a record with the provided information into the given buffer.
///
/// It has the following format:
///
///	    Record Header
///
///	    0      1          2      3      4      5      6      7      8
///	    +------+----------+------+------+------+------+------+------+
///	    | Type | Reserved |    Length   |         CRC32             |
///	    +------+----------+------+------+------+------+------+------+
///   
fn encode_record(buf: &mut [u8], rec_len: usize, part: &[u8], i: usize) {
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
    // Explicitly zero Reserved bytes just in case
    buf[1] = 0;
    let crc = calculate_crc32(part);
    let len_part = part.len() as u16;
    buf[2..4].copy_from_slice(&len_part.to_be_bytes());
    buf[4..8].copy_from_slice(&crc.to_be_bytes());

    // Copy the 'part' into the buffer starting from the RECORD_HEADER_SIZE offset
    merge_slices(&mut buf[RECORD_HEADER_SIZE..], part); // Pass a mutable reference to buf
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
            .unwrap_or(&CompressionFormat::NoCompression),
        opts.compression_level
            .as_ref()
            .unwrap_or(&CompressionLevel::BestSpeed),
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

// fn validate_file_header(header: &[u8], id: u64, opts: &Options) -> io::Result<()> {
//     let mut meta = Metadata::new(None);
//     meta.read_from(&mut &header[..])?;

//     // Validate individual fields here
//     let magic = meta.get_int(KEY_MAGIC)?;
//     let version = meta.get_int(KEY_VERSION)?;
//     let segment_id = meta.get_int(KEY_SEGMENT_ID)?;
//     let cf = meta.get_int(KEY_COMPRESSION_FORMAT)?;
//     let cl = meta.get_int(KEY_COMPRESSION_LEVEL)?;

//     if magic != MAGIC || version != VERSION || segment_id != id {
//         return Err(io::Error::new(
//             io::ErrorKind::InvalidData,
//             "Invalid header data",
//         ));
//     }

//     // Check the compression format and level against the provided options
//     if let Some(expected_cf) = &opts.compression_format {
//         if cf != expected_cf.as_u64() {
//             return Err(io::Error::new(
//                 io::ErrorKind::InvalidData,
//                 "Invalid compression format",
//             ));
//         }
//     }

//     if let Some(expected_cl) = &opts.compression_level {
//         if cl != expected_cl.as_u64() {
//             return Err(io::Error::new(
//                 io::ErrorKind::InvalidData,
//                 "Invalid compression format",
//             ));
//         }
//     }

//     Ok(())
// }

fn validate_record(record_type: &RecordType, i: usize) -> Result<(), io::Error> {
    match record_type {
        RecordType::Full => {
            if i != 0 {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "unexpected full record",
                ))
            } else {
                Ok(())
            }
        }
        RecordType::First => {
            if i != 0 {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "unexpected first record, dropping buffer",
                ))
            } else {
                Ok(())
            }
        }
        RecordType::Middle => {
            if i == 0 {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "unexpected middle record, dropping buffer",
                ))
            } else {
                Ok(())
            }
        }
        RecordType::Last => {
            if i == 0 {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    "unexpected last record, dropping buffer",
                ))
            } else {
                Ok(())
            }
        }
        _ => Err(io::Error::new(
            io::ErrorKind::Other,
            "invalid last record type",
        )),
    }
}

fn calculate_crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
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
            .open(&segment_path)
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

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(temp_dir);
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
            .open(&segment_path)
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

        // Cleanup: Drop the temp directory, which deletes its contents
        drop(temp_dir);
    }
}
