pub mod segment;

use std::collections::HashMap;
use std::io::{Read, Write};

/// The size of a single page in bytes.
///
/// The `PAGE_SIZE` constant represents the size of a page used for buffering disk writes in the
/// write-ahead log. It determines the maximum amount of data that can be held in memory before
/// flushing to disk. Larger page sizes can improve write performance but might use more memory.
const PAGE_SIZE: usize = 4096;

/// The magic value for identifying file headers.
///
/// The `MAGIC` constant represents a magic value used in file headers to uniquely identify the
/// format or purpose of the file. It's a hexadecimal value that serves as a signature for file
/// identification and verification.
const MAGIC: u64 = 0xFB98F91D;

/// The version number of the file format.
///
/// The `VERSION` constant represents the version number of the file format used in write-ahead log
/// segments. It provides information about the structure and layout of the data within the segment.
const VERSION: u64 = 1;

/// Constants for key names used in the file header.
const KEY_MAGIC: &str = "magic";
const KEY_VERSION: &str = "version";
const KEY_SEGMENT_ID: &str = "segment_id";
const KEY_COMPRESSION_FORMAT: &str = "compression_format";
const KEY_COMPRESSION_LEVEL: &str = "compression_level";

/// Represents metadata associated with a file.
///
/// The `Metadata` struct defines a container for storing key-value pairs of metadata. This metadata
/// can be used to hold additional information about a file. The data is stored in a hash map where
/// each key is a string and each value is a vector of bytes.
struct Metadata {
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
    fn new_file_header(id: u64, cf: u64, cl: u64) -> Self {
        let mut buf = Metadata::new(None);

        // Set file header key-value pairs using constants
        buf.put_int(KEY_MAGIC, MAGIC);
        buf.put_int(KEY_VERSION, VERSION);
        buf.put_int(KEY_SEGMENT_ID, id);
        buf.put_int(KEY_COMPRESSION_FORMAT, cf);
        buf.put_int(KEY_COMPRESSION_LEVEL, cl);

        buf
    }

    // Returns the serialized bytes representation of Metadata
    fn bytes(&self) -> Vec<u8> {
        let mut b = Vec::new();
        self.write_to(&mut b).unwrap();
        b
    }

    // Reads Metadata from a given reader
    fn read_from<R: Read>(&mut self, reader: &mut R) -> Result<(), std::io::Error> {
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
    fn write_to<W: Write>(&self, writer: &mut W) -> Result<(), std::io::Error> {
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
    fn get_int(&self, key: &str) -> Option<u64> {
        // Use the generic get method to retrieve bytes and convert to u64
        self.get(key)
            .map(|v| u64::from_be_bytes(v.as_slice().try_into().unwrap()))
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

// Reads a field from the given reader
fn read_field<R: Read>(reader: &mut R) -> Result<Vec<u8>, std::io::Error> {
    let mut len_buf = [0; 4];
    reader.read_exact(&mut len_buf)?; // Read 4 bytes for the length
    let len = u32::from_be_bytes(len_buf) as usize; // Convert bytes to length

    let mut fb = vec![0u8; len];
    reader.read_exact(&mut fb)?; // Read the actual field bytes
    Ok(fb)
}

// Writes a field to the given writer
fn write_field<W: Write>(b: &[u8], writer: &mut W) -> Result<(), std::io::Error> {
    let mut len_buf = [0; 4];
    len_buf.copy_from_slice(&(b.len() as u32).to_be_bytes());
    writer.write_all(&len_buf)?; // Write 4 bytes for the length
    writer.write_all(b)?; // Write the actual field bytes
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_new_empty() {
        let metadata = Metadata::new(None);
        assert_eq!(metadata.data.len(), 0);
    }

    #[test]
    fn test_put_and_get_int() {
        let mut metadata = Metadata::new(None);
        metadata.put_int("age", 25);
        assert_eq!(metadata.get_int("age"), Some(25));
    }

    #[test]
    fn test_bytes_roundtrip() {
        let mut metadata = Metadata::new(None);
        metadata.put_int("age", 30);
        metadata.put_int("num", 40);

        let bytes = metadata.bytes();
        let restored_metadata = Metadata::new(Some(bytes));

        assert_eq!(restored_metadata.get_int("age"), Some(30));
        assert_eq!(restored_metadata.get_int("num"), Some(40));
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
}
