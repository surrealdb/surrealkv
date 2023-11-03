use std::collections::HashSet;

use bytes::{Bytes, BytesMut};

use crate::storage::kv::error::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Attribute {
    Deleted,
    // Add more variants for other attribute types
}

impl Attribute {
    fn from_u8(value: u8) -> Option<Attribute> {
        match value {
            0 => Some(Attribute::Deleted),
            _ => None,
        }
    }

    fn kind(&self) -> u8 {
        match self {
            Attribute::Deleted => 0,
        }
    }

    fn serialize(&self) -> Bytes {
        match self {
            Attribute::Deleted => Bytes::new(),
        }
    }

    fn deserialize(&self, bytes: &mut &[u8]) -> Result<Attribute> {
        if bytes.is_empty() {
            return Ok(Attribute::Deleted);
        }

        let _value = bytes[0];
        *bytes = &bytes[1..]; // Consume the attribute byte
        Err(Error::UnknownAttributeType)
    }
}

// Structure representing metadata for a key-value pair
#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    attributes: HashSet<Attribute>,
}

impl Metadata {
    // Create a new metadata instance
    pub(crate) fn new() -> Self {
        Metadata {
            attributes: HashSet::new(),
        }
    }

    // Set the 'deleted' attribute based on the provided flag
    pub(crate) fn as_deleted(&mut self, deleted: bool) -> Result<()> {
        if deleted {
            self.attributes.insert(Attribute::Deleted);
        } else {
            self.attributes.remove(&Attribute::Deleted);
        }

        Ok(())
    }

    // Check if the 'deleted' attribute is present
    pub(crate) fn deleted(&self) -> bool {
        self.attributes.contains(&Attribute::Deleted)
    }

    // Serialize metadata into a byte vector
    pub(crate) fn bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        for attr in &self.attributes {
            buf.extend_from_slice(&[attr.kind()]);
            buf.extend_from_slice(&attr.serialize());
        }

        buf.freeze()
    }

    // Deserialize metadata from Bytes into a Metadata instance
    pub(crate) fn from_bytes(encoded_bytes: &[u8]) -> Result<Self> {
        let mut attributes = HashSet::new();
        let mut cursor = encoded_bytes;

        while !cursor.is_empty() {
            let attr_kind = cursor[0];
            cursor = &cursor[1..]; // Move cursor to the next byte
            if let Some(attr) = Attribute::from_u8(attr_kind) {
                attr.deserialize(&mut cursor)?;
                attributes.insert(attr);
            }
        }

        Ok(Metadata { attributes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_metadata() {
        let metadata = Metadata::new();
        assert!(metadata.attributes.is_empty());
    }

    #[test]
    fn test_as_deleted() {
        let mut metadata = Metadata::new();

        // Test setting 'deleted' attribute
        metadata.as_deleted(true).unwrap();
        assert_eq!(metadata.attributes.len(), 1);
        assert!(metadata.deleted());

        // Test unsetting 'deleted' attribute
        metadata.as_deleted(false).unwrap();
        assert_eq!(metadata.attributes.len(), 0);
        assert!(!metadata.deleted());
    }

    #[test]
    fn test_bytes() {
        let mut metadata = Metadata::new();

        // Test serialization with 'deleted' attribute
        metadata.as_deleted(true).unwrap();
        let bytes = metadata.bytes();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], Attribute::Deleted as u8);

        // Test serialization without 'deleted' attribute
        metadata.as_deleted(false).unwrap();
        let bytes = metadata.bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn test_from_bytes() {
        let mut metadata = Metadata::new();
        metadata.as_deleted(true).unwrap();

        let bytes = metadata.bytes();
        let deserialized_metadata = Metadata::from_bytes(bytes.as_ref()).unwrap();

        assert_eq!(
            metadata.attributes.len(),
            deserialized_metadata.attributes.len()
        );
        assert_eq!(metadata.deleted(), deserialized_metadata.deleted());
    }
}
