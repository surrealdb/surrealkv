use std::collections::HashMap;

use bytes::{Bytes, BytesMut};

use crate::storage::kv::error::Result;

// Enumeration representing different types of attributes
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum AttributeType {
    Deleted,
}

impl AttributeType {
    fn from_u8(value: u8) -> Option<Box<dyn Attribute>> {
        match value {
            0 => Some(Box::new(DeletedAttribute)),
            _ => None,
        }
    }
}

// Trait defining the behavior of an attribute
trait Attribute {
    fn kind(&self) -> AttributeType;
    fn serialize(&self) -> Bytes;
    fn deserialize(&mut self, bytes: &mut &[u8]) -> Result<usize>;
}

// Implementation for a deleted attribute
struct DeletedAttribute;

impl Attribute for DeletedAttribute {
    fn kind(&self) -> AttributeType {
        AttributeType::Deleted
    }

    fn serialize(&self) -> Bytes {
        Bytes::new()
    }

    fn deserialize(&mut self, _: &mut &[u8]) -> Result<usize> {
        Ok(0)
    }
}

// Structure representing metadata for a key-value pair
pub(crate) struct Metadata {
    attributes: HashMap<AttributeType, Box<dyn Attribute>>,
}

impl Metadata {
    // Create a new metadata instance
    pub(crate) fn new() -> Self {
        Metadata {
            attributes: HashMap::new(),
        }
    }

    // Set the 'deleted' attribute based on the provided flag
    pub(crate) fn as_deleted(&mut self, deleted: bool) -> Result<()> {
        if !deleted {
            self.attributes.remove(&AttributeType::Deleted);
            return Ok(());
        }

        if !self.attributes.contains_key(&AttributeType::Deleted) {
            self.attributes
                .insert(AttributeType::Deleted, Box::new(DeletedAttribute));
        }

        Ok(())
    }

    // Check if the 'deleted' attribute is present
    pub(crate) fn deleted(&self) -> bool {
        self.attributes.contains_key(&AttributeType::Deleted)
    }

    // Serialize metadata into a byte vector
    pub(crate) fn bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        for (&attr_kind, attr) in &self.attributes {
            buf.extend_from_slice(&[attr_kind as u8]);
            buf.extend_from_slice(&attr.serialize());
        }

        buf.freeze()
    }

    // Deserialize metadata from Bytes into a Metadata instance
    pub(crate) fn from_bytes(encoded_bytes: &[u8]) -> Result<Self> {
        let mut attributes = HashMap::new();
        let mut cursor = encoded_bytes;

        while !cursor.is_empty() {
            let attr_kind = cursor[0];
            cursor = &cursor[1..]; // Move cursor to the next byte
            if let Some(mut attr) = AttributeType::from_u8(attr_kind) {
                attr.deserialize(&mut cursor)?;
                attributes.insert(attr.kind(), attr);
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
        assert_eq!(bytes[0], AttributeType::Deleted as u8);

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
