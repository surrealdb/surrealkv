use bytes::{Bytes, BytesMut};
use hashbrown::HashSet;

use crate::storage::kv::error::{Error, Result};

/// An enumeration of possible attributes for a key-value pair.
/// Currently, only the `Deleted` attribute is defined.
/// More attribute types can be added as variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Attribute {
    Deleted,
}

impl Attribute {
    /// Converts a `u8` value into an `Attribute` variant.
    /// Returns `None` if the value does not correspond to a known attribute.
    fn from_u8(value: u8) -> Option<Attribute> {
        match value {
            0 => Some(Attribute::Deleted),
            _ => None,
        }
    }

    /// Returns a `u8` that represents the kind of the attribute.
    fn kind(&self) -> u8 {
        match self {
            Attribute::Deleted => 0,
        }
    }

    /// Serializes the attribute into a `Bytes` object.
    fn serialize(&self) -> Bytes {
        match self {
            Attribute::Deleted => Bytes::new(),
        }
    }

    /// Deserializes an attribute from a byte slice.
    /// Returns `Error::UnknownAttributeType` if the attribute type is unknown.
    fn deserialize(&self, bytes: &mut &[u8]) -> Result<Attribute> {
        if bytes.is_empty() {
            return Ok(Attribute::Deleted);
        }

        let _value = bytes[0];
        *bytes = &bytes[1..]; // Consume the attribute byte
        Err(Error::UnknownAttributeType)
    }
}

/// A structure representing metadata for a key-value pair.
/// The metadata consists of a set of attributes.
#[derive(Clone, Debug)]
pub(crate) struct Metadata {
    attributes: HashSet<Attribute>,
}

impl Metadata {
    /// Creates a new `Metadata` instance with no attributes.
    pub(crate) fn new() -> Self {
        Metadata {
            attributes: HashSet::new(),
        }
    }

    /// Sets or removes the 'deleted' attribute based on the provided flag.
    pub(crate) fn as_deleted(&mut self, deleted: bool) -> Result<()> {
        if deleted {
            self.attributes.insert(Attribute::Deleted);
        } else {
            self.attributes.remove(&Attribute::Deleted);
        }

        Ok(())
    }

    /// Checks if the 'deleted' attribute is present.
    pub(crate) fn deleted(&self) -> bool {
        self.attributes.contains(&Attribute::Deleted)
    }

    /// Serializes the metadata into a byte vector.
    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::new();

        for attr in &self.attributes {
            buf.extend_from_slice(&[attr.kind()]);
            buf.extend_from_slice(&attr.serialize());
        }

        buf.freeze()
    }

    /// Deserializes metadata from a byte slice into a `Metadata` instance.
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
    fn new_metadata() {
        let metadata = Metadata::new();
        assert!(metadata.attributes.is_empty());
    }

    #[test]
    fn as_deleted() {
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
    fn bytes() {
        let mut metadata = Metadata::new();

        // Test serialization with 'deleted' attribute
        metadata.as_deleted(true).unwrap();
        let bytes = metadata.to_bytes();
        assert_eq!(bytes.len(), 1);
        assert_eq!(bytes[0], Attribute::Deleted as u8);

        // Test serialization without 'deleted' attribute
        metadata.as_deleted(false).unwrap();
        let bytes = metadata.to_bytes();
        assert!(bytes.is_empty());
    }

    #[test]
    fn from_bytes() {
        let mut metadata = Metadata::new();
        metadata.as_deleted(true).unwrap();

        let bytes = metadata.to_bytes();
        let deserialized_metadata = Metadata::from_bytes(bytes.as_ref()).unwrap();

        assert_eq!(
            metadata.attributes.len(),
            deserialized_metadata.attributes.len()
        );
        assert_eq!(metadata.deleted(), deserialized_metadata.deleted());
    }
}
