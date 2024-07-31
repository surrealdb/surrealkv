use std::hash::{Hash, Hasher};

use ahash::{HashSet, HashSetExt};
use bytes::{BufMut, Bytes, BytesMut};

use crate::storage::kv::error::{Error, Result};

/// An enumeration of possible attributes for a key-value pair.
/// Currently, only the `Deleted` attribute is defined.
/// More attribute types can be added as variants.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Attribute {
    Tombstone,
    Deleted,
}

impl Attribute {
    /// Converts a `u8` value into an `Attribute` variant.
    /// Returns `None` if the value does not correspond to a known attribute.
    fn from_u8(value: u8) -> Option<Attribute> {
        match value {
            0 => Some(Attribute::Tombstone),
            1 => Some(Attribute::Deleted),
            _ => None,
        }
    }

    /// Returns a `u8` that represents the kind of the attribute.
    fn kind(&self) -> u8 {
        match self {
            Attribute::Tombstone => 0,
            Attribute::Deleted => 1,
        }
    }

    /// Serializes the attribute into a `Bytes` object.
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(1); // Allocate buffer for 1 byte
        bytes.put_u8(self.kind()); // Serialize the kind of the attribute into the buffer
        bytes.freeze() // Convert BytesMut into Bytes
    }

    /// Deserializes an attribute from a byte slice.
    /// Returns `Error::UnknownAttributeType` if the attribute type is unknown.
    fn deserialize(&self, bytes: &mut &[u8]) -> Result<Attribute> {
        if bytes.is_empty() {
            return Err(Error::UnknownAttributeType);
        }

        let value = bytes[0];
        *bytes = &bytes[1..]; // Consume the attribute byte
        Self::from_u8(value).ok_or(Error::UnknownAttributeType)
    }
}

/// A structure representing metadata for a key-value pair.
/// The metadata consists of a set of attributes.
#[derive(Clone, Debug)]
pub struct Metadata {
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

    pub(crate) fn as_tombstone(&mut self, tombstone: bool) -> Result<()> {
        if tombstone {
            self.attributes.insert(Attribute::Tombstone);
        } else {
            self.attributes.remove(&Attribute::Tombstone);
        }

        Ok(())
    }

    /// Checks if the 'deleted' attribute is present.
    pub(crate) fn is_deleted(&self) -> bool {
        self.attributes.contains(&Attribute::Deleted)
    }

    /// Checks if the 'tombstone' attribute is present.
    pub(crate) fn is_tombstone(&self) -> bool {
        self.attributes.contains(&Attribute::Tombstone)
    }

    /// Checks if either the 'deleted' or 'tombstone' attribute is present.
    pub(crate) fn is_deleted_or_tombstone(&self) -> bool {
        self.is_deleted() || self.is_tombstone()
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

impl Hash for Metadata {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for attribute in &self.attributes {
            attribute.hash(state);
        }
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
        assert!(metadata.is_deleted());

        // Test unsetting 'deleted' attribute
        metadata.as_deleted(false).unwrap();
        assert_eq!(metadata.attributes.len(), 0);
        assert!(!metadata.is_deleted());
    }

    #[test]
    fn bytes() {
        let mut metadata = Metadata::new();

        // Test serialization with 'deleted' attribute
        metadata.as_deleted(true).unwrap();
        assert_eq!(metadata.attributes.len(), 1);
        let bytes = metadata.to_bytes();
        assert_eq!(bytes.len(), 2);
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
        assert_eq!(metadata.is_deleted(), deserialized_metadata.is_deleted());
    }
}
