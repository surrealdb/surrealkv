use super::*;

#[test]
fn test_v1_key_decode_with_timestamp() {
	let user_key = b"user_account:12345";
	let seq_num = 42u64;
	let kind = KeyKind::Set;
	let timestamp = 1716000000000u64;

	let mut encoded = user_key.to_vec();
	let trailer = (seq_num << 8) | (kind.to_u8() as u64);
	encoded.extend_from_slice(&trailer.to_le_bytes());
	encoded.extend_from_slice(&timestamp.to_be_bytes());

	let parsed = V1ParsedKey::decode(&encoded);
	assert_eq!(parsed.user_key, user_key);
	assert_eq!(parsed.seq_num, seq_num);
	assert_eq!(parsed.kind, KeyKind::Set);
	assert_eq!(parsed.timestamp, timestamp);
	assert!(!parsed.is_tombstone());
}

#[test]
fn test_v1_key_decode_tombstone() {
	let user_key = b"deleted_key";
	let seq_num = 100u64;
	let kind = KeyKind::Delete;
	let timestamp = 1716000099999u64;

	let mut encoded = user_key.to_vec();
	let trailer = (seq_num << 8) | (kind.to_u8() as u64);
	encoded.extend_from_slice(&trailer.to_le_bytes());
	encoded.extend_from_slice(&timestamp.to_be_bytes());

	let parsed = V1ParsedKey::decode(&encoded);
	assert_eq!(parsed.user_key, user_key);
	assert_eq!(parsed.seq_num, seq_num);
	assert_eq!(parsed.kind, KeyKind::Delete);
	assert_eq!(parsed.timestamp, timestamp);
	assert!(parsed.is_tombstone());
}

#[test]
fn test_footer_magic_validation() {
	let mut buf = vec![0u8; 50];
	buf[0] = 1; // format: LSMV1
	buf[1] = 1; // checksum: crc32c
			 // Fake valid block handles: (0, 100) -> 0x00, 0x64
	buf[2] = 0;
	buf[3] = 100;
	buf[4] = 105;
	buf[5] = 50;

	// Append valid magic
	buf[42..50].copy_from_slice(&V1_MAGIC_FOOTER);

	let footer = Footer::decode(&buf).expect("Footer should decode");
	assert_eq!(footer.format, 1);
	assert_eq!(footer.checksum, 1);
}

#[test]
fn test_footer_bad_magic() {
	let buf = vec![0u8; 50];
	let err = Footer::decode(&buf).unwrap_err();
	assert!(matches!(err, Error::BadMagic { .. }));
}
