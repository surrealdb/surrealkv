//! Helpers shared by the tests of this module.

/// The textual forms a formatter or a log line could render `bytes` in.
fn renderings(bytes: &[u8]) -> Vec<String> {
	vec![
		format!("{bytes:?}"),
		format!("{bytes:x?}"),
		format!("{bytes:X?}"),
		format!("{bytes:#x?}"),
		bytes.iter().map(|b| format!("{b:02x}")).collect(),
		bytes.iter().map(|b| format!("{b:02X}")).collect(),
		bytes.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(", "),
		bytes.iter().map(|b| b.to_string()).collect::<Vec<_>>().join(","),
	]
}

/// Panics if `text` renders any of `secrets` whole, as an array, in hex or in decimal.
pub(crate) fn assert_leaks_no_bytes(text: &str, secrets: &[&[u8]]) {
	for secret in secrets {
		for needle in renderings(secret) {
			assert!(!text.contains(needle.as_str()), "output leaks key bytes: {text}");
		}
	}
}

/// Panics if any single byte of `secrets` shows up in `text` as a word of its own, in decimal
/// or in two-digit hex. Only meaningful for secrets made of bytes that cannot collide with the
/// small numbers and names the text is expected to hold, such as bytes of 0xA0 and above.
pub(crate) fn assert_no_byte_tokens(text: &str, secrets: &[&[u8]]) {
	for token in text.split(|c: char| !c.is_ascii_alphanumeric()) {
		for secret in secrets {
			for byte in *secret {
				assert_ne!(token, byte.to_string(), "output leaks a key byte: {text}");
				assert_ne!(
					token.to_lowercase(),
					format!("{byte:02x}"),
					"output leaks a byte: {text}"
				);
			}
		}
	}
}
