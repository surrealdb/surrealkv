//! Local immutable object adapter. Filesystem mechanics are contained here;
//! table and branch code operate only on `ObjectStore`.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::hash as crc32;

use super::{
	ByteRange, DeleteOutcome, ListCursor, ObjectCapabilities, ObjectId, ObjectMetadata, ObjectPage,
	ObjectPrefix, ObjectStore, PutOutcome, PutRequest,
};
use crate::api::{DurabilityClass, ErrorCode, KernelError, KernelResult};

const CONTAINER_MAGIC: [u8; 8] = *b"SKVOBJ01";
const CONTAINER_VERSION: u16 = 1;
const PREFIX_LEN: usize = 26;
const MAX_HEADER_LEN: usize = 4 * 1024 * 1024;
const MAX_ATTRIBUTES: usize = 1024;
const MAX_ATTRIBUTE_KEY: usize = 1024;
const MAX_ATTRIBUTE_VALUE: usize = 64 * 1024;

#[derive(Clone)]
pub(crate) struct LocalObjectStore {
	root: PathBuf,
	page_size: usize,
	temp_counter: Arc<AtomicU64>,
}

impl LocalObjectStore {
	pub(crate) fn open(root: PathBuf, page_size: usize) -> KernelResult<Self> {
		if page_size == 0 {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "page size must be non-zero"));
		}
		fs::create_dir_all(&root).map_err(io_error)?;
		let root = fs::canonicalize(root).map_err(io_error)?;
		Ok(Self {
			root,
			page_size,
			temp_counter: Arc::new(AtomicU64::new(0)),
		})
	}

	fn object_path(&self, id: &ObjectId) -> PathBuf {
		self.root.join(format!("{}.obj", encode_hex(id.0.as_bytes())))
	}

	fn create_temp(&self) -> KernelResult<(PathBuf, File)> {
		for _ in 0..128 {
			let sequence = self.temp_counter.fetch_add(1, Ordering::Relaxed);
			let path = self.root.join(format!(".put-{}-{sequence}.tmp", std::process::id()));
			match OpenOptions::new().write(true).create_new(true).open(&path) {
				Ok(file) => return Ok((path, file)),
				Err(error) if error.kind() == ErrorKind::AlreadyExists => {}
				Err(error) => return Err(io_error(error)),
			}
		}
		Err(KernelError::new(
			ErrorCode::ResourceExhausted,
			"unable to allocate unique object staging file",
		))
	}

	fn sync_root(&self) -> KernelResult<()> {
		File::open(&self.root).and_then(|directory| directory.sync_all()).map_err(io_error)
	}
}

#[async_trait]
impl ObjectStore for LocalObjectStore {
	fn capabilities(&self) -> ObjectCapabilities {
		ObjectCapabilities {
			ranged_reads: true,
			unique_put: true,
			paginated_list: true,
			idempotent_delete: true,
			durability: DurabilityClass::CrashDurable,
		}
	}

	async fn read_range(&self, id: &ObjectId, range: ByteRange) -> KernelResult<Bytes> {
		let path = self.object_path(id);
		tokio::task::spawn_blocking(move || {
			let mut file = File::open(path).map_err(not_found_or_io)?;
			let header = read_header(&mut file)?;
			if range.start > range.end || range.end > header.body_len {
				return Err(KernelError::new(ErrorCode::InvalidArgument, "range exceeds object"));
			}
			let len = usize::try_from(range.end - range.start).map_err(|_| {
				KernelError::new(ErrorCode::ResourceExhausted, "range exceeds address space")
			})?;
			let start = (header.header_len as u64)
				.checked_add(range.start)
				.ok_or_else(|| KernelError::new(ErrorCode::InvalidArgument, "range overflow"))?;
			file.seek(SeekFrom::Start(start)).map_err(io_error)?;
			let mut output = vec![0; len];
			file.read_exact(&mut output).map_err(io_error)?;
			Ok(Bytes::from(output))
		})
		.await
		.map_err(join_error)?
	}

	async fn put_unique(&self, request: PutRequest) -> KernelResult<PutOutcome> {
		let store = self.clone();
		tokio::task::spawn_blocking(move || {
			let header = encode_header(request.body.len(), &request.attributes)?;
			let final_path = store.object_path(&request.id);
			let (temp_path, mut temp) = store.create_temp()?;
			let staged = (|| -> KernelResult<()> {
				temp.write_all(&header).map_err(io_error)?;
				for chunk in request.body.chunks() {
					temp.write_all(chunk).map_err(io_error)?;
				}
				temp.sync_all().map_err(io_error)
			})();
			if let Err(error) = staged {
				let _ = fs::remove_file(&temp_path);
				return Err(error);
			}

			match fs::hard_link(&temp_path, &final_path) {
				Ok(()) => {
					let _ = fs::remove_file(&temp_path);
					store.sync_root()?;
					Ok(PutOutcome::Created)
				}
				Err(error) if error.kind() == ErrorKind::AlreadyExists => {
					let same = existing_matches(&final_path, &request)?;
					let _ = fs::remove_file(&temp_path);
					if same {
						Ok(PutOutcome::AlreadyExistsSame)
					} else {
						Err(KernelError::new(
							ErrorCode::AlreadyExists,
							"object ID already contains different bytes",
						))
					}
				}
				Err(error) => {
					let _ = fs::remove_file(&temp_path);
					Err(io_error(error))
				}
			}
		})
		.await
		.map_err(join_error)?
	}

	async fn metadata(&self, id: &ObjectId) -> KernelResult<ObjectMetadata> {
		let path = self.object_path(id);
		let id = id.clone();
		tokio::task::spawn_blocking(move || {
			let mut file = File::open(path).map_err(not_found_or_io)?;
			let header = read_header(&mut file)?;
			Ok(ObjectMetadata {
				id,
				len: header.body_len,
				attributes: header.attributes,
			})
		})
		.await
		.map_err(join_error)?
	}

	async fn list_page(
		&self,
		prefix: &ObjectPrefix,
		cursor: Option<ListCursor>,
	) -> KernelResult<ObjectPage> {
		let store = self.clone();
		let prefix = prefix.clone();
		tokio::task::spawn_blocking(move || {
			let mut ids = Vec::new();
			for entry in fs::read_dir(&store.root).map_err(io_error)? {
				let entry = entry.map_err(io_error)?;
				let name = entry.file_name();
				let Some(name) = name.to_str() else {
					continue;
				};
				let Some(encoded) = name.strip_suffix(".obj") else {
					continue;
				};
				let Some(decoded) = decode_hex(encoded) else {
					continue;
				};
				let Ok(id) = String::from_utf8(decoded) else {
					continue;
				};
				if id.starts_with(&prefix.0)
					&& cursor.as_ref().is_none_or(|cursor| id.as_str() > cursor.0.as_str())
				{
					ids.push(id);
				}
			}
			ids.sort();
			let has_more = ids.len() > store.page_size;
			ids.truncate(store.page_size);
			let mut objects = Vec::with_capacity(ids.len());
			for id in ids {
				let object_id = ObjectId(id);
				let mut file =
					File::open(store.object_path(&object_id)).map_err(not_found_or_io)?;
				let header = read_header(&mut file)?;
				objects.push(ObjectMetadata {
					id: object_id,
					len: header.body_len,
					attributes: header.attributes,
				});
			}
			let next = has_more.then(|| ListCursor(objects.last().unwrap().id.0.clone()));
			Ok(ObjectPage {
				objects,
				next,
			})
		})
		.await
		.map_err(join_error)?
	}

	async fn delete(&self, id: &ObjectId) -> KernelResult<DeleteOutcome> {
		let store = self.clone();
		let path = self.object_path(id);
		tokio::task::spawn_blocking(move || match fs::remove_file(path) {
			Ok(()) => {
				store.sync_root()?;
				Ok(DeleteOutcome::Deleted)
			}
			Err(error) if error.kind() == ErrorKind::NotFound => Ok(DeleteOutcome::NotFound),
			Err(error) => Err(io_error(error)),
		})
		.await
		.map_err(join_error)?
	}
}

struct ContainerHeader {
	header_len: usize,
	body_len: u64,
	attributes: BTreeMap<String, String>,
}

fn encode_header(body_len: u64, attributes: &BTreeMap<String, String>) -> KernelResult<Bytes> {
	if attributes.len() > MAX_ATTRIBUTES {
		return Err(KernelError::new(ErrorCode::ResourceExhausted, "too many attributes"));
	}
	let mut output = BytesMut::new();
	output.extend_from_slice(&CONTAINER_MAGIC);
	output.put_u16(CONTAINER_VERSION);
	output.put_u32(0);
	output.put_u64(body_len);
	output.put_u32(
		u32::try_from(attributes.len())
			.map_err(|_| KernelError::new(ErrorCode::ResourceExhausted, "too many attributes"))?,
	);
	for (key, value) in attributes {
		if key.len() > MAX_ATTRIBUTE_KEY || value.len() > MAX_ATTRIBUTE_VALUE {
			return Err(KernelError::new(
				ErrorCode::ResourceExhausted,
				"object attribute exceeds limit",
			));
		}
		output.put_u16(u16::try_from(key.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "attribute key exceeds format")
		})?);
		output.put_u32(u32::try_from(value.len()).map_err(|_| {
			KernelError::new(ErrorCode::ResourceExhausted, "attribute value exceeds format")
		})?);
		output.extend_from_slice(key.as_bytes());
		output.extend_from_slice(value.as_bytes());
	}
	let header_len = output
		.len()
		.checked_add(4)
		.ok_or_else(|| KernelError::new(ErrorCode::ResourceExhausted, "header is too large"))?;
	if header_len > MAX_HEADER_LEN {
		return Err(KernelError::new(ErrorCode::ResourceExhausted, "header is too large"));
	}
	output[10..14].copy_from_slice(&(header_len as u32).to_be_bytes());
	let checksum = crc32(&output);
	output.put_u32(checksum);
	Ok(output.freeze())
}

fn read_header(file: &mut File) -> KernelResult<ContainerHeader> {
	let mut prefix = [0; PREFIX_LEN];
	file.read_exact(&mut prefix).map_err(io_error)?;
	if prefix[..8] != CONTAINER_MAGIC
		|| u16::from_be_bytes([prefix[8], prefix[9]]) != CONTAINER_VERSION
	{
		return Err(corruption("invalid local object container"));
	}
	let header_len = u32::from_be_bytes(prefix[10..14].try_into().unwrap()) as usize;
	if !(PREFIX_LEN + 4..=MAX_HEADER_LEN).contains(&header_len) {
		return Err(corruption("invalid local object header length"));
	}
	let mut encoded = vec![0; header_len];
	encoded[..PREFIX_LEN].copy_from_slice(&prefix);
	file.read_exact(&mut encoded[PREFIX_LEN..]).map_err(io_error)?;
	if crc32(&encoded[..header_len - 4])
		!= u32::from_be_bytes(encoded[header_len - 4..].try_into().unwrap())
	{
		return Err(corruption("local object header checksum mismatch"));
	}
	let body_len = u64::from_be_bytes(encoded[14..22].try_into().unwrap());
	let count = u32::from_be_bytes(encoded[22..26].try_into().unwrap()) as usize;
	if count > MAX_ATTRIBUTES {
		return Err(corruption("local object attribute count exceeds limit"));
	}
	let mut offset = PREFIX_LEN;
	let mut attributes = BTreeMap::new();
	for _ in 0..count {
		let key_len = take_u16(&encoded, &mut offset)? as usize;
		let value_len = take_u32(&encoded, &mut offset)? as usize;
		if key_len > MAX_ATTRIBUTE_KEY || value_len > MAX_ATTRIBUTE_VALUE {
			return Err(corruption("local object attribute exceeds limit"));
		}
		let key = take(&encoded, &mut offset, key_len)?;
		let value = take(&encoded, &mut offset, value_len)?;
		let key = String::from_utf8(key.to_vec())
			.map_err(|_| corruption("local object attribute key is not UTF-8"))?;
		let value = String::from_utf8(value.to_vec())
			.map_err(|_| corruption("local object attribute value is not UTF-8"))?;
		if attributes.insert(key, value).is_some() {
			return Err(corruption("duplicate local object attribute"));
		}
	}
	if offset != header_len - 4 {
		return Err(corruption("local object header contains trailing bytes"));
	}
	let file_len = file.metadata().map_err(io_error)?.len();
	if (header_len as u64).checked_add(body_len) != Some(file_len) {
		return Err(corruption("local object body length mismatch"));
	}
	Ok(ContainerHeader {
		header_len,
		body_len,
		attributes,
	})
}

fn existing_matches(path: &Path, request: &PutRequest) -> KernelResult<bool> {
	let mut file = File::open(path).map_err(not_found_or_io)?;
	let header = read_header(&mut file)?;
	if header.body_len != request.body.len() || header.attributes != request.attributes {
		return Ok(false);
	}
	for chunk in request.body.chunks() {
		let mut existing = vec![0; chunk.len()];
		file.read_exact(&mut existing).map_err(io_error)?;
		if existing.as_slice() != chunk.as_ref() {
			return Ok(false);
		}
	}
	Ok(true)
}

fn take<'a>(bytes: &'a [u8], offset: &mut usize, len: usize) -> KernelResult<&'a [u8]> {
	let end =
		offset.checked_add(len).ok_or_else(|| corruption("local object field length overflow"))?;
	let output =
		bytes.get(*offset..end).ok_or_else(|| corruption("local object field is truncated"))?;
	*offset = end;
	Ok(output)
}

fn take_u16(bytes: &[u8], offset: &mut usize) -> KernelResult<u16> {
	Ok(u16::from_be_bytes(
		take(bytes, offset, 2)?.try_into().map_err(|_| corruption("truncated u16"))?,
	))
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> KernelResult<u32> {
	Ok(u32::from_be_bytes(
		take(bytes, offset, 4)?.try_into().map_err(|_| corruption("truncated u32"))?,
	))
}

fn encode_hex(bytes: &[u8]) -> String {
	const DIGITS: &[u8; 16] = b"0123456789abcdef";
	let mut output = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		output.push(DIGITS[(byte >> 4) as usize] as char);
		output.push(DIGITS[(byte & 0x0f) as usize] as char);
	}
	output
}

fn decode_hex(value: &str) -> Option<Vec<u8>> {
	if value.len() % 2 != 0 {
		return None;
	}
	value
		.as_bytes()
		.chunks_exact(2)
		.map(|pair| Some((nibble(pair[0])? << 4) | nibble(pair[1])?))
		.collect()
}

fn nibble(value: u8) -> Option<u8> {
	match value {
		b'0'..=b'9' => Some(value - b'0'),
		b'a'..=b'f' => Some(value - b'a' + 10),
		_ => None,
	}
}

fn not_found_or_io(error: std::io::Error) -> KernelError {
	if error.kind() == ErrorKind::NotFound {
		KernelError::new(ErrorCode::NotFound, "object not found")
	} else {
		io_error(error)
	}
}

fn io_error(error: std::io::Error) -> KernelError {
	KernelError::new(ErrorCode::Unavailable, format!("local object IO: {error}"))
}

fn join_error(error: tokio::task::JoinError) -> KernelError {
	KernelError::new(ErrorCode::Unavailable, format!("local object task failed: {error}"))
}

fn corruption(message: &'static str) -> KernelError {
	KernelError::new(ErrorCode::Corruption, message)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn local_owner_contract_streams_chunks_and_reopens() {
		let directory = tempfile::tempdir().unwrap();
		let store = LocalObjectStore::open(directory.path().join("objects"), 2).unwrap();
		let id = ObjectId("tables/unsafe/../id".to_string());
		let request = PutRequest {
			id: id.clone(),
			body: super::super::ObjectBody::new(vec![
				Bytes::from_static(b"abc"),
				Bytes::from_static(b"def"),
			])
			.unwrap(),
			attributes: BTreeMap::from([("digest".to_string(), "value".to_string())]),
		};
		assert_eq!(store.put_unique(request.clone()).await.unwrap(), PutOutcome::Created);
		assert_eq!(store.put_unique(request).await.unwrap(), PutOutcome::AlreadyExistsSame);
		assert_eq!(
			store.read_range(&id, ByteRange::new(2, 5).unwrap()).await.unwrap(),
			Bytes::from_static(b"cde")
		);

		let reopened = LocalObjectStore::open(directory.path().join("objects"), 2).unwrap();
		assert_eq!(reopened.metadata(&id).await.unwrap().len, 6);
		assert_eq!(reopened.delete(&id).await.unwrap(), DeleteOutcome::Deleted);
		assert_eq!(reopened.delete(&id).await.unwrap(), DeleteOutcome::NotFound);
	}

	#[tokio::test]
	async fn local_owner_regression_refuses_torn_container() {
		let directory = tempfile::tempdir().unwrap();
		let store = LocalObjectStore::open(directory.path().join("objects"), 2).unwrap();
		let id = ObjectId("torn".to_string());
		store
			.put_unique(PutRequest {
				id: id.clone(),
				body: super::super::ObjectBody::from_bytes(Bytes::from_static(b"durable")),
				attributes: BTreeMap::new(),
			})
			.await
			.unwrap();
		let path = store.object_path(&id);
		let file = OpenOptions::new().write(true).open(path).unwrap();
		let shortened = file.metadata().unwrap().len() - 1;
		file.set_len(shortened).unwrap();
		file.sync_all().unwrap();

		assert_eq!(store.metadata(&id).await.unwrap_err().code, ErrorCode::Corruption);
	}
}
