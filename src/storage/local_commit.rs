//! Local single-writer authority: append-only framed roots plus two durable
//! root slots. Recovery follows the newest valid slot and ignores journal
//! suffixes that were never installed.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use fs2::FileExt;
use parking_lot::Mutex;

use super::{
	AuthorityRoot, AuthoritySession, CommitCapabilities, CommitOutcome, CommitProposal,
	CommitStore, OpenMode, ReconcileOutcome,
};
use crate::api::{
	AuthorityFence, DurabilityClass, ErrorCode, KernelError, KernelResult, OperationId, SessionId,
};

const FRAME_MAGIC: [u8; 8] = *b"SKVCMT01";
const SLOT_MAGIC: [u8; 8] = *b"SKVSLOT1";
const VERSION: u16 = 1;
const FRAME_PREFIX_LEN: usize = 38;
const SLOT_LEN: usize = 38;
const MAX_ROOT_LEN: usize = 128 * 1024 * 1024;

type OperationLedger = BTreeMap<OperationId, (AuthorityFence, Bytes)>;

struct State {
	root: AuthorityRoot,
	writer_epoch: u64,
	next_session: u64,
	operations: OperationLedger,
}

#[derive(Clone, Copy)]
struct Slot {
	fence: AuthorityFence,
	offset: u64,
	frame_len: u64,
}

struct Frame {
	fence: AuthorityFence,
	operation: OperationId,
	root: Bytes,
	encoded_len: u64,
}

pub(crate) struct LocalCommitStore {
	root_dir: PathBuf,
	journal_path: PathBuf,
	_lock: File,
	state: Arc<Mutex<State>>,
	temp_counter: AtomicU64,
}

impl LocalCommitStore {
	pub(crate) fn open(root_dir: PathBuf) -> KernelResult<Self> {
		fs::create_dir_all(&root_dir).map_err(io_error)?;
		let root_dir = fs::canonicalize(root_dir).map_err(io_error)?;
		let lock_path = root_dir.join("writer.lock");
		let lock = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.truncate(false)
			.open(lock_path)
			.map_err(io_error)?;
		lock.try_lock_exclusive().map_err(|error| {
			KernelError::new(
				ErrorCode::Fenced,
				format!("local authority already has a writer: {error}"),
			)
		})?;
		let journal_path = root_dir.join("roots.journal");
		OpenOptions::new()
			.read(true)
			.append(true)
			.create(true)
			.open(&journal_path)
			.map_err(io_error)?;
		let (root, operations) = recover(&root_dir, &journal_path)?;
		Ok(Self {
			root_dir,
			journal_path,
			_lock: lock,
			state: Arc::new(Mutex::new(State {
				root,
				writer_epoch: 0,
				next_session: 0,
				operations,
			})),
			temp_counter: AtomicU64::new(0),
		})
	}

	fn publish_slot(&self, slot: Slot) -> KernelResult<()> {
		let index = slot.fence.0 % 2;
		let final_path = self.root_dir.join(format!("root-{index}.slot"));
		let sequence = self.temp_counter.fetch_add(1, Ordering::Relaxed);
		let temp_path =
			self.root_dir.join(format!(".root-{index}-{}-{sequence}.tmp", std::process::id()));
		let encoded = encode_slot(slot);
		let result = (|| -> KernelResult<()> {
			let mut file = OpenOptions::new()
				.write(true)
				.create_new(true)
				.open(&temp_path)
				.map_err(io_error)?;
			file.write_all(&encoded).map_err(io_error)?;
			file.sync_all().map_err(io_error)?;
			fs::rename(&temp_path, &final_path).map_err(io_error)?;
			File::open(&self.root_dir).and_then(|directory| directory.sync_all()).map_err(io_error)
		})();
		if result.is_err() {
			let _ = fs::remove_file(temp_path);
		}
		result
	}
}

#[async_trait]
impl CommitStore for LocalCommitStore {
	fn capabilities(&self) -> CommitCapabilities {
		CommitCapabilities {
			conditional_publish: true,
			writer_fencing: true,
			reconcile_unknown: true,
			durability: DurabilityClass::CrashDurable,
		}
	}

	async fn open(&self, mode: OpenMode) -> KernelResult<AuthoritySession> {
		let mut state = self.state.lock();
		state.next_session = state.next_session.checked_add(1).ok_or_else(|| {
			KernelError::new(ErrorCode::ResourceExhausted, "session ID exhausted")
		})?;
		if mode == OpenMode::ReadWrite {
			state.writer_epoch = state.writer_epoch.checked_add(1).ok_or_else(|| {
				KernelError::new(ErrorCode::ResourceExhausted, "writer epoch exhausted")
			})?;
		}
		Ok(AuthoritySession {
			id: SessionId::from_u128(state.next_session as u128),
			writer_epoch: state.writer_epoch,
			mode,
		})
	}

	async fn load_root(&self, session: &AuthoritySession) -> KernelResult<AuthorityRoot> {
		let state = self.state.lock();
		if session.mode == OpenMode::ReadWrite && session.writer_epoch != state.writer_epoch {
			return Err(KernelError::new(ErrorCode::Fenced, "writer session is stale"));
		}
		Ok(state.root.clone())
	}

	async fn commit(
		&self,
		session: &mut AuthoritySession,
		expected: AuthorityFence,
		proposal: CommitProposal,
	) -> KernelResult<CommitOutcome> {
		let mut state = self.state.lock();
		if let Some((fence, bytes)) = state.operations.get(&proposal.operation) {
			if *bytes != proposal.root {
				return Err(KernelError::new(
					ErrorCode::InvalidArgument,
					"operation ID reused with a different root",
				));
			}
			return Ok(CommitOutcome::Confirmed(*fence));
		}
		if session.mode != OpenMode::ReadWrite || session.writer_epoch != state.writer_epoch {
			return Ok(CommitOutcome::Fenced);
		}
		if expected != state.root.fence {
			return Ok(CommitOutcome::Conflict(state.root.fence));
		}
		if proposal.root.len() > MAX_ROOT_LEN {
			return Err(KernelError::new(ErrorCode::ResourceExhausted, "root exceeds local limit"));
		}
		let fence = AuthorityFence(state.root.fence.0.checked_add(1).ok_or_else(|| {
			KernelError::new(ErrorCode::ResourceExhausted, "authority fence exhausted")
		})?);
		let encoded = encode_frame(fence, proposal.operation, &proposal.root)?;
		let mut journal = OpenOptions::new()
			.read(true)
			.append(true)
			.open(&self.journal_path)
			.map_err(io_error)?;
		let offset = journal.seek(SeekFrom::End(0)).map_err(io_error)?;
		journal.write_all(&encoded).map_err(io_error)?;
		journal.sync_data().map_err(io_error)?;
		self.publish_slot(Slot {
			fence,
			offset,
			frame_len: encoded.len() as u64,
		})?;
		state.root = AuthorityRoot {
			fence,
			bytes: proposal.root.clone(),
		};
		state.operations.insert(proposal.operation, (fence, proposal.root));
		Ok(CommitOutcome::Confirmed(fence))
	}

	async fn reconcile(&self, operation: OperationId) -> KernelResult<ReconcileOutcome> {
		Ok(self
			.state
			.lock()
			.operations
			.get(&operation)
			.map(|(fence, _)| *fence)
			.map_or(ReconcileOutcome::NotCommitted, ReconcileOutcome::Confirmed))
	}
}

fn recover(root_dir: &Path, journal_path: &Path) -> KernelResult<(AuthorityRoot, OperationLedger)> {
	let mut slots = Vec::new();
	for index in 0..2 {
		let path = root_dir.join(format!("root-{index}.slot"));
		match fs::read(path) {
			Ok(bytes) => slots.push(decode_slot(&bytes)?),
			Err(error) if error.kind() == ErrorKind::NotFound => {}
			Err(error) => return Err(io_error(error)),
		}
	}
	let Some(authority) = slots.into_iter().max_by_key(|slot| slot.fence) else {
		return Ok((
			AuthorityRoot {
				fence: AuthorityFence(0),
				bytes: Bytes::new(),
			},
			BTreeMap::new(),
		));
	};
	let journal_len = fs::metadata(journal_path).map_err(io_error)?.len();
	let authoritative_end = authority
		.offset
		.checked_add(authority.frame_len)
		.ok_or_else(|| corruption("root slot frame range overflow"))?;
	if authoritative_end > journal_len {
		return Err(corruption("root slot points beyond journal"));
	}
	let mut journal = File::open(journal_path).map_err(io_error)?;
	let mut offset = 0u64;
	let mut expected_fence = AuthorityFence(1);
	let mut operations = BTreeMap::new();
	let mut root = None;
	while offset < authoritative_end {
		let frame = read_frame(&mut journal, offset, authoritative_end)?;
		if frame.fence != expected_fence {
			return Err(corruption("local authority fence chain is discontinuous"));
		}
		if operations.insert(frame.operation, (frame.fence, frame.root.clone())).is_some() {
			return Err(corruption("duplicate operation in local authority journal"));
		}
		offset = offset
			.checked_add(frame.encoded_len)
			.ok_or_else(|| corruption("journal offset overflow"))?;
		expected_fence = AuthorityFence(expected_fence.0 + 1);
		if offset == authoritative_end {
			root = Some(AuthorityRoot {
				fence: frame.fence,
				bytes: frame.root,
			});
		}
	}
	let root = root.ok_or_else(|| corruption("root slot does not end on a frame"))?;
	if root.fence != authority.fence || authority.frame_len == 0 {
		return Err(corruption("root slot does not match authoritative frame"));
	}
	Ok((root, operations))
}

fn encode_frame(
	fence: AuthorityFence,
	operation: OperationId,
	root: &Bytes,
) -> KernelResult<Bytes> {
	let mut output = BytesMut::with_capacity(FRAME_PREFIX_LEN + root.len() + 4);
	output.extend_from_slice(&FRAME_MAGIC);
	output.put_u16(VERSION);
	output.put_u64(fence.0);
	output.extend_from_slice(&operation.0);
	output.put_u32(u32::try_from(root.len()).map_err(|_| {
		KernelError::new(ErrorCode::ResourceExhausted, "root exceeds journal format")
	})?);
	output.extend_from_slice(root);
	let checksum = crc32fast::hash(&output);
	output.put_u32(checksum);
	Ok(output.freeze())
}

fn read_frame(file: &mut File, offset: u64, limit: u64) -> KernelResult<Frame> {
	file.seek(SeekFrom::Start(offset)).map_err(io_error)?;
	let mut prefix = [0; FRAME_PREFIX_LEN];
	file.read_exact(&mut prefix).map_err(|_| corruption("authority frame prefix is truncated"))?;
	if prefix[..8] != FRAME_MAGIC
		|| u16::from_be_bytes(prefix[8..10].try_into().unwrap()) != VERSION
	{
		return Err(corruption("invalid authority frame"));
	}
	let root_len = u32::from_be_bytes(prefix[34..38].try_into().unwrap()) as usize;
	if root_len > MAX_ROOT_LEN {
		return Err(corruption("authority root exceeds limit"));
	}
	let encoded_len = (FRAME_PREFIX_LEN as u64)
		.checked_add(root_len as u64)
		.and_then(|len| len.checked_add(4))
		.ok_or_else(|| corruption("authority frame length overflow"))?;
	if offset.checked_add(encoded_len).is_none_or(|end| end > limit) {
		return Err(corruption("authority frame exceeds installed journal range"));
	}
	let mut encoded = vec![0; encoded_len as usize];
	encoded[..FRAME_PREFIX_LEN].copy_from_slice(&prefix);
	file.read_exact(&mut encoded[FRAME_PREFIX_LEN..])
		.map_err(|_| corruption("authority frame is truncated"))?;
	let body_len = encoded.len() - 4;
	if crc32fast::hash(&encoded[..body_len])
		!= u32::from_be_bytes(encoded[body_len..].try_into().unwrap())
	{
		return Err(corruption("authority frame checksum mismatch"));
	}
	let mut operation = [0; 16];
	operation.copy_from_slice(&encoded[18..34]);
	Ok(Frame {
		fence: AuthorityFence(u64::from_be_bytes(encoded[10..18].try_into().unwrap())),
		operation: OperationId(operation),
		root: Bytes::copy_from_slice(&encoded[FRAME_PREFIX_LEN..FRAME_PREFIX_LEN + root_len]),
		encoded_len,
	})
}

fn encode_slot(slot: Slot) -> Bytes {
	let mut output = BytesMut::with_capacity(SLOT_LEN);
	output.extend_from_slice(&SLOT_MAGIC);
	output.put_u16(VERSION);
	output.put_u64(slot.fence.0);
	output.put_u64(slot.offset);
	output.put_u64(slot.frame_len);
	let checksum = crc32fast::hash(&output);
	output.put_u32(checksum);
	output.freeze()
}

fn decode_slot(bytes: &[u8]) -> KernelResult<Slot> {
	if bytes.len() != SLOT_LEN
		|| bytes[..8] != SLOT_MAGIC
		|| u16::from_be_bytes(bytes[8..10].try_into().unwrap()) != VERSION
		|| crc32fast::hash(&bytes[..SLOT_LEN - 4])
			!= u32::from_be_bytes(bytes[SLOT_LEN - 4..].try_into().unwrap())
	{
		return Err(corruption("invalid local authority root slot"));
	}
	Ok(Slot {
		fence: AuthorityFence(u64::from_be_bytes(bytes[10..18].try_into().unwrap())),
		offset: u64::from_be_bytes(bytes[18..26].try_into().unwrap()),
		frame_len: u64::from_be_bytes(bytes[26..34].try_into().unwrap()),
	})
}

fn io_error(error: std::io::Error) -> KernelError {
	KernelError::new(ErrorCode::Unavailable, format!("local authority IO: {error}"))
}

fn corruption(message: &'static str) -> KernelError {
	KernelError::new(ErrorCode::Corruption, message)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn local_authority_owner_regression_reopens_and_fences_sessions() {
		let directory = tempfile::tempdir().unwrap();
		let path = directory.path().join("authority");
		let store = LocalCommitStore::open(path.clone()).unwrap();
		let mut first = CommitStore::open(&store, OpenMode::ReadWrite).await.unwrap();
		assert_eq!(
			store
				.commit(
					&mut first,
					AuthorityFence(0),
					CommitProposal {
						operation: OperationId::from_u128(1),
						root: Bytes::from_static(b"one"),
					},
				)
				.await
				.unwrap(),
			CommitOutcome::Confirmed(AuthorityFence(1))
		);
		let mut second = CommitStore::open(&store, OpenMode::ReadWrite).await.unwrap();
		assert_eq!(
			store
				.commit(
					&mut first,
					AuthorityFence(1),
					CommitProposal {
						operation: OperationId::from_u128(2),
						root: Bytes::from_static(b"stale"),
					},
				)
				.await
				.unwrap(),
			CommitOutcome::Fenced
		);
		store
			.commit(
				&mut second,
				AuthorityFence(1),
				CommitProposal {
					operation: OperationId::from_u128(3),
					root: Bytes::from_static(b"two"),
				},
			)
			.await
			.unwrap();
		drop(store);

		let reopened = LocalCommitStore::open(path).unwrap();
		let reader = CommitStore::open(&reopened, OpenMode::ReadOnly).await.unwrap();
		assert_eq!(reopened.load_root(&reader).await.unwrap().bytes, b"two"[..]);
		assert_eq!(
			reopened.reconcile(OperationId::from_u128(3)).await.unwrap(),
			ReconcileOutcome::Confirmed(AuthorityFence(2))
		);
	}

	#[test]
	fn local_authority_recovery_ignores_uninstalled_journal_suffix() {
		let directory = tempfile::tempdir().unwrap();
		let path = directory.path().join("authority");
		let store = LocalCommitStore::open(path.clone()).unwrap();
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			let mut session = CommitStore::open(&store, OpenMode::ReadWrite).await.unwrap();
			store
				.commit(
					&mut session,
					AuthorityFence(0),
					CommitProposal {
						operation: OperationId::from_u128(1),
						root: Bytes::from_static(b"installed"),
					},
				)
				.await
				.unwrap();
		});
		let suffix = encode_frame(
			AuthorityFence(2),
			OperationId::from_u128(2),
			&Bytes::from_static(b"not-installed"),
		)
		.unwrap();
		OpenOptions::new()
			.append(true)
			.open(&store.journal_path)
			.unwrap()
			.write_all(&suffix)
			.unwrap();
		drop(store);

		let reopened = LocalCommitStore::open(path).unwrap();
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			let reader = CommitStore::open(&reopened, OpenMode::ReadOnly).await.unwrap();
			assert_eq!(reopened.load_root(&reader).await.unwrap().bytes, b"installed"[..]);
			assert_eq!(
				reopened.reconcile(OperationId::from_u128(2)).await.unwrap(),
				ReconcileOutcome::NotCommitted
			);
		});
	}

	#[test]
	fn local_authority_present_but_torn_slot_fails_closed() {
		let directory = tempfile::tempdir().unwrap();
		let path = directory.path().join("authority");
		let store = LocalCommitStore::open(path.clone()).unwrap();
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			let mut session = CommitStore::open(&store, OpenMode::ReadWrite).await.unwrap();
			for (expected, operation, root) in
				[(0, 1, Bytes::from_static(b"one")), (1, 2, Bytes::from_static(b"two"))]
			{
				store
					.commit(
						&mut session,
						AuthorityFence(expected),
						CommitProposal {
							operation: OperationId::from_u128(operation),
							root,
						},
					)
					.await
					.unwrap();
			}
		});
		let newest = path.join("root-0.slot");
		let file = OpenOptions::new().write(true).open(newest).unwrap();
		file.set_len((SLOT_LEN - 1) as u64).unwrap();
		file.sync_all().unwrap();
		drop(store);

		let error = LocalCommitStore::open(path)
			.err()
			.expect("present corrupt slot must not silently fall back");
		assert_eq!(error.code, ErrorCode::Corruption);
	}

	#[test]
	fn local_authority_valid_slot_pointing_to_torn_frame_fails_closed() {
		let directory = tempfile::tempdir().unwrap();
		let path = directory.path().join("authority");
		let store = LocalCommitStore::open(path.clone()).unwrap();
		let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
		runtime.block_on(async {
			let mut session = CommitStore::open(&store, OpenMode::ReadWrite).await.unwrap();
			store
				.commit(
					&mut session,
					AuthorityFence(0),
					CommitProposal {
						operation: OperationId::from_u128(1),
						root: Bytes::from_static(b"installed"),
					},
				)
				.await
				.unwrap();
		});
		let journal = OpenOptions::new().write(true).open(&store.journal_path).unwrap();
		let shortened = journal.metadata().unwrap().len() - 1;
		journal.set_len(shortened).unwrap();
		journal.sync_all().unwrap();
		drop(store);

		let error = LocalCommitStore::open(path).err().expect("torn installed frame must fail");
		assert_eq!(error.code, ErrorCode::Corruption);
	}
}
