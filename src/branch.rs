//! Branch selectors, write operations, and reference-model tests.

use std::collections::BTreeMap;

use bytes::Bytes;

use super::api::{
	BranchGeneration, BranchId, CommitTimestamp, CommitVersion, ErrorCode, KernelError,
	KernelResult,
};

pub(crate) const DEFAULT_BRANCH_NAME: &str = "main";
#[cfg(test)]
const MAX_BRANCH_NAME_LEN: usize = 255;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchRecord {
	pub(crate) id: BranchId,
	pub(crate) name: String,
	pub(crate) generation: BranchGeneration,
	pub(crate) head: CommitVersion,
	pub(crate) deleted: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BranchCatalog {
	default_branch: BranchId,
	records: BTreeMap<BranchId, BranchRecord>,
	live_names: BTreeMap<String, BranchId>,
	name_generations: BTreeMap<String, BranchGeneration>,
}

impl BranchCatalog {
	pub(crate) fn new(default_branch: BranchId) -> Self {
		let record = BranchRecord {
			id: default_branch,
			name: DEFAULT_BRANCH_NAME.to_owned(),
			generation: BranchGeneration(0),
			head: CommitVersion(0),
			deleted: false,
		};
		Self {
			default_branch,
			records: BTreeMap::from([(default_branch, record)]),
			live_names: BTreeMap::from([(DEFAULT_BRANCH_NAME.to_owned(), default_branch)]),
			name_generations: BTreeMap::from([(
				DEFAULT_BRANCH_NAME.to_owned(),
				BranchGeneration(0),
			)]),
		}
	}

	#[cfg(test)]
	pub(crate) fn default_branch(&self) -> BranchId {
		self.default_branch
	}

	#[cfg(test)]
	pub(crate) fn create(
		&mut self,
		id: BranchId,
		name: &str,
		head: CommitVersion,
	) -> KernelResult<BranchRecord> {
		validate_branch_name(name)?;
		if self.records.contains_key(&id) || self.live_names.contains_key(name) {
			return Err(KernelError::new(ErrorCode::AlreadyExists, "branch already exists"));
		}
		let generation = self
			.name_generations
			.get(name)
			.map_or(BranchGeneration(0), |previous| BranchGeneration(previous.0 + 1));
		let record = BranchRecord {
			id,
			name: name.to_owned(),
			generation,
			head,
			deleted: false,
		};
		self.records.insert(id, record.clone());
		self.live_names.insert(name.to_owned(), id);
		self.name_generations.insert(name.to_owned(), generation);
		Ok(record)
	}

	#[cfg(test)]
	pub(crate) fn get(&self, id: BranchId) -> KernelResult<&BranchRecord> {
		self.records
			.get(&id)
			.filter(|record| !record.deleted)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))
	}

	#[cfg(test)]
	pub(crate) fn get_by_name(&self, name: &str) -> KernelResult<&BranchRecord> {
		let id = self
			.live_names
			.get(name)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		self.get(*id)
	}

	pub(crate) fn validate_owner(
		&self,
		id: BranchId,
		generation: BranchGeneration,
	) -> KernelResult<&BranchRecord> {
		let record = self
			.records
			.get(&id)
			.ok_or_else(|| KernelError::new(ErrorCode::Fenced, "branch identity is stale"))?;
		if record.deleted || record.generation != generation {
			return Err(KernelError::new(ErrorCode::Fenced, "branch generation is stale"));
		}
		Ok(record)
	}

	#[cfg(test)]
	pub(crate) fn list(&self) -> impl Iterator<Item = &BranchRecord> {
		self.live_names.values().filter_map(|id| self.records.get(id))
	}

	#[cfg(test)]
	pub(crate) fn advance_head(
		&mut self,
		id: BranchId,
		generation: BranchGeneration,
		expected: CommitVersion,
		new_head: CommitVersion,
	) -> KernelResult<()> {
		let record = self
			.records
			.get_mut(&id)
			.filter(|record| !record.deleted)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		if record.generation != generation {
			return Err(KernelError::new(ErrorCode::Fenced, "branch generation is stale"));
		}
		if record.head != expected || new_head <= expected {
			return Err(KernelError::new(ErrorCode::Conflict, "branch head changed"));
		}
		record.head = new_head;
		Ok(())
	}

	#[cfg(test)]
	pub(crate) fn delete(&mut self, id: BranchId) -> KernelResult<bool> {
		if id == self.default_branch {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"default branch cannot be deleted",
			));
		}
		let record = self
			.records
			.get_mut(&id)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		if record.deleted {
			return Ok(false);
		}
		record.deleted = true;
		self.live_names.remove(&record.name);
		Ok(true)
	}
}

#[cfg(test)]
fn validate_branch_name(name: &str) -> KernelResult<()> {
	if name.is_empty()
		|| name.len() > MAX_BRANCH_NAME_LEN
		|| name != name.trim()
		|| name.bytes().any(|byte| byte.is_ascii_control())
	{
		return Err(KernelError::new(ErrorCode::InvalidArgument, "invalid branch name"));
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadSelector {
	Latest,
	Version(CommitVersion),
	Timestamp(CommitTimestamp),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WriteOperation {
	Put {
		key: Bytes,
		value: Bytes,
		expires_at: Option<CommitTimestamp>,
	},
	Delete {
		key: Bytes,
	},
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(test)]
pub(crate) struct ModelCommitReceipt {
	pub(crate) version: CommitVersion,
	pub(crate) timestamp: CommitTimestamp,
	pub(crate) branch_head: CommitVersion,
}

#[derive(Clone, Debug)]
#[cfg(test)]
struct ModelValue {
	version: CommitVersion,
	timestamp: CommitTimestamp,
	value: Option<Bytes>,
	expires_at: Option<CommitTimestamp>,
}

#[derive(Clone, Debug)]
#[cfg(test)]
struct ModelBranch {
	name: String,
	generation: BranchGeneration,
	head: CommitVersion,
	deleted: bool,
	history: BTreeMap<Bytes, Vec<ModelValue>>,
}

/// Independent logical oracle. Copying state during fork is intentional here:
/// the model optimizes for obvious semantics, not implementation shape.
#[cfg(test)]
pub(crate) struct BranchModel {
	branches: BTreeMap<BranchId, ModelBranch>,
	names: BTreeMap<String, BranchId>,
	name_generations: BTreeMap<String, BranchGeneration>,
	timeline: Vec<(CommitTimestamp, CommitVersion)>,
	global_version: CommitVersion,
	current_time: CommitTimestamp,
	steps: usize,
}

#[cfg(test)]
impl BranchModel {
	pub(crate) fn new(main_id: BranchId) -> Self {
		let main = ModelBranch {
			name: "main".to_string(),
			generation: BranchGeneration(0),
			head: CommitVersion(0),
			deleted: false,
			history: BTreeMap::new(),
		};
		Self {
			branches: BTreeMap::from([(main_id, main)]),
			names: BTreeMap::from([("main".to_string(), main_id)]),
			name_generations: BTreeMap::from([("main".to_string(), BranchGeneration(0))]),
			timeline: Vec::new(),
			global_version: CommitVersion(0),
			current_time: CommitTimestamp(0),
			steps: 0,
		}
	}

	pub(crate) fn advance_time(&mut self, timestamp: CommitTimestamp) -> KernelResult<()> {
		if timestamp < self.current_time {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"model clock cannot move backwards",
			));
		}
		self.current_time = timestamp;
		self.steps += 1;
		Ok(())
	}

	pub(crate) fn head(&self, branch: BranchId) -> KernelResult<CommitVersion> {
		Ok(self.live_branch(branch)?.head)
	}

	pub(crate) fn generation(&self, branch: BranchId) -> KernelResult<BranchGeneration> {
		Ok(self.live_branch(branch)?.generation)
	}

	pub(crate) fn steps(&self) -> usize {
		self.steps
	}

	pub(crate) fn commit(
		&mut self,
		branch: BranchId,
		expected_head: CommitVersion,
		timestamp: CommitTimestamp,
		writes: Vec<WriteOperation>,
	) -> KernelResult<ModelCommitReceipt> {
		let current_head = self.live_branch(branch)?.head;
		if current_head != expected_head {
			return Err(KernelError::new(ErrorCode::Conflict, "branch head changed"));
		}
		if self.timeline.last().is_some_and(|(last, _)| timestamp.0 <= last.0) {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"commit timestamps must increase",
			));
		}
		if writes.is_empty() {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "empty model commit"));
		}

		let version = CommitVersion(self.global_version.0 + 1);
		let state = self.branches.get_mut(&branch).unwrap();
		for write in writes {
			let (key, value, expires_at) = match write {
				WriteOperation::Put {
					key,
					value,
					expires_at,
				} => (key, Some(value), expires_at),
				WriteOperation::Delete {
					key,
				} => (key, None, None),
			};
			state.history.entry(key).or_default().push(ModelValue {
				version,
				timestamp,
				value,
				expires_at,
			});
		}
		state.head = version;
		self.global_version = version;
		self.timeline.push((timestamp, version));
		self.current_time = self.current_time.max(timestamp);
		self.steps += 1;
		Ok(ModelCommitReceipt {
			version,
			timestamp,
			branch_head: version,
		})
	}

	pub(crate) fn fork(
		&mut self,
		source: BranchId,
		selector: ReadSelector,
		new_id: BranchId,
		name: &str,
	) -> KernelResult<BranchGeneration> {
		if self.names.contains_key(name) || self.branches.contains_key(&new_id) {
			return Err(KernelError::new(ErrorCode::AlreadyExists, "branch already exists"));
		}
		let source_state = self.live_branch(source)?.clone();
		let cap = self.resolve_selector(&source_state, selector)?;
		let generation = self
			.name_generations
			.get(name)
			.map_or(BranchGeneration(0), |generation| BranchGeneration(generation.0 + 1));
		let history = source_state
			.history
			.into_iter()
			.filter_map(|(key, values)| {
				let retained: Vec<_> =
					values.into_iter().filter(|value| value.version <= cap).collect();
				(!retained.is_empty()).then_some((key, retained))
			})
			.collect();
		self.branches.insert(
			new_id,
			ModelBranch {
				name: name.to_string(),
				generation,
				head: cap,
				deleted: false,
				history,
			},
		);
		self.names.insert(name.to_string(), new_id);
		self.name_generations.insert(name.to_string(), generation);
		self.steps += 1;
		Ok(generation)
	}

	pub(crate) fn delete_branch(&mut self, branch: BranchId) -> KernelResult<()> {
		let state = self
			.branches
			.get_mut(&branch)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))?;
		if state.name == "main" {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "main cannot be deleted"));
		}
		if state.deleted {
			return Ok(());
		}
		state.deleted = true;
		self.names.remove(&state.name);
		self.steps += 1;
		Ok(())
	}

	pub(crate) fn get(
		&self,
		branch: BranchId,
		key: &[u8],
		selector: ReadSelector,
	) -> KernelResult<Option<Bytes>> {
		let state = self.live_branch(branch)?;
		let version = self.resolve_selector(state, selector)?;
		let timestamp = match selector {
			ReadSelector::Timestamp(timestamp) => timestamp,
			ReadSelector::Latest => self.current_time,
			ReadSelector::Version(_) => self.timestamp_for_version(version),
		};
		Ok(state.history.get(key).and_then(|history| {
			history.iter().rev().find(|value| value.version <= version).and_then(|value| {
				if value.expires_at.is_some_and(|expiry| expiry.0 <= timestamp.0) {
					None
				} else {
					value.value.clone()
				}
			})
		}))
	}

	pub(crate) fn scan(
		&self,
		branch: BranchId,
		start: &[u8],
		end: &[u8],
		selector: ReadSelector,
	) -> KernelResult<Vec<(Bytes, Bytes)>> {
		if start > end {
			return Err(KernelError::new(ErrorCode::InvalidArgument, "invalid key range"));
		}
		let state = self.live_branch(branch)?;
		let mut output = Vec::new();
		for key in state.history.keys().filter(|key| key.as_ref() >= start && key.as_ref() < end) {
			if let Some(value) = self.get(branch, key, selector)? {
				output.push((key.clone(), value));
			}
		}
		Ok(output)
	}

	pub(crate) fn history(
		&self,
		branch: BranchId,
		key: &[u8],
	) -> KernelResult<Vec<(CommitVersion, CommitTimestamp, Option<Bytes>)>> {
		let state = self.live_branch(branch)?;
		Ok(state.history.get(key).map_or_else(Vec::new, |values| {
			values
				.iter()
				.rev()
				.map(|value| (value.version, value.timestamp, value.value.clone()))
				.collect()
		}))
	}

	fn live_branch(&self, branch: BranchId) -> KernelResult<&ModelBranch> {
		self.branches
			.get(&branch)
			.filter(|branch| !branch.deleted)
			.ok_or_else(|| KernelError::new(ErrorCode::NotFound, "branch not found"))
	}

	fn resolve_selector(
		&self,
		branch: &ModelBranch,
		selector: ReadSelector,
	) -> KernelResult<CommitVersion> {
		let requested = match selector {
			ReadSelector::Latest => branch.head,
			ReadSelector::Version(version) => version,
			ReadSelector::Timestamp(timestamp) => self
				.timeline
				.iter()
				.rev()
				.find(|(candidate, _)| *candidate <= timestamp)
				.map_or(CommitVersion(0), |(_, version)| *version),
		};
		if requested > self.global_version {
			return Err(KernelError::new(
				ErrorCode::InvalidArgument,
				"selector exceeds published database version",
			));
		}
		Ok(requested)
	}

	fn timestamp_for_version(&self, version: CommitVersion) -> CommitTimestamp {
		self.timeline
			.iter()
			.rev()
			.find(|(_, candidate)| *candidate <= version)
			.map_or(CommitTimestamp(0), |(timestamp, _)| *timestamp)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn catalog_delete_recreate_increments_generation_and_fences_stale_owner() {
		let main = BranchId::from_u128(1);
		let first = BranchId::from_u128(2);
		let second = BranchId::from_u128(3);
		let mut catalog = BranchCatalog::new(main);
		let original = catalog.create(first, " agent/work", CommitVersion(0));
		assert_eq!(original.unwrap_err().code, ErrorCode::InvalidArgument);
		let original = catalog.create(first, "agent/work", CommitVersion(4)).unwrap();
		assert_eq!(original.generation, BranchGeneration(0));
		assert!(catalog.delete(first).unwrap());
		assert!(!catalog.delete(first).unwrap());
		let recreated = catalog.create(second, "agent/work", CommitVersion(8)).unwrap();
		assert_eq!(recreated.generation, BranchGeneration(1));
		let stale =
			catalog.advance_head(second, original.generation, CommitVersion(8), CommitVersion(9));
		assert_eq!(stale.unwrap_err().code, ErrorCode::Fenced);
	}

	#[test]
	fn catalog_protects_default_and_lists_only_live_names() {
		let main = BranchId::from_u128(1);
		let child = BranchId::from_u128(2);
		let mut catalog = BranchCatalog::new(main);
		catalog.create(child, "child", CommitVersion(3)).unwrap();
		assert_eq!(catalog.default_branch(), main);
		assert_eq!(catalog.get_by_name("child").unwrap().id, child);
		assert_eq!(catalog.list().count(), 2);
		assert_eq!(catalog.delete(main).unwrap_err().code, ErrorCode::InvalidArgument);
		catalog.delete(child).unwrap();
		assert_eq!(catalog.list().count(), 1);
		assert_eq!(catalog.get_by_name("child").unwrap_err().code, ErrorCode::NotFound);
	}

	#[test]
	fn catalog_expected_head_transition_is_strict() {
		let main = BranchId::from_u128(1);
		let mut catalog = BranchCatalog::new(main);
		catalog
			.advance_head(main, BranchGeneration(0), CommitVersion(0), CommitVersion(2))
			.unwrap();
		assert_eq!(catalog.get(main).unwrap().head, CommitVersion(2));
		let conflict =
			catalog.advance_head(main, BranchGeneration(0), CommitVersion(0), CommitVersion(3));
		assert_eq!(conflict.unwrap_err().code, ErrorCode::Conflict);
	}

	#[test]
	fn deleted_branch_owner_is_fenced_even_before_name_recreation() {
		let main = BranchId::from_u128(1);
		let child = BranchId::from_u128(2);
		let mut catalog = BranchCatalog::new(main);
		let record = catalog.create(child, "child", CommitVersion(0)).unwrap();
		catalog.delete(child).unwrap();
		let error = catalog.validate_owner(record.id, record.generation).unwrap_err();
		assert_eq!(error.code, ErrorCode::Fenced);
	}

	#[test]
	fn owner_model_masks_older_values_with_tombstones_and_ttl() {
		let main = BranchId::from_u128(1);
		let mut model = BranchModel::new(main);
		model
			.commit(
				main,
				CommitVersion(0),
				CommitTimestamp(10),
				vec![WriteOperation::Put {
					key: Bytes::from_static(b"ttl"),
					value: Bytes::from_static(b"value"),
					expires_at: Some(CommitTimestamp(20)),
				}],
			)
			.unwrap();
		assert_eq!(
			model.get(main, b"ttl", ReadSelector::Latest).unwrap(),
			Some(Bytes::from_static(b"value"))
		);
		model.advance_time(CommitTimestamp(20)).unwrap();
		assert_eq!(model.get(main, b"ttl", ReadSelector::Latest).unwrap(), None);
		assert_eq!(
			model.get(main, b"ttl", ReadSelector::Version(CommitVersion(1))).unwrap(),
			Some(Bytes::from_static(b"value")),
			"historical version evaluates TTL at that version's commit time"
		);

		model
			.commit(
				main,
				CommitVersion(1),
				CommitTimestamp(30),
				vec![WriteOperation::Delete {
					key: Bytes::from_static(b"ttl"),
				}],
			)
			.unwrap();
		assert_eq!(model.get(main, b"ttl", ReadSelector::Latest).unwrap(), None);
		assert_eq!(model.history(main, b"ttl").unwrap().len(), 2);
	}

	#[test]
	fn owner_model_accepts_global_selector_above_sparse_branch_head() {
		let main = BranchId::from_u128(1);
		let child = BranchId::from_u128(2);
		let snapshot = BranchId::from_u128(3);
		let mut model = BranchModel::new(main);
		model
			.commit(
				main,
				CommitVersion(0),
				CommitTimestamp(10),
				vec![WriteOperation::Put {
					key: Bytes::from_static(b"a"),
					value: Bytes::from_static(b"main"),
					expires_at: None,
				}],
			)
			.unwrap();
		model.fork(main, ReadSelector::Latest, child, "child").unwrap();
		model
			.commit(
				child,
				CommitVersion(1),
				CommitTimestamp(20),
				vec![WriteOperation::Put {
					key: Bytes::from_static(b"b"),
					value: Bytes::from_static(b"child"),
					expires_at: None,
				}],
			)
			.unwrap();

		assert_eq!(
			model.get(main, b"a", ReadSelector::Version(CommitVersion(2))).unwrap(),
			Some(Bytes::from_static(b"main"))
		);
		model
			.fork(main, ReadSelector::Timestamp(CommitTimestamp(20)), snapshot, "snapshot")
			.unwrap();
		assert_eq!(model.head(snapshot).unwrap(), CommitVersion(2));
		assert_eq!(model.get(snapshot, b"b", ReadSelector::Latest).unwrap(), None);
	}
}
