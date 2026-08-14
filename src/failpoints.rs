//! Injected failure at the engine's durable steps.
//!
//! Every branch operation ends in a publish, and every one has a rollback path
//! for that publish failing. Those paths were written and never executed: no
//! test could reach them, because nothing could make a publish fail.
//!
//! # Why this is injected rather than global
//!
//! The obvious implementation is a process-wide (or thread-local) registry that
//! production code consults through a `#[cfg(test)]` macro. That is what
//! `fail-rs` does, and it has three costs this engine does not need to pay:
//! hidden mutable state that any code can reach, stringly-typed point names that
//! typo silently, and — worst — a production build that compiles *different
//! code* from the one the tests exercise.
//!
//! So the policy is a dependency, carried on [`crate::Options`] exactly as the
//! logical clock is. The engine consults it through a plain method call at the
//! same places in every build; only the *policy* differs. Tests get the real
//! code path, and there is no global anywhere.
//!
//! The production policy is [`NoFaults`], which is zero-sized and returns `Ok`.
//! The dynamic call sits next to an fsync, so its cost is not measurable.

use crate::error::Result;

/// A durable step the engine can be made to fail at.
///
/// An enum rather than a name: the set is small, closed, and worth being
/// exhaustive over.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FaultPoint {
	/// The catalog publish that commits every branch operation: create, fork,
	/// delete, detach, TTL expiry, and the merge promotion edge.
	CatalogPublish,
	/// One owner's level-set state publish — the tail of a flush or compaction.
	OwnerStatePublish,
	/// The root publish that names every owner's newest state version.
	RootPublish,
}

/// Consulted at each [`FaultPoint`]. Returning an error makes that step fail as
/// if the underlying IO had.
pub(crate) trait FaultPolicy: std::fmt::Debug + Send + Sync {
	fn check(&self, point: FaultPoint) -> Result<()>;
}

/// The policy every store gets unless a test replaces it: nothing ever fails.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct NoFaults;

impl FaultPolicy for NoFaults {
	#[inline]
	fn check(&self, _point: FaultPoint) -> Result<()> {
		Ok(())
	}
}

#[cfg(test)]
pub(crate) use scripted::ScriptedFaults;

#[cfg(test)]
mod scripted {
	use std::collections::HashMap;
	use std::sync::Mutex;

	use super::{FaultPoint, FaultPolicy};
	use crate::error::{Error, Result};

	/// A policy a test scripts in advance: each point fails a stated number of
	/// times, then stops.
	///
	/// State lives in the instance, so two stores in two parallel tests cannot
	/// see each other's script — which is the practical reason a global registry
	/// would have needed a thread-local, and the reason this one does not.
	#[derive(Debug, Default)]
	pub(crate) struct ScriptedFaults {
		/// Remaining failures per point. `None` means "until the store is
		/// dropped".
		remaining: Mutex<HashMap<FaultPoint, Option<usize>>>,
	}

	impl ScriptedFaults {
		pub(crate) fn new() -> Self {
			Self::default()
		}

		/// Fails the next `times` calls at `point`.
		pub(crate) fn fail_times(&self, point: FaultPoint, times: usize) {
			self.remaining.lock().unwrap().insert(point, Some(times));
		}

		/// Fails every call at `point` until [`ScriptedFaults::clear`].
		pub(crate) fn fail_always(&self, point: FaultPoint) {
			self.remaining.lock().unwrap().insert(point, None);
		}

		pub(crate) fn clear(&self, point: FaultPoint) {
			self.remaining.lock().unwrap().remove(&point);
		}

		/// Whether `point` still has failures scripted. Lets a test assert its
		/// fault was actually consumed rather than assuming the path was reached.
		pub(crate) fn is_armed(&self, point: FaultPoint) -> bool {
			self.remaining.lock().unwrap().contains_key(&point)
		}
	}

	impl FaultPolicy for ScriptedFaults {
		fn check(&self, point: FaultPoint) -> Result<()> {
			let mut remaining = self.remaining.lock().unwrap();
			let Some(count) = remaining.get_mut(&point) else {
				return Ok(());
			};
			if let Some(count) = count {
				*count -= 1;
				if *count == 0 {
					remaining.remove(&point);
				}
			}
			Err(Error::Io(std::sync::Arc::new(std::io::Error::other(format!(
				"injected failure at {point:?}"
			)))))
		}
	}
}
