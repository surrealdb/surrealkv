use std::{
    cmp::Ordering, collections::btree_map, iter::Peekable, marker::PhantomData, ops::RangeBounds,
};

use bytes::Bytes;
use vart::VariableSizeKey;

use crate::{
    indexer::IndexValue,
    store::Core,
    transaction::{ReadSet, ReadSetEntry, ScanResult, WriteSet, WriteSetEntry},
    util::convert_range_bounds_bytes,
    Result,
};

/// An iterator over the snapshot and write set.
/// This iterator is used to perform a merging scan over the snapshot and write set.
/// The iterator will return the values in the snapshot and write set in the order of
/// their keys.
/// If a key is present in both the snapshot and the write set, the value from the write
/// set will be returned.
/// If a key is present in the snapshot but not in the write set, the value from the snapshot
/// will be returned.
/// If a key is present in the write set but not in the snapshot, the value from the write set
/// will be returned.
/// The iterator will add the keys that are read from the snapshot to the read set.
pub(crate) struct MergingScanIterator<'a, R, I: Iterator> {
    core: &'a Core,
    read_set: Option<&'a mut ReadSet>,
    savepoints: u32,
    snap_iter: Peekable<I>,
    write_set_iter: Peekable<btree_map::Range<'a, Bytes, Vec<WriteSetEntry>>>,
    limit: usize,
    count: usize,
    _phantom: PhantomData<R>,
}

impl<'a, R, I: Iterator> MergingScanIterator<'a, R, I>
where
    R: RangeBounds<VariableSizeKey>,
    I: Iterator<Item = (&'a [u8], &'a IndexValue, u64, u64)>,
{
    pub(crate) fn new(
        core: &'a Core,
        write_set: &'a WriteSet,
        read_set: Option<&'a mut ReadSet>,
        savepoints: u32,
        snap_iter: I,
        range: &R,
        limit: Option<usize>,
    ) -> Self {
        let range_bytes = convert_range_bounds_bytes(range);
        MergingScanIterator::<R, I> {
            core,
            read_set,
            savepoints,
            snap_iter: snap_iter.peekable(),
            write_set_iter: write_set.range(range_bytes).peekable(),
            limit: limit.unwrap_or(usize::MAX),
            count: 0,
            _phantom: PhantomData,
        }
    }

    fn add_to_read_set(read_set: &mut ReadSet, key: &[u8], version: u64, savepoints: u32) {
        let entry = ReadSetEntry::new(key, version, savepoints);
        read_set.push(entry);
    }
}

impl<'a, R, I> Iterator for MergingScanIterator<'a, R, I>
where
    R: RangeBounds<VariableSizeKey>,
    I: Iterator<Item = (&'a [u8], &'a IndexValue, u64, u64)>,
{
    type Item = Result<ScanResult<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.limit {
            return None;
        }

        // Fast path when write set is empty
        if self.write_set_iter.peek().is_none() {
            // If the write set does not contain values in the scan range,
            // do the scan only in the snapshot. This optimisation is quite
            // important according to the benches.
            let result = self.read_from_snapshot();
            if result.is_some() {
                self.count += 1;
            }
            return result;
        }

        // Merging path
        // If both the write set and the snapshot contain values from the requested
        // range, perform a somewhat slower merging scan.

        // Determine which iterator has the next value
        let has_snap = self.snap_iter.peek().is_some();
        let has_ws = self.write_set_iter.peek().is_some();

        let result = match (has_snap, has_ws) {
            (false, false) => None,
            (true, false) => self.read_from_snapshot(),
            (false, true) => self.read_from_write_set(),
            (true, true) => {
                // Now we can safely do the comparison
                if let (Some((snap_key, _, _, _)), Some((ws_key, _))) =
                    (self.snap_iter.peek(), self.write_set_iter.peek())
                {
                    match snap_key.as_ref().cmp(ws_key.as_ref()) {
                        Ordering::Less => self.read_from_snapshot(),
                        Ordering::Greater => self.read_from_write_set(),
                        Ordering::Equal => {
                            self.snap_iter.next(); // Skip snapshot entry
                            self.read_from_write_set()
                        }
                    }
                } else {
                    // This should never happen since we checked above
                    None
                }
            }
        };

        if result.is_some() {
            self.count += 1;
        }
        result
    }
}

impl<'a, R, I> MergingScanIterator<'a, R, I>
where
    R: RangeBounds<VariableSizeKey>,
    I: Iterator<Item = (&'a [u8], &'a IndexValue, u64, u64)>,
{
    fn read_from_snapshot(&mut self) -> Option<<Self as Iterator>::Item> {
        self.snap_iter.next().map(|(key, value, version, ts)| {
            if let Some(read_set) = self.read_set.as_mut() {
                Self::add_to_read_set(read_set, key, version, self.savepoints);
            }
            match value.resolve(self.core) {
                Ok(v) => Ok((key, v, ts)),
                Err(e) => Err(e),
            }
        })
    }

    fn read_from_write_set(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some((ws_key, ws_entries)) = self.write_set_iter.next() {
            if let Some(ws_entry) = ws_entries.last() {
                if ws_entry.e.is_deleted_or_tombstone() {
                    return self.next();
                }
                return Some(Ok((
                    ws_key.as_ref(),
                    ws_entry.e.value.to_vec(),
                    ws_entry.e.ts,
                )));
            }
        }
        None
    }
}

/// An iterator over the keys in the snapshot and write set.
/// It does not add anything to the read set.
pub(crate) struct KeyScanIterator<'a, R, I: Iterator> {
    snap_iter: Peekable<I>,
    write_set_iter: Peekable<btree_map::Range<'a, Bytes, Vec<WriteSetEntry>>>,
    limit: Option<usize>,
    count: usize,
    _phantom: PhantomData<R>,
}

impl<'a, R, I: Iterator> KeyScanIterator<'a, R, I>
where
    R: RangeBounds<VariableSizeKey>,
    I: Iterator<Item = (&'a [u8], &'a IndexValue, u64, u64)>,
{
    pub(crate) fn new(
        write_set: &'a WriteSet,
        snap_iter: I,
        range: &R,
        limit: Option<usize>,
    ) -> Self {
        let range_bytes = convert_range_bounds_bytes(range);
        KeyScanIterator {
            snap_iter: snap_iter.peekable(),
            write_set_iter: write_set.range(range_bytes).peekable(),
            limit,
            count: 0,
            _phantom: PhantomData,
        }
    }

    fn read_from_snapshot(&mut self) -> Option<&'a [u8]> {
        self.snap_iter.next().map(|(key, ..)| key)
    }

    fn read_from_write_set(&mut self) -> Option<&'a [u8]> {
        if let Some((ws_key, ws_entries)) = self.write_set_iter.next() {
            if let Some(ws_entry) = ws_entries.last() {
                if ws_entry.e.is_deleted_or_tombstone() {
                    return self.next();
                }
                return Some(ws_key.as_ref());
            }
        }
        None
    }
}

impl<'a, R, I> Iterator for KeyScanIterator<'a, R, I>
where
    R: RangeBounds<VariableSizeKey>,
    I: Iterator<Item = (&'a [u8], &'a IndexValue, u64, u64)>,
{
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(limit) = self.limit {
            if self.count >= limit {
                return None;
            }
        }

        // Fast path when write set is empty
        if self.write_set_iter.peek().is_none() {
            let result = self.read_from_snapshot();
            if result.is_some() {
                self.count += 1;
            }
            return result;
        }

        // Determine which iterator has the next value
        let has_snap = self.snap_iter.peek().is_some();
        let has_ws = self.write_set_iter.peek().is_some();

        let result = match (has_snap, has_ws) {
            (false, false) => None,
            (true, false) => self.read_from_snapshot(),
            (false, true) => self.read_from_write_set(),
            (true, true) => {
                if let (Some((snap_key, _, _, _)), Some((ws_key, _))) =
                    (self.snap_iter.peek(), self.write_set_iter.peek())
                {
                    match snap_key.as_ref().cmp(ws_key.as_ref()) {
                        Ordering::Less => self.read_from_snapshot(),
                        Ordering::Greater => self.read_from_write_set(),
                        Ordering::Equal => {
                            self.snap_iter.next(); // Skip snapshot entry
                            self.read_from_write_set()
                        }
                    }
                } else {
                    None
                }
            }
        };

        if result.is_some() {
            self.count += 1;
        }
        result
    }
}
