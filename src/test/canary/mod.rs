//! The canary: tests that make "encrypted at rest" a claim that can fail.
//!
//! A feature that does nothing passes every test that only checks that the data comes back. These
//! look at the files. Markers that nothing else could have written are put into keys and values,
//! and every byte of every file that the database leaves is scanned for them; the same run
//! without a key manager shows that the scan would have found them, and a plaintext file planted
//! among the sealed ones shows that it fails when it should. The other half shows that a key
//! that does not fit and a file that is not what it should be are refused with the right error
//! and without a file being touched.
//!
//! - `scan` walks a directory for markers and is tested on its own.
//! - `files` checks what a scan for markers cannot (every file is a sealed kind), and takes the
//!   image of a directory, which a refusal is compared against.
//! - `matrix` takes a database through every place that its data can be, in each configuration.
//! - `migration` shows the backup that a migration leaves, and that an encrypted database refuses
//!   to leave one.
//! - `refusal` is the wrong-key and damaged-file matrix of whole databases.
//!
//! # Running them
//!
//! They are in the library's own tests, so they run in the normal lib tests and in the
//! `encryption-preview` job, on Linux and macOS, and are only built on Windows, where CI runs no
//! tests. They are gated out of the wasm build as a whole. The one test that is for Unix alone
//! is the scan that is shown not to follow a symbolic link: it is `#[cfg(unix)]` inside `scan`.
//! None is ignored and none depends on timing. The markers are drawn from a seed that is in every
//! message; `SURREALKV_CANARY_SEED` repeats a run.
//!
//! # The claims, and what proves each
//!
//! Each row is a claim of the encryption feature, the tests of this module that prove it, and the
//! older tests of the surfaces that prove its parts. Under each, what is **not claimed**, and
//! why: the claim is no wider than its tests.
//!
//! ## Encrypted at rest on every surface
//!
//! No byte of any file of an encrypted database, of its checkpoint or of the database that a
//! restore leaves, holds a key, a value, the bound of a range deletion or a key inside a value,
//! for every table compression, with the value log off, on with small values and on with large
//! ones, with eventual and immediate commits, under both suites, in the WAL alone, after a
//! rotation, after a torn tail was repaired, flushed, compacted, written straight to level zero,
//! in the value log, in a checkpoint and after a restore. Every file is of a sealed kind, and the
//! root key of the key manager is in none.
//!
//! - `matrix::no_marker_is_in_any_file_of_a_database_whose_tables_are_not_compressed`,
//!   `matrix::no_marker_is_in_any_file_of_a_database_whose_tables_are_compressed_with_snappy`,
//!   `matrix::no_marker_is_in_any_file_of_a_database_whose_tables_are_compressed_with_zstd`,
//!   `matrix::the_configurations_that_touch_every_surface_are_clean_under_the_other_suite_as_well`:
//!   the 18 configurations, each with its control that finds every marker in the same run without a
//!   key manager before the encrypted run is made.
//! - `matrix::the_canary_fails_for_a_plaintext_file_that_is_planted_in_an_encrypted_database`: the
//!   scan and the check of what each file is both fail when they should.
//! - `scan::tests::*` (a marker in each kind of place and name, split by a read boundary at every
//!   offset, in a large file, behind a link that is not followed, and a directory with none) and
//!   `files::tests::*`.
//! - `files::tests::a_key_in_a_file_is_found_and_a_key_that_is_in_none_is_not` and
//!   `scan::tests::a_key_that_is_not_text_is_found_like_a_marker_and_has_a_group_of_its_own`: the
//!   search for the root key, which every encrypted run of `matrix` and the databases of `refusal`
//!   make, finds a key where one is, and is not a search of no file.
//! - Older, of one surface each: `sstable_encryption_tests::no_marker_is_anywhere_in_a_sealed_table_but_every_one_is_in_it_when_it_is_opened`,
//!   `vlog_sealed_tree_tests::the_values_a_commit_separates_are_sealed_and_no_key_or_value_is_in_the_log`,
//!   `wal_sealed_tree_tests::no_key_and_no_value_is_in_any_byte_of_the_wal_of_a_sealed_tree`,
//!   `encryption_open_tests::a_database_created_with_a_key_manager_is_encrypted_and_opens_again_with_the_key`.
//!
//! **Not claimed.** File names, how many files there are, their sizes and when they were
//! written; where each block of a table ends and how long it is sealed, which is also what the
//! compression of a block shows; how long the key and the value of each entry of the value log
//! are together, since an entry is not padded; and what the manifest holds, which is table
//! numbers and sequence numbers and no user data even when it is not encrypted, so that the scan
//! has nothing to find in it and its sealing is shown by what kind of file it is. Not looked at
//! at all: the memtable, the block cache, swap and core dumps, the key in memory, the remains of
//! deleted files on a disk, and a copy that an operator made. The compression of the WAL
//! (LZ4) cannot be set through `Options`, so no tree reaches it; at the level of a WAL it is
//! covered by
//! `wal_sealed_tree_tests::a_compressed_segment_hides_the_markers_as_well_and_the_control_shows_them`.
//! A table that was compressed in the clear holds no marker in its raw bytes where the codec
//! codes literals (zstd): the control for it reads the blocks, and the check of what each file
//! is covers the encrypted side.
//!
//! ## One salt for each writer
//!
//! Under positional nonces a file is safe as long as no two writers seal under one key, and the key
//! of a file is made from a salt that each writer draws for itself. What can be seen of that
//! without a key is the salt in the header of each file: no two files of an encrypted database,
//! of its checkpoint or of what a restore leaves carry one salt unless one is a copy of the other,
//! or the start of it, as a checkpoint's copy of a file that has grown since is. Every observation
//! of `matrix` makes the check, which is after a repair of a torn tail (the segment that was
//! cut and the one that replaces it), a checkpoint and a restore, and a restart, none of which may
//! bring a salt back.
//!
//! - `matrix::*` (the check is part of `files::assert_every_file_is_sealed`, which every encrypted
//!   observation makes) and
//!   `files::tests::two_files_that_share_a_salt_are_copies_or_the_start_of_one_another`, which
//!   shows that the check fails for two files of one salt and different bytes.
//! - Older, for each writer:
//!   `manifest_encryption_tests::every_write_of_the_manifest_is_sealed_under_a_new_salt`,
//!   `manifest_encryption_tests::a_write_that_is_tried_again_after_a_failure_is_sealed_under_a_new_salt`,
//!   `wal_sealed_tests::a_rotation_starts_a_segment_with_a_header_and_a_salt_of_its_own`,
//!   `wal_sealed_writer_tests::a_tree_replaces_a_failed_sealed_segment_with_one_that_has_its_own_salt`,
//!   `vlog_sealed_tests::a_restart_never_reopens_a_sealed_file_and_starts_a_new_one_with_a_new_salt`,
//!   `vlog_sealed_tests::a_write_that_fails_anywhere_never_gives_a_position_out_twice`,
//!   `vlog_sealed_tests::a_writer_that_is_asked_to_seal_a_position_again_is_refused_and_poisoned`,
//!   `block_io_tests::a_sink_whose_sealer_failed_does_not_ask_it_for_that_position_again`,
//!   `tde_tests::threads_racing_through_the_same_positions_never_seal_one_twice`.
//!
//! **Not claimed.** That a position is sealed once in a file: two different messages at one
//! position of one file cannot be told from a file that was written once, without the key, so that
//! is for the cipher, which refuses a position that is not after the last, and for the tests of
//! each writer above. A salt that comes twice from a broken or a cloned random source: two files
//! that carry it and are not copies are found, which is all that can be seen.
//!
//! ## AEAD integrity
//!
//! A byte changed in any block of a table of any of the six kinds, in a record of the WAL or in
//! the body of the manifest, with every checksum made again, is `AuthenticationFailed` where it
//! is read, and never a wrong value; a block moved to another place or another file does not
//! open.
//!
//! - `refusal::a_changed_byte_in_a_block_of_each_kind_fails_authentication_where_the_block_is_read`,
//!   `refusal::a_changed_manifest_is_the_error_of_the_part_that_changed_and_no_file_changes`,
//!   `refusal::a_damaged_wal_is_never_repaired_unless_it_is_a_tear_in_the_last_segment`,
//!   `refusal::a_changed_entry_of_the_value_log_is_a_corrupt_value_and_never_a_wrong_one`.
//! - Older: `sstable_encryption_tests::a_flipped_bit_in_a_block_of_any_kind_fails_authentication_and_the_failure_reaches_the_caller`,
//!   `sstable_encryption_tests::a_block_copied_to_another_place_or_another_kind_in_its_own_file_does_not_open`,
//!   `sstable_encryption_tests::a_block_of_another_file_of_the_same_key_does_not_open_wherever_it_is_put`,
//!   `wal_sealed_recovery_tests::a_record_that_was_changed_and_whose_checksum_was_made_again_is_an_authentication_failure`,
//!   `wal_sealed_recovery_tests::a_record_that_is_sealed_at_another_index_or_in_another_segment_does_not_authenticate`,
//!   `vlog_sealed_tests::an_entry_that_was_moved_or_copied_between_positions_and_files_does_not_open`.
//!
//! **Not claimed.** That a whole file, or the whole database, is the newest there was: a file
//! replaced by an older one of the same name and database, a table swapped for another of its id,
//! a manifest rolled back. That a log has all it had: a WAL that was cut at a record boundary,
//! emptied, or whose last block was zeroed, recovers as a shorter log. At the level of a whole
//! database a changed entry of the value log is found by the checksum that the pointer holds,
//! which no one without the key can make agree, as a corrupt value (`Error::Other`), and the tag
//! of an entry is shown only where a pointer can be made to agree, in `vlog_sealed_tests`.
//!
//! ## A wrong or missing key is safe
//!
//! Another root behind the key id is `WrongKey`, no key manager is `NotConfigured`, a context of
//! another database is `DbIdMismatch`, a key the manager does not have is `UnknownKeyId` and one
//! that was retired is `KeyRetired`; a file of another database with the same key is
//! `DbIdMismatch`. In every recovery mode, with a WAL that holds acknowledged writes, the open is
//! refused with that error and every file and directory of the database, its WAL included, has
//! the name and the bytes that it had. A key that rotated needs both keys until one is retired,
//! and a decrypt-only key reads.
//!
//! - `refusal::a_key_that_does_not_fit_is_refused_with_its_error_and_no_file_changes`,
//!   `refusal::a_file_of_another_database_with_the_same_key_is_refused_for_its_database`,
//!   `refusal::a_key_that_rotated_is_needed_until_it_is_retired_and_then_the_database_is_refused`,
//!   and the cases of damage in `refusal`, each with the image of the directory before and after.
//! - Older: `wal_sealed_recovery_tests::a_key_that_does_not_fit_is_refused_in_every_mode_and_nothing_changes`,
//!   `wal_sealed_recovery_tests::a_torn_tail_is_not_repaired_by_a_wal_whose_key_does_not_fit`,
//!   `wal_sealed_recovery_tests::a_tear_is_not_repaired_when_a_record_before_it_fails_to_authenticate`,
//!   `encryption_open_tests::a_database_that_is_opened_with_the_wrong_configuration_is_refused_and_not_one_byte_changes`,
//!   `encryption_open_tests::a_database_written_under_two_keys_needs_both_and_a_key_that_was_retired_is_refused`,
//!   `vlog_sealed_tests::a_key_that_does_not_fit_is_an_error_and_never_a_reason_to_touch_a_file`.
//!
//! - A file that names another database and that the manifest does not reach is refused, kept and
//!   never deleted as an old file: a table that the manifest does not list
//!   (`orphan_cleanup_tests::a_table_of_another_database_refuses_the_open_and_nothing_is_deleted`),
//!   and a segment of the WAL below the log number, which no replay reads and which is deleted by
//!   its number (`wal_foreign_segment_tests::a_segment_of_another_database_below_the_log_number_refuses_the_open_and_changes_nothing`,
//!   `wal_foreign_segment_tests::a_manifest_of_another_database_does_not_have_the_segments_of_this_one_deleted`,
//!   `wal_foreign_segment_tests::a_segment_of_another_database_that_appears_while_the_database_is_open_is_kept`).
//!   What a crash of this database leaves, a file that is empty, zeros or cut short, is cleaned as
//!   it always was (`wal_foreign_segment_tests::what_this_database_left_behind_below_the_log_number_is_opened_over_and_deleted`).
//!
//! **Not claimed.** That the manifest is this database's: the identity of an opened database is
//! what its manifest says, so a manifest of another database that is put in place of this one's is
//! taken for the database, and what is refused is each file that names a database other than the
//! one that the manifest does. A database whose only key is decrypt-only: it is not tested. An
//! open that is not refused is not unchanged, since it makes a segment of the WAL; the cases of a
//! damaged file that the open does not reach allow that and nothing else.
//!
//! ## Plaintext files are unchanged
//!
//! With no key manager a table, a value-log file, a WAL segment and a manifest are the bytes that
//! they always were, and a database that is not encrypted is read and written as before.
//!
//! - `sstable_layout_tests::a_plaintext_table_is_what_the_reference_writer_writes`,
//!   `sstable_layout_tests::the_blocks_of_a_table_tile_the_file_up_to_the_footer`,
//!   `sstable_encryption_tests::a_table_without_a_context_is_written_as_it_always_was`,
//!   `manifest_encryption_tests::a_manifest_without_a_context_is_the_bytes_that_a_plaintext_database_always_had`,
//!   `wal_sealed_tests::a_plaintext_segment_is_framed_as_it_always_was`,
//!   `vlog_sealed_tests::a_plaintext_log_still_writes_the_bytes_it_always_did`,
//!   `vlog_sealed_tests::a_plaintext_log_is_left_as_it_was_found`. The control run of every
//!   configuration in `matrix` writes and reads a plaintext database through the same path.
//!
//! **Not claimed.** That plaintext bytes are the same as in a release: that is for the format
//! fixtures, which this series does not add.
//!
//! ## Strict mode: no mixed directory
//!
//! A database that is configured with a key refuses a plaintext file, and one that is not
//! refuses a sealed file, as a hard error and never by repair: for a table, a value-log file and
//! a WAL segment, and for a whole database of the other kind.
//!
//! - `refusal::a_file_of_the_other_kind_is_refused_in_either_direction_and_nothing_changes`,
//!   `refusal::a_key_that_does_not_fit_is_refused_with_its_error_and_no_file_changes` (a plaintext
//!   database opened with a key).
//! - Older: `encryption_open_tests::a_plaintext_database_is_not_opened_with_a_key_manager_and_not_one_byte_changes`,
//!   `sstable_encryption_tests::a_table_is_refused_by_a_database_of_the_other_kind_after_reading_only_its_footer`,
//!   `vlog_sealed_tests::a_log_reads_the_kind_of_file_it_is_configured_for_and_refuses_the_other`,
//!   `wal_sealed_recovery_tests::a_plaintext_segment_in_a_sealed_wal_is_refused_in_every_mode_and_nothing_changes`,
//!   `wal_sealed_tests::a_sealed_segment_is_refused_by_a_plaintext_reader_and_a_plaintext_wal_does_not_open_on_it`.
//!
//! ## Checkpoint and restore modes
//!
//! A checkpoint of an encrypted database holds its sealed files as they are and restores with the
//! key into the database it was taken from; one of another kind, another database or another
//! suite is refused before anything is cleared.
//!
//! - `matrix::*` (the checkpoint, the commit after it and the restore, for every configuration).
//! - `encryption_checkpoint_tests::a_checkpoint_of_an_encrypted_database_holds_the_sealed_manifest_as_it_is_and_restores_with_the_key`,
//!   `encryption_checkpoint_tests::a_checkpoint_of_another_kind_or_database_is_refused_before_anything_is_cleared`,
//!   `encryption_checkpoint_tests::a_checkpoint_of_the_same_database_in_another_suite_is_refused`,
//!   `encryption_checkpoint_tests::an_encrypted_checkpoint_is_not_restored_into_a_database_that_is_not_encrypted`,
//!   `encryption_checkpoint_tests::the_value_log_of_a_checkpoint_is_checked_from_its_first_bytes_before_anything_is_cleared`,
//!   `sstable_encryption_tree_tests::a_plain_checkpoint_is_not_restored_into_an_encrypted_database`.
//!
//! - A segment of another database, of another number, or in the clear in the WAL of a checkpoint
//!   is refused before anything is cleared
//!   (`encryption_checkpoint_wal_tests::a_segment_of_the_wal_of_a_checkpoint_that_is_not_of_this_database_is_refused_before_anything_is_cleared`).
//!
//! **Not claimed.** A restore of a checkpoint into another database or an empty directory: a
//! checkpoint of an encrypted database is a database, and is opened where it is. The tables that
//! a checkpoint holds and its manifest does not list are copied by a restore and refused by the
//! next open if they are another database's.
//!
//! ## No plaintext copy in a migration backup
//!
//! A directory that holds the files of another store is not migrated into an encrypted database:
//! the open is refused before anything is read or moved, so no backup directory exists and the
//! old data is in no file but the one it was in.
//!
//! - `migration::a_migration_leaves_a_backup_in_the_clear_and_an_encrypted_database_refuses_to`
//!   (with its control: the same directory without a key is migrated, and the backup and the new
//!   table hold the markers).
//! - Older, for the other two stores: `encryption_surfaces_tests::a_database_of_another_store_in_the_directory_of_an_encrypted_one_is_not_migrated_into_it`,
//!   `sstable_encryption_tree_tests::a_foreign_database_is_not_migrated_into_an_encrypted_one`,
//!   `encryption_open_tests::opening_an_encrypted_database_never_leaves_a_copy_of_its_data_in_the_clear`.
//!
//! **Not claimed.** Adopting a database that exists: there is no conversion, an encrypted database
//! is made when it is created. The converter is a later item.
//!
//! ## Not part of these claims
//!
//! The scrubber and any I/O backend are not here. The fail-closed guard is not removed by this
//! module: `tests/encryption_guard.rs` is what says that it is there until the last commit of the
//! series.

mod files;
mod matrix;
mod migration;
mod refusal;
mod scan;
