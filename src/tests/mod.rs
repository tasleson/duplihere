// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! Unit tests for the duplicate detection internals.
//!
//! The tests are split by area: [`hashing`] covers turning files into hash signatures,
//! [`collision`] covers finding and cleaning up duplicate regions and [`files`] covers file
//! name bookkeeping and option handling.
//!
//! Note that `FILE_LOOKUP` is process global and the tests run in parallel, so any test that
//! touches it must use unique file names and only use the ids handed back to it.

use crate::*;
use std::sync::atomic::{AtomicU64, Ordering};

mod collision;
mod files;
mod hashing;

/// Minimum duplicate length used by most tests, small enough to keep the fixtures readable.
const MIN_LINES: u32 = 3;

/// Line signatures for three "files".  Files 0 and 1 share a two line prefix only, which is
/// shorter than MIN_LINES, while files 0 and 2 are identical.
fn test_file_hashes() -> Vec<Vec<u64>> {
    vec![vec![1, 2, 10, 11], vec![1, 2, 20, 21], vec![1, 2, 10, 11]]
}

/// The start of `file_id`, the position most collision tests care about.
fn line(file_id: u32) -> LineId {
    line_at(file_id, 0)
}

/// A specific line within `file_id`.
fn line_at(file_id: u32, line_number: u32) -> LineId {
    LineId {
        file_id,
        line_number,
    }
}

/// A file name no other test (or run) uses, so that registering it in the global `FILE_LOOKUP`
/// always yields a fresh id.
fn unique_name(tag: &str) -> Arc<String> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    Arc::new(format!(
        "/duplihere-test/{}-{}-{}",
        tag,
        process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Register a brand new name in the global `FILE_LOOKUP`, returning its id and name.  The lock
/// is released before returning so callers can safely take it again.
fn register_unique_file(tag: &str) -> (u32, Arc<String>) {
    let name = unique_name(tag);
    let id = FILE_LOOKUP
        .lock()
        .unwrap()
        .register_file(Arc::clone(&name))
        .expect("unique file name should not already be registered");
    (id, name)
}

/// Build a `Collision` the way `maximize_collision` does, i.e. with an unset signature.
fn collision(key: u64, num_lines: u32, start_lines: Vec<LineId>) -> Collision {
    Collision {
        key,
        num_lines,
        start_lines,
        sig: 0,
    }
}

/// Write `contents` to `name` inside `dir`, creating parent directories as needed, and return
/// the path it was written to.
fn write_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> std::path::PathBuf {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(&path, contents).unwrap();
    path
}

/// `&str` view of a path, for the APIs that take file names as strings.
fn path_str(path: &std::path::Path) -> &str {
    path.to_str().unwrap()
}
