// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! Tests for file name bookkeeping, file discovery and command line defaults.

use super::{path_str, write_file};
use crate::*;
use tempfile::tempdir;

/// The canonical, absolute form of a path as `files_to_process` reports it.
fn canonical(path: &std::path::Path) -> String {
    canonicalize(path).unwrap().to_str().unwrap().to_string()
}

/// Sorted canonical names out of `files_to_process`, which returns them in glob order.
fn sorted_names(found: &[(u32, Arc<String>)]) -> Vec<String> {
    let mut names: Vec<String> = found.iter().map(|(_, name)| (**name).clone()).collect();
    names.sort();
    names
}

/// Ids are handed out in registration order, a name is only ever registered once and the id maps
/// back to the name it was registered with.
#[test]
fn file_id_hands_out_sequential_ids_once_per_name() {
    const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];
    let mut lookup = FileId::new();

    for (expected_id, name) in NAMES.iter().enumerate() {
        let id = lookup.register_file(Arc::new(name.to_string()));
        assert_eq!(id, Some(expected_id as u32), "registering {}", name);
    }
    assert_eq!(lookup.number_files(), NAMES.len() as u32);

    for (id, name) in NAMES.iter().enumerate() {
        assert_eq!(*lookup.id_to_name(id as u32), *name);
        assert_eq!(
            lookup.register_file(Arc::new(name.to_string())),
            None,
            "{} should already be registered",
            name
        );
    }
    assert_eq!(lookup.number_files(), NAMES.len() as u32);
}

/// Globs may match directories and may overlap with each other; only real files come back, and
/// each of them exactly once.
#[test]
fn files_to_process_skips_directories_and_repeats() {
    let dir = tempdir().unwrap();
    let top = write_file(dir.path(), "a.txt", b"a\n");
    let nested = write_file(dir.path(), "sub/c.txt", b"c\n");
    write_file(dir.path(), "sub/ignored.md", b"not matched\n");
    // A directory whose name matches the glob.
    std::fs::create_dir(dir.path().join("notafile.txt")).unwrap();

    let globs = vec![
        format!("{}/**/*.txt", path_str(dir.path())),
        // Deliberately matches a.txt a second time.
        format!("{}/a.txt", path_str(dir.path())),
    ];
    let found = files_to_process(&globs, &[]);

    assert_eq!(
        sorted_names(&found),
        vec![canonical(&top), canonical(&nested)]
    );
}

/// Anything underneath an excluded directory is dropped, including nested files.
#[test]
fn files_to_process_filters_excluded_directories() {
    let dir = tempdir().unwrap();
    let keep = write_file(dir.path(), "keep/a.txt", b"a\n");
    write_file(dir.path(), "skip/b.txt", b"b\n");
    write_file(dir.path(), "skip/deeper/c.txt", b"c\n");

    let globs = vec![format!("{}/**/*.txt", path_str(dir.path()))];
    let excludes = vec![path_str(&dir.path().join("skip")).to_string()];
    let found = files_to_process(&globs, &excludes);

    assert_eq!(sorted_names(&found), vec![canonical(&keep)]);
}

/// Blank lines, `#` comments and anything that isn't a u64 are skipped; everything else is
/// available to suppress a result.  (Only the invalid value warns, but stderr isn't captured.)
#[test]
fn get_ignore_hashes_keeps_only_valid_hashes() {
    const VALID: [u64; 3] = [42, 18446744073709551615, 0];
    let dir = tempdir().unwrap();
    let file = write_file(
        dir.path(),
        "ignores.txt",
        b"42\n\n# a comment\n   \n  18446744073709551615  \nnot-a-hash\n0\n",
    );

    let ignores = get_ignore_hashes(path_str(&file));

    assert_eq!(ignores.len(), VALID.len());
    for hash in VALID {
        assert!(ignores.contains_key(&hash), "missing hash {}", hash);
    }
}

/// The documented defaults used when the user supplies no options.
#[test]
fn options_defaults_match_the_documented_values() {
    let opts = Options::default();

    assert_eq!(opts.lines, 6);
    assert_eq!(opts.threads, 4);
    assert!(!opts.print);
    assert!(!opts.json);
    assert!(!opts.version);
    assert!(opts.file_globs.is_empty());
    assert!(opts.exclude_dirs.is_empty());
    assert!(opts.ignore.is_empty());
}
