// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! Tests for turning files into line and rolling hash signatures.

use super::{path_str, write_file};
use crate::*;
use tempfile::tempdir;

/// Hashing is a pure function of the input: equal input hashes equal, different input doesn't.
#[test]
fn calculate_hash_is_deterministic_per_value() {
    let cases: [(&str, &str, bool); 4] = [
        ("same text", "same text", true),
        ("", "", true),
        ("text", "texu", false),
        ("text", " text", false),
    ];

    for (left, right, expected_equal) in cases {
        assert_eq!(
            calculate_hash(left) == calculate_hash(right),
            expected_equal,
            "unexpected hash comparison for {:?} and {:?}",
            left,
            right
        );
    }
}

/// Leading and trailing whitespace is stripped before hashing, so the same text at different
/// levels of indentation produces the same signature.  That is the whole point of the tool.
#[test]
fn file_signatures_ignore_surrounding_whitespace() {
    let dir = tempdir().unwrap();
    let file = write_file(
        dir.path(),
        "indented.txt",
        b"let x = 1;\n        let x = 1;\n\tlet x = 1;   \nlet y = 2;\n",
    );

    let sigs = file_signatures(path_str(&file));

    assert_eq!(sigs.len(), 4);
    assert_eq!(sigs[0], sigs[1]);
    assert_eq!(sigs[0], sigs[2]);
    assert_ne!(sigs[0], sigs[3]);
    assert_eq!(sigs[0], calculate_hash("let x = 1;"));
}

/// Blank lines still count as lines, and a final line without a trailing newline is not lost.
#[test]
fn file_signatures_keeps_blank_and_unterminated_lines() {
    let dir = tempdir().unwrap();
    let file = write_file(dir.path(), "ragged.txt", b"alpha\n\n   \nomega");

    let sigs = file_signatures(path_str(&file));

    let blank = calculate_hash("");
    assert_eq!(
        sigs,
        vec![
            calculate_hash("alpha"),
            blank,
            blank,
            calculate_hash("omega")
        ]
    );
}

/// A file we cannot open is reported on stderr and yields no signatures rather than panicking.
#[test]
fn file_signatures_missing_file_is_empty() {
    let dir = tempdir().unwrap();
    let missing = dir.path().join("does-not-exist.txt");

    assert!(file_signatures(path_str(&missing)).is_empty());
}

/// Binary garbage in an otherwise text file is decoded lossily instead of panicking.
#[test]
fn file_signatures_handles_invalid_utf8() {
    let dir = tempdir().unwrap();
    let file = write_file(dir.path(), "binary.txt", b"a\xFFb\nplain\n");

    let sigs = file_signatures(path_str(&file));

    assert_eq!(sigs.len(), 2);
    assert_eq!(
        sigs[0],
        calculate_hash(String::from_utf8_lossy(b"a\xFFb").trim())
    );
    assert_eq!(sigs[1], calculate_hash("plain"));
}

/// One window per possible start position, each labelled with the line it starts on.
#[test]
fn rolling_hashes_reports_one_window_per_start_line() {
    let signatures = [1, 2, 3, 4, 5];

    let windows = rolling_hashes(&signatures, 2);

    let line_numbers: Vec<u32> = windows.iter().map(|(_, line)| *line).collect();
    assert_eq!(line_numbers, vec![0, 1, 2, 3]);
}

/// Back to back identical windows are collapsed to the first one (a run of identical lines would
/// otherwise report a duplicate at every offset), but a window that repeats later is kept.
#[test]
fn rolling_hashes_collapses_only_consecutive_duplicates() {
    // Windows: (1,1)@0, (1,1)@1 [collapsed], (1,2)@2
    let consecutive = rolling_hashes(&[1, 1, 1, 2], 2);
    assert_eq!(
        consecutive.iter().map(|(_, l)| *l).collect::<Vec<_>>(),
        vec![0, 2]
    );

    // Windows: (1,2)@0, (2,9)@1, (9,1)@2, (1,2)@3 -- the repeat of (1,2) is not consecutive.
    let separated = rolling_hashes(&[1, 2, 9, 1, 2], 2);
    assert_eq!(
        separated.iter().map(|(_, l)| *l).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(separated[0].0, separated[3].0);
}

/// A file shorter than the requested duplicate length cannot contain one.
#[test]
fn rolling_hashes_of_too_short_file_is_empty() {
    assert!(rolling_hashes(&[1, 2, 3], 4).is_empty());
}

/// Processing a file records its line signatures in the shared vector and registers a bucket for
/// each distinct rolling hash, and two identical files share every bucket.
#[test]
fn process_file_registers_line_hashes_and_buckets() {
    const WINDOW_LINES: usize = 2;
    const LINES: &[u8] = b"one\ntwo\nthree\nfour\n";
    let dir = tempdir().unwrap();
    let first = write_file(dir.path(), "first.txt", LINES);
    let second = write_file(dir.path(), "second.txt", LINES);

    let file_hashes: Mutex<Vec<Vec<u64>>> = Mutex::new(vec![vec![]; 2]);
    let collision_hashes: DashMap<u64, Vec<LineId>> = DashMap::new();

    process_file(
        0,
        path_str(&first),
        WINDOW_LINES,
        &file_hashes,
        &collision_hashes,
    );

    let expected_signatures = file_signatures(path_str(&first));
    let expected_buckets = rolling_hashes(&expected_signatures, WINDOW_LINES).len();
    assert_eq!(expected_buckets, 3);
    assert_eq!(file_hashes.lock().unwrap()[0], expected_signatures);
    assert_eq!(collision_hashes.len(), expected_buckets);

    process_file(
        1,
        path_str(&second),
        WINDOW_LINES,
        &file_hashes,
        &collision_hashes,
    );

    assert_eq!(file_hashes.lock().unwrap()[1], expected_signatures);
    assert_eq!(
        collision_hashes.len(),
        expected_buckets,
        "identical files must not create new buckets"
    );
    for bucket in collision_hashes.iter() {
        let mut ids: Vec<u32> = bucket.value().iter().map(|l| l.file_id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![0, 1]);
    }
}
