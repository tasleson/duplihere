// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! Tests for locating duplicate regions and tidying up the results.

use super::{collision, line, line_at, register_unique_file, test_file_hashes, MIN_LINES};
use crate::*;

/// Two regions overlap only when they are in the same file and within `end` lines of each other.
#[test]
fn overlap_only_within_the_same_file_and_range() {
    const END: u32 = 3;
    let cases: [(&str, LineId, LineId, bool); 7] = [
        ("identical line", line_at(0, 5), line_at(0, 5), true),
        ("right inside range", line_at(0, 5), line_at(0, 8), true),
        ("right just past range", line_at(0, 5), line_at(0, 9), false),
        ("left inside range", line_at(0, 8), line_at(0, 5), true),
        ("left just past range", line_at(0, 9), line_at(0, 5), false),
        (
            "different files, same line",
            line_at(0, 5),
            line_at(1, 5),
            false,
        ),
        (
            "different files, in range",
            line_at(0, 5),
            line_at(1, 6),
            false,
        ),
    ];

    for (name, left, right, expected) in cases {
        assert_eq!(overlap(&left, &right, END), expected, "case: {}", name);
    }
}

/// A rolling hash bucket collision hands maximize_collision two line ranges that don't really
/// match, or match for fewer lines than requested.  Ensure we only report at least min_lines.
#[test]
fn maximize_collision_honors_min_lines() {
    let file_hashes = test_file_hashes();

    let cases: [(u32, u32, Option<u32>); 3] = [
        // Only two matching lines, fewer than min_lines, nothing to report.
        (0, 1, None),
        // Fully matching files, report all four lines.
        (0, 2, Some(4)),
        // Same file, overlapping with itself.
        (0, 0, None),
    ];

    for (left, right, expected) in cases {
        let result = maximize_collision(&file_hashes, &line(left), &line(right), MIN_LINES);
        assert_eq!(
            result.map(|c| c.num_lines),
            expected,
            "unexpected result for files {} and {}",
            left,
            right
        );
    }
}

/// One `maximize_collision` scenario: name, per file line signatures, both start points and the
/// number of duplicate lines we expect to be reported.
type WalkCase = (&'static str, Vec<Vec<u64>>, LineId, LineId, Option<u32>);

/// The walk starts wherever the bucket pointed us, extends while the lines keep matching and
/// stops at the end of either file.  A match of exactly min_lines is still a match.
#[test]
fn maximize_collision_walks_until_the_lines_stop_matching() {
    let cases: [WalkCase; 5] = [
        (
            "match starting part way into a file",
            vec![vec![9, 9, 1, 2, 3, 4], vec![1, 2, 3, 4]],
            line_at(0, 2),
            line_at(1, 0),
            Some(4),
        ),
        (
            "walk stops at the end of the shorter file",
            vec![vec![1, 2, 3, 4, 5], vec![1, 2, 3, 4]],
            line_at(0, 0),
            line_at(1, 0),
            Some(4),
        ),
        (
            "exactly min_lines is reported",
            vec![vec![1, 2, 3, 7], vec![1, 2, 3, 8]],
            line_at(0, 0),
            line_at(1, 0),
            Some(MIN_LINES),
        ),
        (
            "one line short of min_lines is dropped",
            vec![vec![1, 2, 7, 8], vec![1, 2, 9, 9]],
            line_at(0, 0),
            line_at(1, 0),
            None,
        ),
        (
            // The starts are min_lines apart so the walk is allowed to begin, but it grows long
            // enough to run into the other copy: the same text matching itself, nothing to report.
            "walk grows until the two copies in one file overlap",
            vec![vec![1, 2, 3, 4, 1, 2, 3, 4]],
            line_at(0, 0),
            line_at(0, 4),
            None,
        ),
    ];

    for (name, file_hashes, left, right, expected) in cases {
        let result = maximize_collision(&file_hashes, &left, &right, MIN_LINES);
        assert_eq!(result.map(|c| c.num_lines), expected, "case: {}", name);
    }
}

/// walk_collision is handed the contents of one rolling hash bucket.  Two entries land in the
/// same bucket when their digests match, which normally means the text matches, but can also
/// happen by chance.  Nothing between here and the printed report re-checks the length of a
/// match, so a bucket holding text that doesn't really match must produce no result at all.
#[test]
fn walk_collision_drops_short_bucket_collisions() {
    let file_hashes = test_file_hashes();

    // Files 0 and 1 only match for two lines, fewer than MIN_LINES: report nothing.
    let short = DashMap::new();
    walk_collision(&[line(0), line(1)], &file_hashes, MIN_LINES, &short);
    assert!(
        short.is_empty(),
        "reported a duplicate shorter than min_lines: {:?}",
        short.iter().map(|e| e.num_lines).collect::<Vec<_>>()
    );

    // Control: files 0 and 2 are identical, so the real duplicate is still reported in full.
    let genuine = DashMap::new();
    walk_collision(&[line(0), line(2)], &file_hashes, MIN_LINES, &genuine);
    let reported: Vec<u32> = genuine.iter().map(|e| e.num_lines).collect();
    assert_eq!(reported, vec![4]);
}

/// Every pair of matching regions in a bucket hashes to the same key, so the three way duplicate
/// becomes a single result whose start lines accumulate one pair at a time.
#[test]
fn walk_collision_merges_every_pair_into_one_result() {
    let file_hashes = vec![vec![1, 2, 3, 4], vec![1, 2, 3, 4], vec![1, 2, 3, 4]];
    let results = DashMap::new();

    walk_collision(
        &[line(0), line(1), line(2)],
        &file_hashes,
        MIN_LINES,
        &results,
    );

    assert_eq!(results.len(), 1);
    let entry = results.iter().next().unwrap();
    assert_eq!(entry.num_lines, 4);
    // Pairs (0,1), (0,2) and (1,2), each contributing both of its start lines.
    assert_eq!(
        entry.start_lines,
        vec![line(0), line(1), line(0), line(2), line(1), line(2)]
    );
}

/// A genuine cross file duplicate survives the whole pipeline, while a bucket that only ever saw
/// a single region contributes nothing (it is also dropped up front to save memory).
#[test]
fn find_collisions_drops_lone_entries_and_keeps_duplicates() {
    let mut file_hashes = vec![vec![1, 2, 3, 4], vec![1, 2, 3, 4]];
    let collision_hash: DashMap<u64, Vec<LineId>> = DashMap::new();
    collision_hash.insert(0xA, vec![line(0)]);
    collision_hash.insert(0xB, vec![line(0), line(1)]);

    let opts = Options {
        lines: MIN_LINES,
        ..Options::default()
    };
    let results = find_collisions(collision_hash, &mut file_hashes, &opts);

    assert_eq!(results.len(), 1);
    let entry = results.iter().next().unwrap();
    assert_eq!(entry.num_lines, 4);
    assert_eq!(entry.start_lines, vec![line(0), line(1)]);
}

/// Within one file, start lines closer together than the duplicate length describe the same
/// repeating text, so they collapse.  Start lines in different files are always kept.
#[test]
fn remove_overlap_same_file_collapses_only_single_file_runs() {
    const NUM_LINES: u32 = 5;
    let cases: [(&str, Vec<LineId>, Vec<LineId>); 4] = [
        (
            "overlapping starts in one file collapse",
            vec![line_at(0, 0), line_at(0, 2)],
            vec![line_at(0, 0)],
        ),
        (
            "starts exactly num_lines apart still collapse",
            vec![line_at(0, 0), line_at(0, NUM_LINES)],
            vec![line_at(0, 0)],
        ),
        (
            "starts one past num_lines are kept",
            vec![line_at(0, 0), line_at(0, NUM_LINES + 1)],
            vec![line_at(0, 0), line_at(0, NUM_LINES + 1)],
        ),
        (
            "different files are left alone",
            vec![line_at(0, 0), line_at(1, 2)],
            vec![line_at(0, 0), line_at(1, 2)],
        ),
    ];

    for (name, start_lines, expected) in cases {
        let mut c = collision(1, NUM_LINES, start_lines);
        c.remove_overlap_same_file();
        assert_eq!(c.start_lines, expected, "case: {}", name);
    }
}

/// Scrubbing sorts, removes exact duplicates, collapses same file overlaps and then computes a
/// signature that only depends on the surviving start lines and length.
#[test]
fn scrub_dedups_and_computes_a_stable_signature() {
    const NUM_LINES: u32 = 4;
    let mut c = collision(
        1,
        NUM_LINES,
        vec![line_at(1, 7), line_at(0, 3), line_at(1, 7), line_at(0, 3)],
    );
    c.scrub();
    assert_eq!(c.start_lines, vec![line_at(0, 3), line_at(1, 7)]);

    // Equal content, different insertion order and duplicates: same signature.
    let mut same = collision(99, NUM_LINES, vec![line_at(1, 7), line_at(0, 3)]);
    same.scrub();
    assert_eq!(same.signature(), c.signature());

    // A different length describes different text, so the signature must differ.
    let mut longer = collision(1, NUM_LINES + 1, vec![line_at(0, 3), line_at(1, 7)]);
    longer.scrub();
    assert_ne!(longer.signature(), c.signature());

    // Same lines in a different file are a different duplicate, so they must not share a
    // signature: otherwise process_report would drop one of them as already reported.
    let mut elsewhere = collision(1, NUM_LINES, vec![line_at(0, 3), line_at(2, 7)]);
    elsewhere.scrub();
    assert_ne!(elsewhere.signature(), c.signature());

    // Scrubbing also collapses overlapping starts within one file, not just exact duplicates.
    let mut overlapping = collision(1, NUM_LINES, vec![line_at(0, 2), line_at(0, 0)]);
    overlapping.scrub();
    assert_eq!(overlapping.start_lines, vec![line_at(0, 0)]);
}

/// The JSON form resolves file ids back to names through the global lookup.
#[test]
fn collision_serializes_with_resolved_file_names() {
    const NUM_LINES: u32 = 4;
    const KEY: u64 = 0x1234;
    let (first_id, first_name) = register_unique_file("serialize");
    let (second_id, second_name) = register_unique_file("serialize");

    let c = collision(
        KEY,
        NUM_LINES,
        vec![line_at(first_id, 0), line_at(second_id, 10)],
    );

    let json: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&c).unwrap()).unwrap();

    assert_eq!(json["key"], serde_json::json!(KEY));
    assert_eq!(json["num_lines"], serde_json::json!(NUM_LINES));
    assert_eq!(
        json["files"],
        serde_json::json!([[*first_name, 0], [*second_name, 10]])
    );
}
