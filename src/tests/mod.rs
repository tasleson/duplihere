// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! Unit tests for the duplicate detection internals.

use super::*;

const MIN_LINES: u32 = 3;

/// Line signatures for three "files".  Files 0 and 1 share a two line prefix only, which is
/// shorter than MIN_LINES, while files 0 and 2 are identical.
fn test_file_hashes() -> Vec<Vec<u64>> {
    vec![vec![1, 2, 10, 11], vec![1, 2, 20, 21], vec![1, 2, 10, 11]]
}

fn line(file_id: u32) -> LineId {
    LineId {
        file_id,
        line_number: 0,
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
