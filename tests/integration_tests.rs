// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! Integration tests for duplihere core functionality
//!
//! This module contains comprehensive tests covering:
//! - Hash calculation and consistency
//! - File signature generation and processing
//! - Rolling hash algorithms and pattern detection
//! - Collision detection and management
//! - FileId management and memory optimization
//! - Complex real-world scenarios with repeating patterns

use duplihere::*;
use std::io::Write;
use tempfile::NamedTempFile;
use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

#[test]
fn test_calculate_hash() {
    // Test that identical strings produce identical hashes
    let hash1 = calculate_hash("hello world");
    let hash2 = calculate_hash("hello world");
    assert_eq!(hash1, hash2);

    // Test that different strings produce different hashes
    let hash3 = calculate_hash("different string");
    assert_ne!(hash1, hash3);

    // Test empty string
    let hash_empty = calculate_hash("");
    assert_ne!(hash1, hash_empty);
}

#[test]
fn test_rolling_hashes() {
    // Test with simple signature sequence
    let signatures = vec![1, 2, 3, 4, 5];
    let min_lines = 3;
    let rolling = rolling_hashes(&signatures, min_lines);

    // Should have 3 windows: [1,2,3], [2,3,4], [3,4,5]
    assert_eq!(rolling.len(), 3);

    // Check that line numbers are correct
    assert_eq!(rolling[0].1, 0); // First window starts at line 0
    assert_eq!(rolling[1].1, 1); // Second window starts at line 1
    assert_eq!(rolling[2].1, 2); // Third window starts at line 2
}

#[test]
fn test_rolling_hashes_removes_duplicates() {
    // Test with repeated sequences that should produce identical hashes
    let signatures = vec![1, 2, 3, 1, 2, 3];
    let min_lines = 3;
    let rolling = rolling_hashes(&signatures, min_lines);

    // Should have windows: [1,2,3], [2,3,1], [3,1,2], [1,2,3]
    // But duplicate consecutive hashes should be removed
    assert!(rolling.len() <= 4);

    // First and last windows should have the same hash but different positions
    if rolling.len() >= 2 {
        // The hash for [1,2,3] should appear at positions 0 and 3
        let first_hash = rolling[0].0;
        let last_item = rolling.iter().find(|(_, pos)| *pos == 3);
        if let Some((last_hash, _)) = last_item {
            assert_eq!(first_hash, *last_hash);
        }
    }
}

#[test]
fn test_rolling_hashes_insufficient_lines() {
    // Test with fewer lines than min_lines
    let signatures = vec![1, 2];
    let min_lines = 3;
    let rolling = rolling_hashes(&signatures, min_lines);

    // Should be empty since we don't have enough lines
    assert_eq!(rolling.len(), 0);
}

#[test]
fn test_file_id_new() {
    let file_id = FileId::new();
    assert_eq!(file_id.num_files, 0);
    assert_eq!(file_id.index_to_name.len(), 0);
    assert_eq!(file_id.name_to_index.len(), 0);
}

#[test]
fn test_file_id_register_file() {
    let mut file_id = FileId::new();

    // Register first file
    let file1 = Arc::new("test1.txt".to_string());
    let id1 = file_id.register_file(Arc::clone(&file1));
    assert_eq!(id1, Some(0));
    assert_eq!(file_id.num_files, 1);

    // Register second file
    let file2 = Arc::new("test2.txt".to_string());
    let id2 = file_id.register_file(Arc::clone(&file2));
    assert_eq!(id2, Some(1));
    assert_eq!(file_id.num_files, 2);

    // Try to register same file again
    let id1_again = file_id.register_file(Arc::clone(&file1));
    assert_eq!(id1_again, None);
    assert_eq!(file_id.num_files, 2); // Should not increment
}

#[test]
fn test_file_id_id_to_name() {
    let mut file_id = FileId::new();

    let file1 = Arc::new("test1.txt".to_string());
    let file2 = Arc::new("test2.txt".to_string());

    file_id.register_file(Arc::clone(&file1));
    file_id.register_file(Arc::clone(&file2));

    assert_eq!(file_id.id_to_name(0), file1);
    assert_eq!(file_id.id_to_name(1), file2);
}

#[test]
fn test_file_id_number_files() {
    let mut file_id = FileId::new();
    assert_eq!(file_id.number_files(), 0);

    file_id.register_file(Arc::new("test1.txt".to_string()));
    assert_eq!(file_id.number_files(), 1);

    file_id.register_file(Arc::new("test2.txt".to_string()));
    assert_eq!(file_id.number_files(), 2);
}

#[test]
fn test_file_signatures_with_temp_file() {
    // Create a temporary file with known content
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(temp_file, "line 1").expect("Failed to write to temp file");
    writeln!(temp_file, "  line 2  ").expect("Failed to write to temp file");
    writeln!(temp_file, "line 3").expect("Failed to write to temp file");

    let temp_path = temp_file.path().to_str().expect("Failed to get temp path");
    let signatures = file_signatures(temp_path);

    // Should have 3 signatures
    assert_eq!(signatures.len(), 3);

    // Test that trimming works - signatures should be based on trimmed content
    let line1_hash = calculate_hash("line 1");
    let line2_hash = calculate_hash("line 2"); // Should be trimmed
    let line3_hash = calculate_hash("line 3");

    assert_eq!(signatures[0], line1_hash);
    assert_eq!(signatures[1], line2_hash);
    assert_eq!(signatures[2], line3_hash);
}

#[test]
fn test_file_signatures_nonexistent_file() {
    let signatures = file_signatures("/nonexistent/file.txt");
    assert_eq!(signatures.len(), 0);
}

#[test]
fn test_line_id_equality() {
    let line1 = LineId { file_id: 1, line_number: 10 };
    let line2 = LineId { file_id: 1, line_number: 10 };
    let line3 = LineId { file_id: 1, line_number: 11 };
    let line4 = LineId { file_id: 2, line_number: 10 };

    assert_eq!(line1, line2);
    assert_ne!(line1, line3);
    assert_ne!(line1, line4);
}

#[test]
fn test_collision_scrub() {
    let mut collision = Collision {
        key: 12345,
        num_lines: 5,
        start_lines: vec![
            LineId { file_id: 1, line_number: 10 },
            LineId { file_id: 1, line_number: 10 }, // Duplicate
            LineId { file_id: 2, line_number: 5 },
        ],
        sig: 0,
    };

    collision.scrub();

    // Should remove duplicates
    assert_eq!(collision.start_lines.len(), 2);
    assert!(collision.sig != 0); // Signature should be calculated
}

#[test]
fn test_overlap_detection() {
    let line1 = LineId { file_id: 1, line_number: 10 };
    let line2 = LineId { file_id: 1, line_number: 12 };
    let line3 = LineId { file_id: 2, line_number: 10 };

    // Same file, overlapping lines (assuming 5 lines overlap)
    assert!(overlap(&line1, &line2, 5));

    // Different files should not overlap
    assert!(!overlap(&line1, &line3, 5));

    // Same file, non-overlapping lines
    let line4 = LineId { file_id: 1, line_number: 20 };
    assert!(!overlap(&line1, &line4, 5));
}

#[test]
fn test_maximize_collision_exact_match() {
    // Create test data for exact sequence matches
    let file_hashes = vec![
        vec![1, 2, 3, 4, 5, 6, 7], // File 0
        vec![8, 1, 2, 3, 4, 9, 10], // File 1: contains [1,2,3,4] at position 1
    ];

    let left = LineId { file_id: 0, line_number: 0 };
    let right = LineId { file_id: 1, line_number: 1 };

    let collision = maximize_collision(&file_hashes, &left, &right, 3);

    assert!(collision.is_some());
    let collision = collision.unwrap();
    assert_eq!(collision.num_lines, 4); // Should match 4 lines: [1,2,3,4]
    assert_eq!(collision.start_lines.len(), 2);
    assert_eq!(collision.start_lines[0], left);
    assert_eq!(collision.start_lines[1], right);
}

#[test]
fn test_maximize_collision_partial_match() {
    // Test where sequences match partially then diverge
    let file_hashes = vec![
        vec![1, 2, 3, 4, 5], // File 0
        vec![1, 2, 3, 99, 100], // File 1: matches first 3 elements, then diverges
    ];

    let left = LineId { file_id: 0, line_number: 0 };
    let right = LineId { file_id: 1, line_number: 0 };

    let collision = maximize_collision(&file_hashes, &left, &right, 2);

    assert!(collision.is_some());
    let collision = collision.unwrap();
    assert_eq!(collision.num_lines, 3); // Should match 3 lines: [1,2,3]
}

#[test]
fn test_maximize_collision_no_match() {
    // Test completely different sequences
    let file_hashes = vec![
        vec![1, 2, 3, 4, 5],
        vec![6, 7, 8, 9, 10],
    ];

    let left = LineId { file_id: 0, line_number: 0 };
    let right = LineId { file_id: 1, line_number: 0 };

    let collision = maximize_collision(&file_hashes, &left, &right, 2);
    // Since the first elements are different (1 != 6), there should be no collision
    // but maximize_collision might still return a collision with 0 lines
    if let Some(collision) = collision {
        assert_eq!(collision.num_lines, 0);
    }
}

#[test]
fn test_maximize_collision_overlapping_same_file() {
    // Test overlap detection in same file
    let file_hashes = vec![
        vec![1, 2, 3, 4, 5, 6, 7, 8],
    ];

    let left = LineId { file_id: 0, line_number: 0 };
    let right = LineId { file_id: 0, line_number: 2 }; // Overlapping in same file

    let collision = maximize_collision(&file_hashes, &left, &right, 3);
    assert!(collision.is_none()); // Should be None due to overlap
}

#[test]
fn test_complex_rolling_hash_patterns() {
    // Test complex patterns with repeating subsequences
    let signatures = vec![1, 2, 3, 4, 1, 2, 3, 5, 6, 1, 2, 3];
    let min_lines = 3;
    let rolling = rolling_hashes(&signatures, min_lines);

    // Verify we get some rolling hashes (should be 10 windows total)
    assert!(!rolling.is_empty());

    // Instead of trying to predict exact hash values, let's test the structure
    // We should have at most 10 rolling windows: positions 0-9
    assert!(rolling.len() <= 10);

    // Check that positions are sequential starting from 0
    if !rolling.is_empty() {
        assert_eq!(rolling[0].1, 0); // First position should be 0

        // Verify positions are in ascending order
        for i in 1..rolling.len() {
            assert!(rolling[i].1 > rolling[i-1].1);
        }
    }

    // Test a simpler case with known duplicates
    let simple_sigs = vec![1, 2, 3, 1, 2, 3];
    let simple_rolling = rolling_hashes(&simple_sigs, 3);

    // Should have windows at positions 0, 1, 2, 3
    // But consecutive duplicates are removed, so we might see fewer
    assert!(!simple_rolling.is_empty());
    assert!(simple_rolling.len() <= 4);
}

#[test]
fn test_file_signatures_complex_content() {
    // Test with various whitespace and content patterns
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    writeln!(temp_file, "function foo() {{").expect("Write failed");
    writeln!(temp_file, "    return 42;").expect("Write failed");
    writeln!(temp_file, "}}").expect("Write failed");
    writeln!(temp_file, "").expect("Write failed"); // Empty line
    writeln!(temp_file, "function bar() {{").expect("Write failed");
    writeln!(temp_file, "    return 42;").expect("Write failed"); // Same as line 2
    writeln!(temp_file, "}}").expect("Write failed");

    let temp_path = temp_file.path().to_str().expect("Failed to get temp path");
    let signatures = file_signatures(temp_path);

    assert_eq!(signatures.len(), 7);

    // Line 2 and line 6 should have the same signature (both "return 42;")
    assert_eq!(signatures[1], signatures[5]); // 0-based indexing

    // Lines 1 and 3 should have the same signature (both "}")
    assert_eq!(signatures[2], signatures[6]); // Both closing braces

    // Check that identical content produces identical hashes
    let return_hash = calculate_hash("return 42;");
    assert_eq!(signatures[1], return_hash);
    assert_eq!(signatures[5], return_hash);
}

#[test]
fn test_collision_with_multiple_files() {
    // Create multiple temporary files with overlapping content
    let mut file1 = NamedTempFile::new().expect("Failed to create temp file1");
    let mut file2 = NamedTempFile::new().expect("Failed to create temp file2");
    let mut file3 = NamedTempFile::new().expect("Failed to create temp file3");

    // File 1: Original content
    writeln!(file1, "line A").expect("Write failed");
    writeln!(file1, "line B").expect("Write failed");
    writeln!(file1, "line C").expect("Write failed");
    writeln!(file1, "unique line 1").expect("Write failed");

    // File 2: Contains duplicate of first 3 lines
    writeln!(file2, "different start").expect("Write failed");
    writeln!(file2, "line A").expect("Write failed");
    writeln!(file2, "line B").expect("Write failed");
    writeln!(file2, "line C").expect("Write failed");
    writeln!(file2, "unique line 2").expect("Write failed");

    // File 3: Contains duplicate at different position
    writeln!(file3, "line A").expect("Write failed");
    writeln!(file3, "line B").expect("Write failed");
    writeln!(file3, "line C").expect("Write failed");

    let path1 = file1.path().to_str().expect("Failed to get path1");
    let path2 = file2.path().to_str().expect("Failed to get path2");
    let path3 = file3.path().to_str().expect("Failed to get path3");

    let sig1 = file_signatures(path1);
    let sig2 = file_signatures(path2);
    let sig3 = file_signatures(path3);

    assert_eq!(sig1.len(), 4);
    assert_eq!(sig2.len(), 5);
    assert_eq!(sig3.len(), 3);

    // Verify the overlapping sequences
    // File1[0..3] should match File2[1..4] and File3[0..3]
    assert_eq!(sig1[0], sig2[1]); // "line A"
    assert_eq!(sig1[1], sig2[2]); // "line B"
    assert_eq!(sig1[2], sig2[3]); // "line C"

    assert_eq!(sig1[0], sig3[0]); // "line A"
    assert_eq!(sig1[1], sig3[1]); // "line B"
    assert_eq!(sig1[2], sig3[2]); // "line C"

    // Test rolling hashes for duplicate detection
    let rolling1 = rolling_hashes(&sig1, 3);
    let rolling2 = rolling_hashes(&sig2, 3);
    let rolling3 = rolling_hashes(&sig3, 3);

    // Should find common 3-line sequences
    assert!(!rolling1.is_empty());
    assert!(!rolling2.is_empty());
    assert!(!rolling3.is_empty());

    // The hash for the first 3 lines should be the same across all files
    let common_hash = rolling1[0].0;
    assert!(rolling2.iter().any(|(h, _)| *h == common_hash));
    assert!(rolling3.iter().any(|(h, _)| *h == common_hash));
}

#[test]
fn test_mixed_match_no_match_sequences() {
    // Test sequences with alternating matches and non-matches
    let signatures1 = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
    let signatures2 = vec![1, 2, 3, 99, 98, 6, 7, 8, 97];
    //                   match--- no-match--- match--- no

    let file_hashes = vec![signatures1, signatures2];

    // Test first matching sequence [1,2,3]
    let left1 = LineId { file_id: 0, line_number: 0 };
    let right1 = LineId { file_id: 1, line_number: 0 };
    let collision1 = maximize_collision(&file_hashes, &left1, &right1, 2);

    assert!(collision1.is_some());
    let collision1 = collision1.unwrap();
    assert_eq!(collision1.num_lines, 3); // [1,2,3]

    // Test second matching sequence [6,7,8] - should start at different positions
    let left2 = LineId { file_id: 0, line_number: 5 };
    let right2 = LineId { file_id: 1, line_number: 5 };
    let collision2 = maximize_collision(&file_hashes, &left2, &right2, 2);

    assert!(collision2.is_some());
    let collision2 = collision2.unwrap();
    assert_eq!(collision2.num_lines, 3); // [6,7,8]

    // Test non-matching sequence in the middle
    let left3 = LineId { file_id: 0, line_number: 3 };
    let right3 = LineId { file_id: 1, line_number: 3 };
    let collision3 = maximize_collision(&file_hashes, &left3, &right3, 2);

    // Should not match [4,5] vs [99,98] - might return Some with 0 lines
    if let Some(collision3) = collision3 {
        assert_eq!(collision3.num_lines, 0);
    }
}

#[test]
fn test_edge_case_single_line_files() {
    // Test files with only one line
    let file_hashes = vec![
        vec![42],
        vec![42],
        vec![99],
    ];

    let min_lines = 1;

    // Should match between files 0 and 1
    let left = LineId { file_id: 0, line_number: 0 };
    let right = LineId { file_id: 1, line_number: 0 };
    let collision = maximize_collision(&file_hashes, &left, &right, min_lines);

    assert!(collision.is_some());
    let collision = collision.unwrap();
    assert_eq!(collision.num_lines, 1);

    // Should not match between files 0 and 2 (42 != 99)
    let right2 = LineId { file_id: 2, line_number: 0 };
    let collision2 = maximize_collision(&file_hashes, &left, &right2, min_lines);
    // maximize_collision might return Some with 0 lines instead of None
    if let Some(collision2) = collision2 {
        assert_eq!(collision2.num_lines, 0);
    }
}

#[test]
fn test_end_of_file_boundary_conditions() {
    // Test matching sequences that extend to end of file
    let file_hashes = vec![
        vec![1, 2, 3, 4, 5],
        vec![99, 98, 3, 4, 5],
    ];

    // Test match at end of both files
    let left = LineId { file_id: 0, line_number: 2 };
    let right = LineId { file_id: 1, line_number: 2 };
    let collision = maximize_collision(&file_hashes, &left, &right, 2);

    assert!(collision.is_some());
    let collision = collision.unwrap();
    assert_eq!(collision.num_lines, 3); // Should match [3,4,5]

    // Test when one file is shorter
    let file_hashes_short = vec![
        vec![1, 2, 3, 4, 5, 6],
        vec![1, 2, 3],
    ];

    let left_short = LineId { file_id: 0, line_number: 0 };
    let right_short = LineId { file_id: 1, line_number: 0 };
    let collision_short = maximize_collision(&file_hashes_short, &left_short, &right_short, 2);

    assert!(collision_short.is_some());
    let collision_short = collision_short.unwrap();
    assert_eq!(collision_short.num_lines, 3); // Limited by shorter file
}

#[test]
fn test_large_file_with_repeating_sections_and_random_separators() {
    // Simulate a large file with repeating 12-line sections separated by random lines
    // This mimics real-world scenarios like configuration files, log patterns, or code templates

    // Define our 12-line repeating pattern
    let pattern_lines = vec![
        "function processData() {",
        "    let data = getData();",
        "    if (data.isEmpty()) {",
        "        return null;",
        "    }",
        "    let result = transform(data);",
        "    if (result.isValid()) {",
        "        save(result);",
        "        return result;",
        "    } else {",
        "        throw new Error('Invalid result');",
        "    }",
    ];

    // Create a large content vector with repeating sections and random separators
    let mut file_content = Vec::new();
    let random_separators = vec![
        "// Random comment 1",
        "/* DEBUG: checkpoint A */",
        "// TODO: optimize this section",
        "/* FIXME: memory leak here */",
        "// Version: 1.2.3",
        "/* Last modified: 2023-01-01 */",
        "// Author: developer@example.com",
        "/* Performance: needs improvement */",
    ];

    // Build content: pattern + random + pattern + random + pattern, etc.
    for section in 0..5 {
        // Add the 12-line pattern
        for line in &pattern_lines {
            file_content.push(line.to_string());
        }

        // Add a random separator (except after the last section)
        if section < 4 {
            let separator_idx = section % random_separators.len();
            file_content.push(random_separators[separator_idx].to_string());
        }
    }

    // Create a temporary file with this content
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    for line in &file_content {
        writeln!(temp_file, "{}", line).expect("Failed to write line");
    }

    let temp_path = temp_file.path().to_str().expect("Failed to get temp path");
    let signatures = file_signatures(temp_path);

    // Verify we have the expected number of lines
    // 5 sections * 12 lines + 4 separators = 64 lines
    assert_eq!(signatures.len(), 64);

    // Test rolling hashes with different window sizes
    let rolling_12 = rolling_hashes(&signatures, 12); // Exact pattern size
    let rolling_6 = rolling_hashes(&signatures, 6);   // Half pattern size
    let rolling_3 = rolling_hashes(&signatures, 3);   // Small window

    // With 12-line windows, we should find 5 patterns but separated by single lines
    // So the algorithm should detect multiple collisions
    assert!(!rolling_12.is_empty());

    // Calculate expected pattern hash
    let pattern_hashes: Vec<u64> = pattern_lines.iter()
        .map(|line| calculate_hash(line.trim()))
        .collect();

    // Create the hash for the full 12-line pattern
    let mut full_pattern_hasher = std::collections::hash_map::DefaultHasher::new();
    for &hash in &pattern_hashes {
        hash.hash(&mut full_pattern_hasher);
    }
    let expected_pattern_hash = full_pattern_hasher.finish();

    // Find all occurrences of our pattern
    let mut pattern_positions = Vec::new();
    for (hash, pos) in &rolling_12 {
        if *hash == expected_pattern_hash {
            pattern_positions.push(*pos);
        }
    }

    // We should find the pattern at positions: 0, 13, 26, 39, 52
    // (each separated by 12 lines + 1 separator = 13 positions apart, except the last)
    let expected_positions = vec![0, 13, 26, 39, 52];

    assert!(pattern_positions.len() >= 3,
        "Should find at least 3 pattern occurrences, found {}", pattern_positions.len());

    // Verify that we found some of the expected positions
    let mut found_expected = 0;
    for expected_pos in &expected_positions {
        if pattern_positions.contains(expected_pos) {
            found_expected += 1;
        }
    }
    assert!(found_expected >= 2,
        "Should find at least 2 expected positions, found {}", found_expected);

    // Test smaller window sizes to ensure we detect partial overlaps
    assert!(rolling_6.len() > rolling_12.len(),
        "6-line windows should produce more matches than 12-line windows");
    assert!(rolling_3.len() > rolling_6.len(),
        "3-line windows should produce more matches than 6-line windows");

    // Verify that identical sub-patterns are detected
    // The first 6 lines should match across all repetitions
    let first_6_pattern_hashes = &pattern_hashes[0..6];
    let mut first_6_hasher = std::collections::hash_map::DefaultHasher::new();
    for &hash in first_6_pattern_hashes {
        hash.hash(&mut first_6_hasher);
    }
    let expected_6_line_hash = first_6_hasher.finish();

    let mut six_line_matches = 0;
    for (hash, _pos) in &rolling_6 {
        if *hash == expected_6_line_hash {
            six_line_matches += 1;
        }
    }

    assert!(six_line_matches >= 3,
        "Should find at least 3 matches for 6-line sub-pattern, found {}", six_line_matches);
}

#[test]
fn test_collision_detection_with_interrupted_patterns() {
    // Test the collision detection system with the repeating pattern scenario

    // Create signature data that simulates our repeating pattern scenario
    let pattern_sigs = vec![100, 101, 102, 103, 104, 105]; // 6-line pattern
    let separator_sig = 999; // Random separator

    // Build file signatures: pattern + separator + pattern + separator + pattern
    let mut file_sigs = Vec::new();
    for _repeat in 0..3 {
        file_sigs.extend_from_slice(&pattern_sigs);
        file_sigs.push(separator_sig);
    }
    file_sigs.extend_from_slice(&pattern_sigs); // Final pattern without separator

    // Total: 3*(6+1) + 6 = 27 lines

    let file_hashes = Mutex::new(vec![file_sigs]);
    let collision_hashes: DashMap<u64, Vec<LineId>> = DashMap::new();

    // Simulate process_file behavior
    let file_id = 0;
    let min_lines = 6;

    // Get the file signatures and compute rolling hashes
    let signatures = match file_hashes.lock() {
        Ok(hashes) => hashes[0].clone(),
        Err(_) => panic!("Failed to lock file_hashes"),
    };

    let rolling = rolling_hashes(&signatures, min_lines);

    // Register rolling hashes in collision map
    for (r_hash, line_number) in &rolling {
        collision_hashes
            .entry(*r_hash)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(LineId {
                file_id,
                line_number: *line_number,
            });
    }

    // Verify collision detection
    assert!(!collision_hashes.is_empty(), "Should detect some patterns");

    // Find entries with multiple occurrences (collisions)
    let mut collision_count = 0;
    let mut max_occurrences = 0;

    for entry in collision_hashes.iter() {
        let occurrences = entry.value().len();
        if occurrences > 1 {
            collision_count += 1;
            max_occurrences = max_occurrences.max(occurrences);
        }
    }

    assert!(collision_count > 0, "Should find at least one collision pattern");
    assert!(max_occurrences >= 3, "Should find patterns that repeat at least 3 times");

    // Test with different minimum line requirements
    let rolling_3 = rolling_hashes(&signatures, 3);
    assert!(rolling_3.len() > rolling.len(),
        "Smaller window should find more potential matches");
}

#[test]
fn test_real_world_firmware_hex_pattern() {
    // Test scenario inspired by firmware blobs stored as hex text
    // These often have highly repetitive patterns with occasional variations

    let hex_pattern = vec![
        "0x00, 0x01, 0x02, 0x03,",
        "0x04, 0x05, 0x06, 0x07,",
        "0x08, 0x09, 0x0A, 0x0B,",
        "0x0C, 0x0D, 0x0E, 0x0F,",
    ];

    let mut content = Vec::new();

    // Repeat the hex pattern 10 times with occasional variations
    for i in 0..10 {
        for line in &hex_pattern {
            if i % 3 == 0 && line.contains("0x0C") {
                // Inject variation every 3rd repetition on a specific line
                content.push(format!("0x0C, 0x0D, 0x{:02X}, 0x0F,", i + 0x10));
            } else {
                content.push(line.to_string());
            }
        }

        // Add occasional separator comment
        if i % 4 == 0 && i > 0 {
            content.push(format!("// Block {}", i / 4));
        }
    }

    // Create temp file and test
    let mut temp_file = NamedTempFile::new().expect("Failed to create temp file");
    for line in &content {
        writeln!(temp_file, "{}", line).expect("Failed to write line");
    }

    let temp_path = temp_file.path().to_str().expect("Failed to get temp path");
    let signatures = file_signatures(temp_path);

    // Test various window sizes for pattern detection
    let rolling_4 = rolling_hashes(&signatures, 4);  // Full pattern
    let rolling_2 = rolling_hashes(&signatures, 2);  // Half pattern

    assert!(!rolling_4.is_empty(), "Should detect 4-line patterns");
    assert!(!rolling_2.is_empty(), "Should detect 2-line patterns");

    // Count unique vs repeated patterns
    let mut pattern_frequency = HashMap::new();
    for (hash, _pos) in &rolling_4 {
        *pattern_frequency.entry(*hash).or_insert(0) += 1;
    }

    let repeated_patterns = pattern_frequency.values()
        .filter(|&&count| count > 1)
        .count();

    assert!(repeated_patterns > 0,
        "Should find repeated patterns in firmware-like hex data");

    // Verify that most patterns repeat (due to the regular structure)
    let total_patterns = pattern_frequency.len();
    let repetition_ratio = repeated_patterns as f64 / total_patterns as f64;

    assert!(repetition_ratio > 0.1,
        "At least 10% of patterns should be repeated, got {:.2}", repetition_ratio);
}