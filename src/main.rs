// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019 Tony Asleson <tony.asleson@gmail.com>
extern crate rags_rs as rags;
use glob::glob;
use rags::argparse;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::canonicalize;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{prelude::*, BufReader};
use std::iter::FromIterator;
use std::process;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn file_signatures(filename: &str) -> Vec<u64> {
    let mut rc: Vec<u64> = Vec::with_capacity(2048);

    match File::open(filename.to_string()) {
        Ok(file) => {
            let mut reader = BufReader::new(file);

            loop {
                let mut buf: Vec<u8> = vec![];
                match reader.read_until(0xA, &mut buf) {
                    Ok(num_bytes) => {
                        if num_bytes == 0 {
                            return rc;
                        } else {
                            let l = String::from_utf8_lossy(&buf);
                            rc.push(calculate_hash(&l.trim()));
                            buf.truncate(0);
                        }
                    }
                    Err(e) => {
                        println!("WARNING: Error processing file {} reason {}", filename, e);
                        return rc;
                    }
                }
            }
        }
        Err(e) => {
            println!("ERROR: Unable to open {}, reason {}", filename, e);
        }
    }

    rc
}

fn rolling_hashes(
    collision_hash: &mut HashMap<u64, Vec<(String, usize)>>,
    filename: &str,
    file_signatures: &[u64],
    min_lines: usize,
) {
    if file_signatures.len() > min_lines {
        let num_lines = file_signatures.len() - min_lines;
        let mut prev_hash: u64 = 0;
        for i in 0..num_lines {
            let mut s = DefaultHasher::new();
            for n in file_signatures.iter().skip(i).take(min_lines) {
                n.hash(&mut s);
            }
            let digest = s.finish();

            if prev_hash != digest {
                match collision_hash.get_mut(&digest) {
                    Some(existing) => existing.push((filename.to_string(), i)),
                    None => {
                        let mut entry: Vec<(String, usize)> = Vec::new();
                        entry.push((filename.to_string(), i));
                        collision_hash.insert(digest, entry);
                    }
                }
            }

            prev_hash = digest;
        }
    }
}

fn process_file(
    collision_hash: &mut HashMap<u64, Vec<(String, usize)>>,
    file_hashes: &mut HashMap<String, Vec<u64>>,
    filename: &str,
    min_lines: usize,
) {
    match canonicalize(filename) {
        Ok(fn_ok) => {
            let c_name_str = String::from(fn_ok.to_str().unwrap());

            if !file_hashes.contains_key(&c_name_str) {
                file_hashes.insert(c_name_str.clone(), file_signatures(&c_name_str));
                rolling_hashes(
                    collision_hash,
                    &c_name_str,
                    file_hashes
                        .get(&c_name_str)
                        .expect("We just inserted filename"),
                    min_lines,
                );
            }
        }
        Err(e) => {
            println!("WARNING: Unable to process file {}, reason {}", filename, e);
        }
    }
}

#[derive(Debug)]
struct Collision {
    key: u64,
    num_lines: usize,
    files: Vec<(String, usize)>,
}

impl Collision {
    fn signature(&self) -> u64 {
        let mut s = DefaultHasher::new();

        for i in &self.files {
            let file_n = &i.0;
            let starts = i.1;
            let end = starts + 1 + self.num_lines;
            let rep = format!("{}{}", end, file_n);
            rep.hash(&mut s);
        }

        s.finish()
    }
}

fn overlap(left: (&str, usize), right: (&str, usize), end: usize) -> bool {
    left.0 == right.0
        && (left.1 == right.1
            || (right.1 >= left.1 && right.1 <= (left.1 + end))
            || (left.1 >= right.1 && left.1 <= (right.1 + end)))
}

fn walk_collision(
    file_hashes: &mut HashMap<String, Vec<u64>>,
    l_info: (&str, usize),
    r_info: (&str, usize),
    min_lines: usize,
) -> Option<Collision> {
    let l_h = file_hashes
        .get(l_info.0)
        .expect("Expect file in file_hashes");
    let r_h = file_hashes
        .get(r_info.0)
        .expect("Expect file in file_hashes");

    // If we have collisions and we overlap, skip
    if overlap(l_info, r_info, min_lines) {
        return None;
    }

    let mut offset: usize = 0;
    let l_num = l_h.len();
    let r_num = r_h.len();
    let mut s = DefaultHasher::new();

    loop {
        let l_index = l_info.1 + offset;
        let r_index = r_info.1 + offset;

        if l_index < l_num && r_index < r_num {
            if l_h[l_index] == r_h[r_index] {
                l_h[l_index].hash(&mut s);
                offset += 1;
            } else {
                break;
            }
        } else {
            break;
        }
    }

    if offset >= min_lines {
        // If after walking we overlap skip too
        if overlap(l_info, r_info, offset) {
            return None;
        }

        let mut files: Vec<(String, usize)> = Vec::new();
        files.push((l_info.0.to_string(), l_info.1));
        files.push((r_info.0.to_string(), r_info.1));
        return Some(Collision {
            key: s.finish(),
            num_lines: offset,
            files,
        });
    }

    None
}

fn find_collisions(
    collision_hash: &mut HashMap<u64, Vec<(String, usize)>>,
    file_hashes: &mut HashMap<String, Vec<u64>>,
    min_lines: usize,
    print_text: bool,
) {
    fn print_dup_text(filename: &str, start: usize, count: usize) {
        let file = File::open(filename).unwrap_or_else(|_| {
            panic!("Unable to open file we have already opened {:?}", filename)
        });
        let mut reader = BufReader::new(file);
        let mut line_number = 0;
        let end = start + count;

        loop {
            let mut buf: Vec<u8> = vec![];

            match reader.read_until(0xA, &mut buf) {
                Ok(num_bytes) => {
                    if num_bytes == 0 {
                        break;
                    } else if line_number >= start && line_number < end {
                        let l = String::from_utf8_lossy(&buf);
                        print!("{}", l);
                    }

                    if line_number > end {
                        break;
                    }

                    line_number += 1;
                }
                Err(e) => {
                    println!("WARNING: Error processing file {} reason {}", filename, e);
                    break;
                }
            }
        }
    }

    fn print_report(printable_results: &mut Vec<&Collision>, print_text: bool, num_files: usize) {
        printable_results.sort_by(|a, b| {
            if a.num_lines == b.num_lines {
                if a.files[0].1 == b.files[0].1 {
                    a.files[0].0.cmp(&b.files[0].0)
                } else {
                    a.files[0].1.cmp(&b.files[0].1)
                }
            } else {
                a.num_lines.cmp(&b.num_lines)
            }
        });
        let mut num_lines = 0;

        for p in printable_results.iter() {
            println!(
                "********************************************************************************"
            );
            println!(
                "Found {} copy & pasted lines in the following files:",
                p.num_lines
            );

            num_lines += p.num_lines * p.files.len();

            for spec_file in &p.files {
                let filename = &spec_file.0;
                let start_line = spec_file.1;
                let end_line = start_line + 1 + p.num_lines;
                println!(
                    "Between lines {} and {} in {}",
                    start_line + 1,
                    end_line,
                    filename
                );
            }

            if print_text {
                print_dup_text(&p.files[0].0, p.files[0].1, p.num_lines);
            }
        }

        println!(
            "Found {} duplicate lines in {} chunks in {} files.\n\
             https://github.com/tasleson/duplihere",
            num_lines,
            printable_results.len(),
            num_files
        )
    }

    let mut results_hash: HashMap<u64, Collision> = HashMap::new();

    // We have processed all the files, remove entries for which we didn't have any collisions
    // to reduce memory consumption
    collision_hash.retain(|_, v| v.len() > 1);
    collision_hash.shrink_to_fit();

    for collisions in collision_hash.values_mut() {
        for l_idx in 0..(collisions.len() - 1) {
            for r_idx in l_idx..collisions.len() {
                let (l_file, l_start) = &collisions[l_idx];
                let (r_file, r_start) = &collisions[r_idx];

                let max_collision = walk_collision(
                    file_hashes,
                    (&l_file, *l_start),
                    (&r_file, *r_start),
                    min_lines,
                );

                if let Some(mut coll) = max_collision {
                    match results_hash.get_mut(&coll.key) {
                        Some(existing) => existing.files.append(&mut coll.files),
                        None => {
                            results_hash.insert(coll.key, coll);
                        }
                    }
                }
            }
        }
    }

    let num_files = file_hashes.len();
    file_hashes.clear();
    file_hashes.shrink_to_fit();
    collision_hash.clear();
    collision_hash.shrink_to_fit();

    let mut final_report: Vec<&mut Collision> = Vec::from_iter(results_hash.values_mut());
    final_report.sort_by(|a, b| a.num_lines.cmp(&b.num_lines).reverse());

    let mut printable_results: Vec<&Collision> = Vec::new();
    let mut chunk_processed: HashMap<u64, bool> = HashMap::new();

    for ea in final_report {
        // Remove duplicates from each by sorting and then dedup
        ea.files.sort_by(|a, b| {
            if a.1 == b.1 {
                a.0.cmp(&b.0) // Number match, order by file name
            } else {
                a.1.cmp(&b.1) // Numbers don't match, order by number
            }
        });
        ea.files.dedup();

        let cs = ea.signature();
        if chunk_processed.get(&cs).is_none() {
            chunk_processed.insert(cs, true);
            printable_results.push(ea);
        }
    }

    chunk_processed.clear();
    chunk_processed.shrink_to_fit();

    print_report(&mut printable_results, print_text, num_files);
}

#[derive(Debug)]
pub struct Options {
    lines: usize,
    print: bool,
    file_globs: Vec<String>,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            lines: 6,
            print: false,
            file_globs: vec![],
        }
    }
}

static LONG_DESC: &str = "Find duplicate lines of text in one or more text files.

The duplicated text can be at different levels of indention,
but otherwise needs to be identical.

More information: https://github.com/tasleson/duplihere";

fn main() -> Result<(), rags::Error> {
    let mut opts = Options::default();
    let mut parser = argparse!();
    parser
        .app_desc("find duplicate text")
        .app_long_desc(LONG_DESC)
        .group("argument", "description")?
        .flag('p', "print", "print duplicate text", &mut opts.print, false)?
        .arg(
            'l',
            "lines",
            "minimum number of duplicate lines",
            &mut opts.lines,
            Some("<number>"),
            false,
        )?
        .list(
            'f',
            "files",
            "1 or more file pattern(s), eg. \"**/*.[h|c]\" \"*.py\"",
            &mut opts.file_globs,
            Some("<pattern 1> <pattern n>"),
            true,
        )?
        .done()?;

    if parser.wants_help() {
        parser.print_help();
    } else {
        let mut collision_hashes: HashMap<u64, Vec<(String, usize)>> = HashMap::new();
        let mut file_hashes: HashMap<String, Vec<u64>> = HashMap::new();

        for g in opts.file_globs {
            match glob(&g) {
                Ok(entries) => {
                    for filename in entries {
                        match filename {
                            Ok(specific_file) => {
                                if specific_file.is_file() {
                                    let file_str_name =
                                        String::from(specific_file.to_str().unwrap());
                                    process_file(
                                        &mut collision_hashes,
                                        &mut file_hashes,
                                        &file_str_name,
                                        opts.lines,
                                    );
                                }
                            }
                            Err(e) => {
                                println!("Unable to process {:?}", e);
                                process::exit(1);
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Bad glob pattern supplied '{}', error: {}", g, e);
                    process::exit(1);
                }
            }
        }

        find_collisions(
            &mut collision_hashes,
            &mut file_hashes,
            opts.lines,
            opts.print,
        );
    }

    Ok(())
}
