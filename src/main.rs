// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019 Tony Asleson <tony.asleson@gmail.com>

extern crate argparse_rs;

use argparse_rs::{ArgParser, ArgType};
use glob::glob;

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::env;
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

fn file_signatures(filename: &String) -> Vec<u64> {
    let mut rc: Vec<u64> = Vec::with_capacity(2048);

    let file = File::open(filename.clone()).expect(&format!("Unable to open file {:?}", filename));
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

fn rolling_hashes(
    collision_hash: &mut HashMap<u64, Vec<(String, usize)>>,
    filename: &String,
    file_signatures: &Vec<u64>,
    min_lines: usize,
) -> () {
    if file_signatures.len() > min_lines {
        let num_lines = file_signatures.len() - min_lines;
        let mut prev_hash: u64 = 0;
        for i in 0..num_lines {
            let mut s = DefaultHasher::new();
            for n in i..(i + min_lines) {
                file_signatures[n].hash(&mut s);
            }
            let digest = s.finish();

            if prev_hash != digest {
                match collision_hash.get_mut(&digest) {
                    Some(existing) => existing.push((filename.clone(), i)),
                    None => {
                        let mut entry: Vec<(String, usize)> = Vec::new();
                        entry.push((filename.clone(), i));
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
    filename: &String,
    min_lines: usize,
) -> () {
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

fn walk_collision(
    file_hashes: &mut HashMap<String, Vec<u64>>,
    left_file: &String,
    left_start: usize,
    right_file: &String,
    right_start: usize,
    min_lines: usize,
) -> Option<Collision> {
    let l_h = file_hashes
        .get(left_file)
        .expect("Expect file in file_hashes");
    let r_h = file_hashes
        .get(right_file)
        .expect("Expect file in file_hashes");

    // If we have collisions where we overlap
    if left_file == right_file
        && (left_start == right_start
            || (right_start >= left_start && right_start <= (left_start + min_lines))
            || (left_start >= right_start && left_start <= (right_start + min_lines)))
    {
        return None;
    }

    let mut offset: usize = 0;
    let l_num = l_h.len();
    let r_num = r_h.len();
    let mut s = DefaultHasher::new();

    loop {
        let l_index = left_start + offset;
        let r_index = right_start + offset;

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

    // If after walking we overlap skip too
    if left_file == right_file
        && offset >= min_lines
        && ((right_start >= left_start && right_start <= (left_start + offset))
            || (left_start >= right_start && left_start <= (right_start + offset)))
    {
        return None;
    }

    if offset >= min_lines {
        let mut files: Vec<(String, usize)> = Vec::new();
        files.push((left_file.clone(), left_start));
        files.push((right_file.clone(), right_start));
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
) -> () {
    fn chunk_sig(coll: &Collision) -> u64 {
        let mut s = DefaultHasher::new();

        for i in &coll.files {
            let file_n = &i.0;
            let starts = i.1;
            let end = starts + 1 + coll.num_lines;
            let rep = format!("{}{}", end, file_n);
            rep.hash(&mut s);
        }

        s.finish()
    }

    fn print_dup_text(filename: &String, start: usize, count: usize) {
        let file =
            File::open(filename.clone()).expect(&format!("Unable to open file {:?}", filename));
        let mut reader = BufReader::new(file);
        let mut line_number = 0;
        let end = start + count;

        loop {
            let mut buf: Vec<u8> = vec![];

            match reader.read_until(0xA, &mut buf) {
                Ok(num_bytes) => {
                    if num_bytes == 0 {
                        break;
                    } else {
                        if line_number >= start && line_number < end {
                            let l = String::from_utf8_lossy(&buf);
                            print!("{}", l);
                        }
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

    fn print_report(printable_results: &mut Vec<&Collision>, print_text: bool) {
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
        let total_num = printable_results.len();

        for p in printable_results {
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
            "Found {} duplicate lines in {} chunks",
            num_lines, total_num
        )
    }

    let mut results_hash: HashMap<u64, Collision> = HashMap::new();

    for collisions in collision_hash.values_mut().filter(|a| a.len() > 1) {
        for l_idx in 0..(collisions.len() - 1) {
            for r_idx in l_idx..collisions.len() {
                let (l_file, l_start) = &collisions[l_idx];
                let (r_file, r_start) = &collisions[r_idx];

                let max_collision =
                    walk_collision(file_hashes, &l_file, *l_start, &r_file, *r_start, min_lines);

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

        let cs = chunk_sig(ea);
        if chunk_processed.get(&cs).is_none() {
            chunk_processed.insert(cs, true);
            printable_results.push(ea);
        }
    }

    print_report(&mut printable_results, print_text);
}

fn main() {
    let mut parser = ArgParser::new("duplihere".into());

    parser.add_opt(
        "lines",
        Some("6"),
        'l',
        false,
        "Minimum number of duplicate lines, default 6",
        ArgType::Option,
    );
    parser.add_opt(
        "print",
        Some("false"),
        'p',
        false,
        "Print duplicate text",
        ArgType::Flag,
    );
    parser.add_opt("files", None, 'f', true, "File pattern(s)", ArgType::List);

    let args: Vec<String> = env::args().collect();
    let mut collision_hashes: HashMap<u64, Vec<(String, usize)>> = HashMap::new();
    let mut file_hashes: HashMap<String, Vec<u64>> = HashMap::new();

    let parsed = parser.parse(args.iter());

    match parsed {
        Ok(p) => {
            let num_lines: usize = p
                .get::<String>("lines")
                .unwrap()
                .parse::<usize>()
                .unwrap_or(6);
            if num_lines < 3 {
                println!("Minimum number of lines is 3, {} supplied!", num_lines);
                parser.help();
                return;
            }

            let str_to_strings = |s: &str| {
                Some(
                    s.split_whitespace()
                        .map(|s| s.to_string())
                        .collect::<Vec<String>>(),
                )
            };

            let print_txt = p.get::<bool>("print").unwrap_or(false);
            let files = p.get_with("files", str_to_strings).unwrap();

            for g in files {
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
                                            num_lines,
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
                num_lines,
                print_txt,
            );
        }
        Err(e) => {
            println!("{}!\n", e);
            parser.help();
        }
    }
}
