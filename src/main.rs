// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019 Tony Asleson <tony.asleson@gmail.com>
extern crate rags_rs as rags;
use glob::glob;
use rags::argparse;

use std::collections::{hash_map::DefaultHasher, HashMap, VecDeque};
use std::fs::{canonicalize, File};
use std::hash::{Hash, Hasher};
use std::io::{prelude::*, BufReader};
use std::{iter::FromIterator, process, rc::Rc};

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn file_signatures(filename: &str) -> Vec<u64> {
    let mut rc: Vec<u64> = Vec::new();

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
    collision_hash: &mut HashMap<u64, Vec<(u32, u32)>>,
    fid: u32,
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
                    Some(existing) => existing.push((fid, i as u32)),
                    None => {
                        let mut entry: Vec<(u32, u32)> = Vec::new();
                        entry.push((fid, i as u32));
                        collision_hash.insert(digest, entry);
                    }
                }
            }

            prev_hash = digest;
        }
    }
}

fn process_file(
    collision_hash: &mut HashMap<u64, Vec<(u32, u32)>>,
    file_hashes: &mut Vec<Vec<u64>>,
    filename: &str,
    min_lines: usize,
    lookup: &mut FileId,
) {
    match canonicalize(filename) {
        Ok(fn_ok) => {
            let c_name_str = String::from(fn_ok.to_str().unwrap());

            if !lookup.file_exists(&c_name_str) {
                let fid = lookup.register_file(&c_name_str);
                file_hashes.insert(fid as usize, file_signatures(&c_name_str));
                rolling_hashes(collision_hash, fid, &file_hashes[fid as usize], min_lines);
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
    num_lines: u32,
    files: Vec<(u32, u32)>,
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

    // Remove overlaps for a collision result when they all refer to the same file.  This gets
    // ugly when a file contains a repeating sequence which is separated by 1 or more lines, but
    // less than the number that are duplicated.
    // A good example of this is:
    // linux/drivers/net/wireless/broadcom/brcm80211/brcmsmac/phy/phytbl_n.c
    fn remove_overlap_same_file(&mut self) {
        let first = &self.files[0].0;
        let mut keep: VecDeque<(u32, u32)> = VecDeque::new();

        // If all the files are the same, process any overlaps.
        if self.files.iter().all(|(file, _)| file == first) {
            while let Some(cur) = self.files.pop() {
                if let Some(next_one) = self.files.last() {
                    if !(cur.1 >= next_one.1 && cur.1 <= next_one.1 + self.num_lines) {
                        keep.push_front(cur);
                    }
                } else {
                    keep.push_front(cur);
                    break;
                }
            }
            self.files = Vec::from(keep);
        }
    }

    fn scrub(&mut self) {
        // Remove duplicates from each by sorting and then dedup
        self.files.sort_by(|a, b| {
            if a.1 == b.1 {
                a.0.cmp(&b.0) // Number match, order by file name
            } else {
                a.1.cmp(&b.1) // Numbers don't match, order by number
            }
        });
        self.files.dedup();
        self.remove_overlap_same_file();
    }
}

fn overlap(left: (u32, u32), right: (u32, u32), end: u32) -> bool {
    left.0 == right.0
        && (left.1 == right.1
            || (right.1 >= left.1 && right.1 <= (left.1 + end))
            || (left.1 >= right.1 && left.1 <= (right.1 + end)))
}

fn walk_collision(
    file_hashes: &mut Vec<Vec<u64>>,
    l_info: (u32, u32),
    r_info: (u32, u32),
    min_lines: u32,
) -> Option<Collision> {
    let l_h = &file_hashes[l_info.0 as usize];
    let r_h = &file_hashes[r_info.0 as usize];

    // If we have collisions and we overlap, skip
    if overlap(l_info, r_info, min_lines) {
        return None;
    }

    let mut offset: u32 = 0;
    let l_num = l_h.len();
    let r_num = r_h.len();
    let mut s = DefaultHasher::new();

    loop {
        let l_index: usize = (l_info.1 + offset) as usize;
        let r_index: usize = (r_info.1 + offset) as usize;

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
    if overlap(l_info, r_info, offset) {
        return None;
    }

    let mut files: Vec<(u32, u32)> = Vec::new();
    files.push((l_info.0, l_info.1));
    files.push((r_info.0, r_info.1));
    Some(Collision {
        key: s.finish(),
        num_lines: offset,
        files,
    })
}

fn print_dup_text(filename: &str, start: usize, count: usize) {
    let file = File::open(filename)
        .unwrap_or_else(|_| panic!("Unable to open file we have already opened {:?}", filename));
    let mut reader = BufReader::new(file);
    let mut line_number = 0;
    let end = start + count;

    while line_number < end {
        let mut buf: Vec<u8> = vec![];
        match reader.read_until(0xA, &mut buf) {
            Ok(num_bytes) => {
                if num_bytes == 0 {
                    break;
                } else if line_number >= start {
                    print!("{}", String::from_utf8_lossy(&buf));
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

fn print_report(printable_results: &[&Collision], print_text: bool, lookup: &FileId) {
    let mut num_lines: u64 = 0;

    for p in printable_results.iter() {
        println!(
            "********************************************************************************"
        );
        println!(
            "Found {} copy & pasted lines in the following files:",
            p.num_lines
        );

        num_lines += (p.num_lines as usize * (p.files.len() - 1)) as u64;

        for spec_file in &p.files {
            let filename = lookup.id_to_name(spec_file.0);
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
            print_dup_text(
                lookup.id_to_name(p.files[0usize].0).as_str(),
                p.files[0usize].1 as usize,
                p.num_lines as usize,
            );
        }
    }

    println!(
        "Found {} duplicate lines in {} chunks in {} files.\n\
         https://github.com/tasleson/duplihere",
        num_lines,
        printable_results.len(),
        lookup.number_files(),
    )
}

fn find_collisions(
    collision_hash: &mut HashMap<u64, Vec<(u32, u32)>>,
    file_hashes: &mut Vec<Vec<u64>>,
    min_lines: u32,
) -> HashMap<u64, Collision> {
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
                    (*l_file, *l_start),
                    (*r_file, *r_start),
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

    results_hash
}

fn process_report(results_hash: &mut HashMap<u64, Collision>, lookup: &FileId, print_text: bool) {
    let mut final_report: Vec<&mut Collision> = Vec::from_iter(results_hash.values_mut());
    final_report.sort_by(|a, b| a.num_lines.cmp(&b.num_lines).reverse());

    let mut printable_results: Vec<&Collision> = Vec::new();

    {
        let mut chunk_processed: HashMap<u64, bool> = HashMap::new();

        for ea in final_report {
            ea.scrub();
            let cs = ea.signature();
            if chunk_processed.get(&cs).is_none() {
                chunk_processed.insert(cs, true);
                printable_results.push(ea);
            }
        }
    }

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

    print_report(&printable_results, print_text, lookup);
}

struct FileId {
    num_files: u32,
    index_to_name: Vec<Rc<String>>,
    name_to_index: HashMap<Rc<String>, u32>,
}

impl FileId {
    fn new() -> FileId {
        FileId {
            num_files: 0,
            index_to_name: vec![],
            name_to_index: HashMap::new(),
        }
    }

    fn register_file(&mut self, file_name: &str) -> u32 {
        let num = self.num_files;
        let name = Rc::new(file_name.to_string());

        self.index_to_name.push(name.clone());
        self.name_to_index.insert(name.clone(), self.num_files);
        if let Some(v) = self.num_files.checked_add(1) {
            self.num_files = v;
        } else {
            eprintln!("Number of files processed exceeds {}", u32::max_value());
            process::exit(2);
        }
        num
    }

    fn id_to_name(&self, index: u32) -> Rc<String> {
        self.index_to_name[index as usize].clone()
    }

    fn file_exists(&self, file_name: &str) -> bool {
        self.name_to_index.contains_key(&file_name.to_string())
    }

    fn number_files(&self) -> u32 {
        self.num_files
    }
}

#[derive(Debug)]
pub struct Options {
    lines: u32,
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
            "file",
            "pattern or file eg. \"**/*.[h|c]\" recursive, \"*.py\", \
            \"file.ext\", can repeat",
            &mut opts.file_globs,
            Some("<pattern or specific file>"),
            true,
        )?
        .done()?;

    if parser.wants_help() {
        parser.print_help();
    } else {
        let mut lookup = FileId::new();
        let mut results_hash: HashMap<u64, Collision>;

        {
            let mut collision_hashes: HashMap<u64, Vec<(u32, u32)>> = HashMap::new();
            let mut file_hashes: Vec<Vec<u64>> = vec![];

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
                                            opts.lines as usize,
                                            &mut lookup,
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
            results_hash = find_collisions(&mut collision_hashes, &mut file_hashes, opts.lines);
        }

        process_report(&mut results_hash, &lookup, opts.print);
    }

    Ok(())
}
