// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019 Tony Asleson <tony.asleson@gmail.com>
#[macro_use]
extern crate lazy_static;

extern crate dashmap;
extern crate rags_rs as rags;
use glob::glob;
use rags::argparse;
use rayon::prelude::*;

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use std::collections::{hash_map::DefaultHasher, HashMap, VecDeque};
use std::fs::{canonicalize, File};
use std::hash::{Hash, Hasher};
use std::io::{prelude::*, BufReader};
use std::process;
use std::sync::{Arc, Mutex};

use dashmap::DashMap;

lazy_static! {
    static ref FILE_LOOKUP: Mutex<FileId> = Mutex::new(FileId::new());
}

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

fn rolling_hashes(file_signatures: &[u64], min_lines: usize) -> Vec<(u64, u32)> {
    let mut rc = vec![];

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
                rc.push((digest, i as u32));
            }

            prev_hash = digest;
        }
    }
    rc
}

fn process_file(
    fid: u32,
    filename: &str,
    min_lines: usize,
    file_hashes: &Mutex<Vec<Vec<u64>>>,
    collision_hashes: &DashMap<u64, Vec<(u32, u32)>>,
) {
    let file_signatures = file_signatures(&filename);
    let file_rolling_hashes = rolling_hashes(&file_signatures, min_lines);

    file_hashes.lock().unwrap()[fid as usize] = file_signatures;

    {
        for e in file_rolling_hashes {
            let (r_hash, line_number) = e;
            match collision_hashes.get_mut(&r_hash) {
                Some(mut existing) => existing.push((fid, line_number)),
                None => {
                    let mut entry: Vec<(u32, u32)> = Vec::new();
                    entry.push((fid, line_number));
                    collision_hashes.insert(r_hash, entry);
                }
            }
        }
    }
}

#[derive(Debug)]
struct Collision {
    key: u64,
    num_lines: u32,
    files: Vec<(u32, u32)>,
    sig: u64,
}

impl Serialize for Collision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let files_infos: Vec<(String, u32)> = self
            .files
            .iter()
            .map(|i| (FILE_LOOKUP.lock().unwrap().id_to_name(i.0).to_string(), i.1))
            .collect();

        let mut fid = serializer.serialize_struct("Collision", 3)?;
        fid.serialize_field("key", &self.key)?;
        fid.serialize_field("num_lines", &self.num_lines)?;
        fid.serialize_field("files", &files_infos)?;
        fid.end()
    }
}

impl Collision {
    fn signature(&self) -> u64 {
        self.sig
    }

    fn _signature(&mut self) {
        let mut s = DefaultHasher::new();

        for i in &self.files {
            let file_n = &i.0;
            let starts = i.1;
            let end = starts + 1 + self.num_lines;
            let rep = format!("{}{}", end, file_n);
            rep.hash(&mut s);
        }
        self.sig = s.finish();
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

        self._signature()
    }
}

#[derive(Debug, Serialize)]
struct ReportResults<'a> {
    num_lines: u64,
    num_ignored: u64,
    duplicates: &'a [Collision],
}

fn overlap(left: (u32, u32), right: (u32, u32), end: u32) -> bool {
    left.0 == right.0
        && (left.1 == right.1
            || (right.1 >= left.1 && right.1 <= (left.1 + end))
            || (left.1 >= right.1 && left.1 <= (right.1 + end)))
}

fn walk_collision(
    file_hashes: &[Vec<u64>],
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
        sig: 0,
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

fn print_report(
    printable_results: &[Collision],
    opts: &Options,
    ignore_hashes: &HashMap<u64, bool>,
) {
    let mut num_lines: u64 = 0;
    let mut ignored: u64 = 0;

    for p in printable_results.iter() {
        if ignore_hashes.contains_key(&p.key) {
            ignored += 1;
        } else {
            num_lines += (p.num_lines as usize * (p.files.len() - 1)) as u64;

            if !opts.json {
                println!(
                    "{}\nHash signature = {}\nFound {} copy & pasted lines in the following files:",
                    "*".repeat(80),
                    p.key,
                    p.num_lines
                );

                for spec_file in &p.files {
                    let filename = FILE_LOOKUP.lock().unwrap().id_to_name(spec_file.0);
                    let start_line = spec_file.1;
                    let end_line = start_line + 1 + p.num_lines;
                    println!(
                        "Between lines {} and {} in {}",
                        start_line + 1,
                        end_line,
                        filename
                    );
                }

                if opts.print {
                    print_dup_text(
                        FILE_LOOKUP
                            .lock()
                            .unwrap()
                            .id_to_name(p.files[0usize].0)
                            .as_str(),
                        p.files[0usize].1 as usize,
                        p.num_lines as usize,
                    );
                }
            }
        }
    }

    if !opts.json {
        println!(
            "Found {} duplicate lines in {} chunks in {} files, {} chunks ignored.\n\
            https://github.com/tasleson/duplihere",
            num_lines,
            printable_results.len() - ignored as usize,
            FILE_LOOKUP.lock().unwrap().number_files(),
            ignored
        )
    } else {
        let r = ReportResults {
            num_lines,
            num_ignored: ignored,
            duplicates: printable_results,
        };
        println!("{}", serde_json::to_string_pretty(&r).unwrap());
    }
}

fn johnny_cash(
    collisions: &[(u32, u32)],
    file_hashes: &[Vec<u64>],
    min_lines: u32,
    results_hash: &DashMap<u64, Collision>,
) {
    for l_idx in 0..(collisions.len() - 1) {
        for r_idx in l_idx..collisions.len() {
            let (l_file, l_start) = &collisions[l_idx];
            let (r_file, r_start) = &collisions[r_idx];

            if let Some(mut coll) = walk_collision(
                file_hashes,
                (*l_file, *l_start),
                (*r_file, *r_start),
                min_lines,
            ) {
                match results_hash.get_mut(&coll.key) {
                    Some(mut existing) => existing.files.append(&mut coll.files),
                    None => {
                        results_hash.insert(coll.key, coll);
                    }
                }
            }
        }
    }
}

fn find_collisions(
    collision_hash: DashMap<u64, Vec<(u32, u32)>>,
    file_hashes: &mut Vec<Vec<u64>>,
    opts: &Options,
) -> DashMap<u64, Collision> {
    let results_hash: DashMap<u64, Collision> = DashMap::new();

    // We have processed all the files, remove entries for which we didn't have any collisions
    // to reduce memory consumption.  For large amounts of text this single call is solely
    // responsible for consuming ~18 % total run-time in a single thread.  At the moment no
    // better approach works for culling the hash.  We need to toss entries that never got a hash
    // collision.
    collision_hash.retain(|_, v| v.len() > 1);
    collision_hash.shrink_to_fit();

    let collision_vec: Vec<Vec<(u32, u32)>> = collision_hash.into_iter().map(|(_, v)| v).collect();

    collision_vec
        .par_iter()
        .for_each(|e| johnny_cash(e, file_hashes, opts.lines, &results_hash));

    results_hash
}

fn process_report(
    results_hash: DashMap<u64, Collision>,
    opts: &Options,
    ignore_hashes: &HashMap<u64, bool>,
) {
    let mut final_report: Vec<Collision> = results_hash.into_iter().map(|(_, v)| v).collect();
    final_report.par_sort_unstable_by(|a, b| a.num_lines.cmp(&b.num_lines).reverse());

    let mut printable_results: Vec<Collision> = Vec::new();

    {
        let mut chunk_processed: HashMap<u64, bool> = HashMap::new();

        final_report.par_iter_mut().for_each(|ea| ea.scrub());

        for ea in final_report {
            let cs = ea.signature();
            if chunk_processed.get(&cs).is_none() {
                chunk_processed.insert(cs, true);
                printable_results.push(ea);
            }
        }
    }

    printable_results.par_sort_unstable_by(|a, b| {
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

    print_report(&printable_results, &opts, &ignore_hashes);
}

fn get_ignore_hashes(file_name: &str) -> HashMap<u64, bool> {
    let mut ignores: HashMap<u64, bool> = HashMap::new();

    let fh = File::open(file_name.to_string());

    match fh {
        Ok(fh) => {
            let buf = BufReader::new(fh);

            for line in buf.lines() {
                let t = line.unwrap();
                let l = t.trim();

                if !l.is_empty() && !l.starts_with('#') {
                    if let Ok(hv) = l.parse::<u64>() {
                        ignores.insert(hv, true);
                    } else {
                        println!("WARNING: Ignore file contains invalid hash value \"{}\"", l);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!(
                "Unable to open supplied ignore file {}, reason: {}",
                file_name, e
            );
            process::exit(2);
        }
    }

    ignores
}

#[derive(Debug)]
struct FileId {
    num_files: u32,
    index_to_name: Vec<Arc<String>>,
    name_to_index: HashMap<Arc<String>, u32>,
}

impl FileId {
    fn new() -> FileId {
        FileId {
            num_files: 0,
            index_to_name: vec![],
            name_to_index: HashMap::new(),
        }
    }

    fn register_file(&mut self, file_name: &str) -> Option<u32> {
        if self.name_to_index.contains_key(&file_name.to_string()) {
            return None;
        }
        let num = self.num_files;
        let name = Arc::new(file_name.to_string());

        self.index_to_name.push(name.clone());
        self.name_to_index.insert(name, self.num_files);
        if let Some(v) = self.num_files.checked_add(1) {
            self.num_files = v;
        } else {
            eprintln!("Number of files processed exceeds {}", u32::max_value());
            process::exit(2);
        }
        Some(num)
    }

    fn id_to_name(&self, index: u32) -> Arc<String> {
        self.index_to_name[index as usize].clone()
    }

    fn number_files(&self) -> u32 {
        self.num_files
    }
}

#[derive(Debug)]
pub struct Options {
    lines: u32,
    print: bool,
    json: bool,
    file_globs: Vec<String>,
    ignore: String,
}

impl Default for Options {
    fn default() -> Options {
        Options {
            lines: 6,
            print: false,
            json: false,
            file_globs: vec![],
            ignore: "".to_string(),
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
        .flag('j', "json", "output JSON", &mut opts.json, false)?
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
        .arg(
            'i',
            "ignore",
            "file containing hash values to ignore, one per line",
            &mut opts.ignore,
            Some("<file name>"),
            false,
        )?
        .done()?;

    if parser.wants_help() {
        parser.print_help();
    } else {
        let results_hash: DashMap<u64, Collision>;
        let mut ignore_hash: HashMap<u64, bool> = HashMap::new();

        {
            let mut files_to_process: Vec<(u32, String)> = vec![];

            if !opts.ignore.is_empty() {
                ignore_hash = get_ignore_hashes(&opts.ignore);
            }

            {
                // Hold the lock on FILE_LOOKUP for the duration as we are single threaded here.
                let mut file_lookup_locked = FILE_LOOKUP.lock().unwrap();

                for g in &opts.file_globs {
                    match glob(&g) {
                        Ok(entries) => {
                            for filename in entries {
                                match filename {
                                    Ok(specific_file) => {
                                        if specific_file.is_file() {
                                            let file_str_name =
                                                String::from(specific_file.to_str().unwrap());

                                            match canonicalize(file_str_name.clone()) {
                                                Ok(fn_ok) => {
                                                    let c_name_str =
                                                        String::from(fn_ok.to_str().unwrap());

                                                    if let Some(fid) = file_lookup_locked
                                                        .register_file(&c_name_str)
                                                    {
                                                        files_to_process.push((fid, c_name_str));
                                                    }
                                                }
                                                Err(e) => {
                                                    println!(
                                                    "WARNING: Unable to process file {}, reason {}",
                                                    file_str_name, e
                                                );
                                                }
                                            }
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
            }

            let collision_hashes: DashMap<u64, Vec<(u32, u32)>> = DashMap::new();
            let file_hashes: Mutex<Vec<Vec<u64>>> =
                Mutex::new(vec![vec![0; 0]; files_to_process.len()]);

            files_to_process.par_iter().for_each(|e| {
                process_file(
                    e.0,
                    &e.1,
                    opts.lines as usize,
                    &file_hashes,
                    &collision_hashes,
                )
            });

            results_hash =
                find_collisions(collision_hashes, &mut file_hashes.lock().unwrap(), &opts);
        }

        process_report(results_hash, &opts, &ignore_hash);
    }

    Ok(())
}
