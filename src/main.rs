// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>
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

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

lazy_static! {
    static ref FILE_LOOKUP: Mutex<FileId> = Mutex::new(FileId::new());
}

/// Generates the hash for 'T' which in this case is a utf-8 string.
fn calculate_hash<T: Hash>(t: T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// For a given file, walk it line by line calculating, removing leading and trailing WS and
/// calculating the signatures for each line, return the information as a vector of hash signatures.
fn file_signatures(filename: &str) -> Vec<u64> {
    let file = match File::open(filename) {
        Ok(file) => file,
        Err(e) => {
            eprintln!("ERROR: Unable to open {}, reason {}", filename, e);
            return Vec::new();
        }
    };

    let mut rc: Vec<u64> = Vec::new();
    let mut reader = BufReader::new(file);
    let mut buf: Vec<u8> = vec![];

    loop {
        match reader.read_until(b'\n', &mut buf) {
            Ok(num_bytes) => {
                if num_bytes == 0 {
                    return rc;
                } else {
                    let l = String::from_utf8_lossy(&buf);
                    rc.push(calculate_hash(l.trim()));
                    buf.clear();
                }
            }
            Err(e) => {
                eprintln!("WARNING: Error processing file {} reason {}", filename, e);
                return rc;
            }
        }
    }
}

/// For a specific file, calculate the hash signature for 'min_lines' in size using a sliding window
/// so that we can detect duplicate text of at least min_lines in size anywhere in each file.
/// Store the hash signature and start line in a vector of tuples which we will then register
/// in the collision hash.
fn rolling_hashes(file_signatures: &[u64], min_lines: usize) -> Vec<(u64, u32)> {
    let mut rc = vec![];
    let mut prev_hash: u64 = 0;

    for (i, window) in file_signatures.windows(min_lines).enumerate() {
        let mut s = DefaultHasher::new();
        for n in window {
            n.hash(&mut s);
        }

        let digest = s.finish();

        if prev_hash != digest {
            rc.push((digest, i as u32));
        }
        prev_hash = digest;
    }

    rc
}

fn process_file(
    file_id: u32,
    filename: &str,
    min_lines: usize,
    file_hashes: &Mutex<Vec<Vec<u64>>>,
    collision_hashes: &DashMap<u64, Vec<LineId>>,
) {
    let file_signatures = file_signatures(filename);
    let file_rolling_hashes = rolling_hashes(&file_signatures, min_lines);

    match file_hashes.lock() {
        Ok(mut hashes) => hashes[file_id as usize] = file_signatures,
        Err(e) => {
            eprintln!("ERROR: Failed to acquire lock on file_hashes: {}", e);
            return;
        }
    }

    for e in file_rolling_hashes {
        let (r_hash, line_number) = e;
        collision_hashes
            .entry(r_hash)
            .or_insert_with(|| Vec::with_capacity(1))
            .push(LineId {
                file_id,
                line_number,
            });
    }
}

/// Used to record a section of duplicated text.  We store the hash signature, how many lines
/// match and a vector of file ids and the starting line in the file.
#[derive(Debug)]
struct Collision {
    key: u64,
    num_lines: u32,
    start_lines: Vec<LineId>,
    sig: u64,
}

/// Used to convert a collision in our results to JSON for it.
impl Serialize for Collision {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let file_lookup_lock = match FILE_LOOKUP.lock() {
            Ok(lock) => lock,
            Err(e) => {
                eprintln!("ERROR: Failed to acquire lock on FILE_LOOKUP: {}", e);
                return Err(serde::ser::Error::custom(
                    "Failed to acquire FILE_LOOKUP lock",
                ));
            }
        };
        let files_infos: Vec<(String, u32)> = self
            .start_lines
            .iter()
            .map(|i| {
                (
                    file_lookup_lock.id_to_name(i.file_id).to_string(),
                    i.line_number,
                )
            })
            .collect();

        let mut fid = serializer.serialize_struct("Collision", 3)?;
        fid.serialize_field("key", &self.key)?;
        fid.serialize_field("num_lines", &self.num_lines)?;
        fid.serialize_field("files", &files_infos)?;
        fid.end()
    }
}

impl Collision {
    /// A signature for a collision is the hash value of the data that represents the collision,
    /// this is used to identify duplicate result collisions, see _signature for calculation.
    fn signature(&self) -> u64 {
        self.sig
    }

    fn _signature(&mut self) {
        let mut s = DefaultHasher::new();

        for i in &self.start_lines {
            let end = i.line_number + 1 + self.num_lines;
            let rep = format!("{}{}", end, i.file_id);
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
        let first = self.start_lines[0].file_id;
        let mut keep: VecDeque<LineId> = VecDeque::new();

        // If all the files are the same, process any overlaps.
        if self
            .start_lines
            .iter()
            .all(|line_id| line_id.file_id == first)
        {
            while let Some(cur) = self.start_lines.pop() {
                if let Some(next_one) = self.start_lines.last() {
                    if !(cur.line_number >= next_one.line_number
                        && cur.line_number <= next_one.line_number + self.num_lines)
                    {
                        keep.push_front(cur);
                    }
                } else {
                    keep.push_front(cur);
                    break;
                }
            }
            self.start_lines = Vec::from(keep);
        }
    }

    /// Given a collision, remove duplicate files from it, any overlaps for the same file
    /// and then generate it's signature.  This is done because we can run into some very
    /// interesting text patterns for firmware blobs stored as hex text which have repeating
    /// sequences.  TODO: Revisit the need for this code with actual examples to explain it better.
    /// I should have taken better notes in the code when I was running into these very interesting
    /// results and wondering what the input looked like.
    fn scrub(&mut self) {
        // Remove duplicates from each by sorting and then dedup
        self.start_lines.sort_by(|a, b| {
            a.line_number
                .cmp(&b.line_number)
                .then_with(|| a.file_id.cmp(&b.file_id))
        });
        self.start_lines.dedup();
        self.remove_overlap_same_file();

        self._signature()
    }
}

/// Some stats on what we processed and found.
#[derive(Debug, Serialize)]
struct ReportResults<'a> {
    num_lines: u64,
    num_ignored: u64,
    duplicates: &'a [Collision],
}

// Check to see if we are checking for duplicate text in the same file and that one or more lines
// overlap with each other.  There is nothing useful to report when this occurs, because the same
// lines of text match each other in the same file.
fn overlap(left: &LineId, right: &LineId, end: u32) -> bool {
    left.file_id == right.file_id
        && (left.line_number == right.line_number
            || (right.line_number >= left.line_number
                && right.line_number <= (left.line_number + end))
            || (left.line_number >= right.line_number
                && left.line_number <= (right.line_number + end)))
}

/// Find the largest number of matching lines by going line by line from a known duplication point
/// and recording it if it's bigger than the default number of matching lines
fn maximize_collision(
    file_hashes: &[Vec<u64>],
    l_info: &LineId, // File id (index into file_hashes), line start
    r_info: &LineId, // File id (index into file_hashes, line start
    min_lines: u32,
) -> Option<Collision> {
    let l_h = &file_hashes[l_info.file_id as usize];
    let r_h = &file_hashes[r_info.file_id as usize];

    // If we have collisions and we overlap, skip
    if overlap(l_info, r_info, min_lines) {
        return None;
    }

    let mut offset: u32 = 0;
    let l_num = l_h.len();
    let r_num = r_h.len();
    let mut s = DefaultHasher::new();

    loop {
        let l_index: usize = (l_info.line_number + offset) as usize;
        let r_index: usize = (r_info.line_number + offset) as usize;

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

    let files: Vec<LineId> = vec![*l_info, *r_info];
    Some(Collision {
        key: s.finish(),
        num_lines: offset,
        start_lines: files,
        sig: 0,
    })
}

/// Given a file name, a start line number, and number of lines, dump the text into the output.
fn print_dup_text(filename: &str, start_line: usize, count: usize) {
    let file = match File::open(filename) {
        Ok(file) => file,
        Err(e) => {
            eprintln!(
                "ERROR: Unable to re-open file {} for printing (deleted during proccessing): {}",
                filename, e
            );
            return;
        }
    };
    let mut reader = BufReader::new(file);
    let mut line_number = 0;
    let end = start_line + count;

    while line_number < end {
        let mut buf: Vec<u8> = vec![];
        match reader.read_until(0xA, &mut buf) {
            Ok(num_bytes) => {
                if num_bytes == 0 {
                    break;
                } else if line_number >= start_line {
                    print!("{}", String::from_utf8_lossy(&buf));
                }

                line_number += 1;
            }
            Err(e) => {
                eprintln!("WARNING: Error processing file {} reason {}", filename, e);
                break;
            }
        }
    }
}

/// Display the output as text or structured JSON.
fn print_report(
    printable_results: &[Collision],
    opts: &Options,
    ignore_hashes: &HashMap<u64, bool>,
) {
    let mut num_lines: u64 = 0;
    let mut ignored: u64 = 0;
    let file_lookup_locked = match FILE_LOOKUP.lock() {
        Ok(lock) => lock,
        Err(e) => {
            eprintln!("ERROR: Failed to acquire lock on FILE_LOOKUP: {}", e);
            return;
        }
    };

    for p in printable_results.iter() {
        if ignore_hashes.contains_key(&p.key) {
            ignored += 1;
        } else {
            num_lines += (p.num_lines as usize * (p.start_lines.len() - 1)) as u64;

            if !opts.json {
                println!(
                    "{}\nHash signature = {}\nFound {} copy & pasted lines in the following files:",
                    "*".repeat(80),
                    p.key,
                    p.num_lines
                );

                for spec_file in &p.start_lines {
                    let filename = file_lookup_locked.id_to_name(spec_file.file_id);
                    let start_line = spec_file.line_number;
                    let end_line = start_line + p.num_lines;
                    println!(
                        "Between lines {} and {} in {}",
                        start_line + 1,
                        end_line,
                        filename
                    );
                }

                if opts.print {
                    print_dup_text(
                        &file_lookup_locked.id_to_name(p.start_lines[0usize].file_id),
                        p.start_lines[0usize].line_number as usize,
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
            file_lookup_locked.number_files(),
            ignored
        )
    } else {
        let r = ReportResults {
            num_lines,
            num_ignored: ignored,
            duplicates: printable_results,
        };
        match serde_json::to_string_pretty(&r) {
            Ok(json) => println!("{}", json),
            Err(e) => {
                eprintln!("ERROR: Failed to serialize results to JSON: {}", e);
                process::exit(1);
            }
        }
    }
}

/// When we have more than one region of text that matches another we will walk all combination
/// of matching text and see if we actually have a bigger overlap of texts.  When we do we will
/// store in in the results hash.
fn walk_collision(
    collisions: &[LineId],
    file_hashes: &[Vec<u64>],
    min_lines: u32,
    results_hash: &DashMap<u64, Collision>,
) {
    for (i, l_id) in collisions[0..(collisions.len() - 1)].iter().enumerate() {
        for r_id in &collisions[i + 1..] {
            if let Some(coll) = maximize_collision(file_hashes, l_id, r_id, min_lines) {
                match results_hash.entry(coll.key) {
                    Entry::Occupied(mut o) => o.get_mut().start_lines.extend(coll.start_lines),
                    Entry::Vacant(o) => {
                        o.insert(coll);
                    }
                }
            }
        }
    }
}

/// At this point in time we have a vector of vectors which contains the line hash signatures and
/// we have also calculated the rolling hash signatures for each file and registered them in the
/// collision_hash.  We now remove any hash entries where the value for the key is 1 and for all
/// the others we will try to determine the maximum size of the collision, aka. the duplicated
/// text number of lines.
fn find_collisions(
    collision_hash: DashMap<u64, Vec<LineId>>,
    file_hashes: &mut [Vec<u64>],
    opts: &Options,
) -> DashMap<u64, Collision> {
    let results_hash: DashMap<u64, Collision> = DashMap::new();

    // We have processed all the files, remove entries for which we didn't have any collisions
    // to reduce memory consumption.  Leveraging internals of dashmap to make this work with
    // multiple threads.
    collision_hash
        .shards()
        .iter()
        .par_bridge()
        .for_each(|s| s.write().retain(|_, v| v.get().len() > 1));
    collision_hash.shrink_to_fit();

    let collision_vec: Vec<Vec<LineId>> = collision_hash.into_iter().map(|(_, v)| v).collect();

    collision_vec
        .par_iter()
        .for_each(|e| walk_collision(e, file_hashes, opts.lines, &results_hash));

    results_hash
}

/// We have all the data, we now need to do some sorting and duplicate removals and then
/// dump the end data.
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
            if let std::collections::hash_map::Entry::Vacant(e) = chunk_processed.entry(cs) {
                e.insert(true);
                printable_results.push(ea);
            }
        }
    }

    printable_results.par_sort_unstable_by(|a, b| {
        a.num_lines
            .cmp(&b.num_lines)
            .then_with(|| {
                a.start_lines[0]
                    .line_number
                    .cmp(&b.start_lines[0].line_number)
            })
            .then_with(|| a.start_lines[0].file_id.cmp(&b.start_lines[0].file_id))
    });

    print_report(&printable_results, opts, ignore_hashes);
}

/// Open the user supplied file which contains the hash signatures for text that we don't
/// want to report on.
fn get_ignore_hashes(file_name: &str) -> HashMap<u64, bool> {
    let mut ignores: HashMap<u64, bool> = HashMap::new();

    let fh = File::open(file_name);

    match fh {
        Ok(fh) => {
            let buf = BufReader::new(fh);

            for line in buf.lines() {
                let t = match line {
                    Ok(line_content) => line_content,
                    Err(e) => {
                        eprintln!(
                            "WARNING: Error reading line from ignore file {}: {}",
                            file_name, e
                        );
                        continue;
                    }
                };
                let l = t.trim();

                if !l.is_empty() && !l.starts_with('#') {
                    if let Ok(hv) = l.parse::<u64>() {
                        ignores.insert(hv, true);
                    } else {
                        eprintln!("WARNING: Ignore file contains invalid hash value \"{}\"", l);
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct LineId {
    file_id: u32,
    line_number: u32,
}

/// Data structure which we use to store the count of how many files we have processed,
/// a vector of file name strings and a hash map which maps file name to integer.  We do this so
/// that we only have one copy of the file names in memory and use an integer to identify the
/// files though out the source code.  This reduces memory consumption significantly and also
/// results in file name compares becoming integer comparisons.
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

    /// Given a file name, if it doesn't already exist we will store the information about which
    /// index it is stored in and it's value.
    fn register_file(&mut self, file_name: Arc<String>) -> Option<u32> {
        if self.name_to_index.contains_key(&file_name) {
            return None;
        }
        let num = self.num_files;

        self.index_to_name.push(Arc::clone(&file_name));
        self.name_to_index
            .insert(Arc::clone(&file_name), self.num_files);
        if let Some(v) = self.num_files.checked_add(1) {
            self.num_files = v;
        } else {
            eprintln!("Number of files processed exceeds {}", u32::MAX);
            process::exit(2);
        }
        Some(num)
    }

    /// Given an id (integer) return the actual file name.
    fn id_to_name(&self, index: u32) -> Arc<String> {
        self.index_to_name[index as usize].clone()
    }

    /// Number of files we have information for.
    fn number_files(&self) -> u32 {
        self.num_files
    }
}

/// Get all files matching `file_globs` and update the global `FILE_LOOKUP`
fn files_to_process(file_globs: &[String]) -> Vec<(u32, Arc<String>)> {
    let mut files_to_process = Vec::new();
    // Hold the lock on FILE_LOOKUP for the duration as we are single threaded here.
    let mut file_lookup_locked = match FILE_LOOKUP.lock() {
        Ok(lock) => lock,
        Err(e) => {
            eprintln!("ERROR: Failed to acquire lock on FILE_LOOKUP: {}", e);
            process::exit(1);
        }
    };

    for g in file_globs {
        let entries = match glob(g) {
            Ok(entries) => entries,
            Err(e) => {
                eprintln!("Bad glob pattern supplied '{}', error: {}", g, e);
                process::exit(1);
            }
        };
        for filename in entries {
            let specific_file = match filename {
                Ok(specific_file) => specific_file,
                Err(e) => {
                    eprintln!("Unable to process {:?}", e);
                    process::exit(1);
                }
            };
            if !specific_file.is_file() {
                continue;
            }
            let file_str_name = specific_file.to_string_lossy().to_string();

            match canonicalize(&file_str_name) {
                Ok(fn_ok) => {
                    let c_name_str = fn_ok.to_string_lossy();
                    let name = Arc::new(c_name_str.to_string());

                    if let Some(fid) = file_lookup_locked.register_file(Arc::clone(&name)) {
                        files_to_process.push((fid, Arc::clone(&name)));
                    }
                }
                Err(e) => {
                    eprintln!(
                        "WARNING: Unable to process file {}, reason {}",
                        file_str_name, e
                    );
                }
            }
        }
    }

    files_to_process
}

/// Command line options.
#[derive(Debug)]
pub struct Options {
    lines: u32,
    print: bool,
    json: bool,
    file_globs: Vec<String>,
    ignore: String,
    threads: usize,
}

/// Default values for the command line options.
impl Default for Options {
    fn default() -> Options {
        Options {
            lines: 6,
            print: false,
            json: false,
            file_globs: vec![],
            ignore: "".to_string(),
            threads: 4,
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
        .arg(
            't',
            "threads",
            "number of threads to utilize. Set to 0 to match #cpu cores",
            &mut opts.threads,
            Some("<thread number>"),
            false,
        )?
        .done()?;

    if parser.wants_help() {
        parser.print_help();
    } else {
        let results_hash: DashMap<u64, Collision>;
        let mut ignore_hash: HashMap<u64, bool> = HashMap::new();

        // Dashmap scales well through ~3-4 threads, then stalls for our use case.
        if opts.threads != 0 {
            rayon::ThreadPoolBuilder::new()
                .num_threads(opts.threads)
                .build_global()
                .expect("Failed to build global thread pool");
        }

        {
            if !opts.ignore.is_empty() {
                ignore_hash = get_ignore_hashes(&opts.ignore);
            }

            let files_to_process: Vec<(u32, Arc<String>)> = files_to_process(&opts.file_globs);

            let collision_hashes: DashMap<u64, Vec<LineId>> = DashMap::new();
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

            results_hash = find_collisions(
                collision_hashes,
                &mut match file_hashes.lock() {
                    Ok(hashes) => hashes,
                    Err(e) => {
                        eprintln!("ERROR: Failed to acquire lock on file_hashes: {}", e);
                        process::exit(1);
                    }
                },
                &opts,
            );
        }

        process_report(results_hash, &opts, &ignore_hash);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

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
        use dashmap::DashMap;
        use std::sync::Mutex;

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
        let mut pattern_frequency = std::collections::HashMap::new();
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
}
