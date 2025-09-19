// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

//! duplihere - Duplicate text finder library
//!
//! This library provides functionality to find duplicate sections of text
//! across one or more files by analyzing line signatures and rolling hashes.

// Edition 2021 - extern crate declarations are no longer needed for most cases
use glob::glob;
use rayon::prelude::*;

use serde::ser::SerializeStruct;
use serde::{Serialize, Serializer};

use std::collections::{hash_map::DefaultHasher, HashMap, VecDeque};
use std::error::Error;
use std::fmt;
use std::fs::{canonicalize, File};
use std::hash::{Hash, Hasher};
use std::io::{prelude::*, BufReader};
#[cfg(windows)]
use std::path::MAIN_SEPARATOR;
use std::process;
use std::sync::{Arc, Mutex};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use lazy_static::lazy_static;

lazy_static! {
    static ref FILE_LOOKUP: Mutex<FileId> = Mutex::new(FileId::new());
}

/// Custom error type for duplihere operations
#[derive(Debug)]
pub enum DupliError {
    Io(std::io::Error),
    Glob(glob::GlobError),
    Pattern(glob::PatternError),
    Mutex(String),
    Serialization(serde_json::Error),
    Parse(String),
    FileLookup(String),
}

impl fmt::Display for DupliError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DupliError::Io(e) => write!(f, "I/O error: {}", e),
            DupliError::Glob(e) => write!(f, "Glob error: {}", e),
            DupliError::Pattern(e) => write!(f, "Pattern error: {}", e),
            DupliError::Mutex(e) => write!(f, "Mutex error: {}", e),
            DupliError::Serialization(e) => write!(f, "Serialization error: {}", e),
            DupliError::Parse(e) => write!(f, "Parse error: {}", e),
            DupliError::FileLookup(e) => write!(f, "File lookup error: {}", e),
        }
    }
}

impl Error for DupliError {}

impl From<std::io::Error> for DupliError {
    fn from(error: std::io::Error) -> Self {
        DupliError::Io(error)
    }
}

impl From<glob::GlobError> for DupliError {
    fn from(error: glob::GlobError) -> Self {
        DupliError::Glob(error)
    }
}

impl From<glob::PatternError> for DupliError {
    fn from(error: glob::PatternError) -> Self {
        DupliError::Pattern(error)
    }
}

impl From<serde_json::Error> for DupliError {
    fn from(error: serde_json::Error) -> Self {
        DupliError::Serialization(error)
    }
}

pub type Result<T> = std::result::Result<T, DupliError>;

/// Normalize path separators for cross-platform compatibility
/// On Windows, this ensures consistent path representation
fn normalize_path_separators(path: &str) -> String {
    #[cfg(windows)]
    {
        path.replace('/', &MAIN_SEPARATOR.to_string())
    }
    #[cfg(not(windows))]
    {
        path.to_string()
    }
}

/// Generates the hash for 'T' which in this case is a utf-8 string.
pub fn calculate_hash<T: Hash>(t: T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

/// For a given file, walk it line by line calculating, removing leading and trailing WS and
/// calculating the signatures for each line, return the information as a vector of hash signatures.
pub fn file_signatures(filename: &str) -> Result<Vec<u64>> {
    let file = File::open(filename)?;
    let mut rc: Vec<u64> = Vec::new();
    let mut reader = BufReader::new(file);
    let mut buf: Vec<u8> = vec![];

    loop {
        let num_bytes = reader.read_until(b'\n', &mut buf)?;
        if num_bytes == 0 {
            break;
        } else {
            let l = String::from_utf8_lossy(&buf);
            rc.push(calculate_hash(l.trim()));
            buf.clear();
        }
    }

    Ok(rc)
}

/// For a specific file, calculate the hash signature for 'min_lines' in size using a sliding window
/// so that we can detect duplicate text of at least min_lines in size anywhere in each file.
/// Store the hash signature and start line in a vector of tuples which we will then register
/// in the collision hash.
pub fn rolling_hashes(file_signatures: &[u64], min_lines: usize) -> Vec<(u64, u32)> {
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
) -> Result<()> {
    let file_signatures = file_signatures(filename)?;
    let file_rolling_hashes = rolling_hashes(&file_signatures, min_lines);

    let mut hashes = file_hashes
        .lock()
        .map_err(|e| DupliError::Mutex(format!("Failed to acquire lock on file_hashes: {}", e)))?;
    hashes[file_id as usize] = file_signatures;
    drop(hashes); // Release lock early

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

    Ok(())
}

/// Used to record a section of duplicated text.  We store the hash signature, how many lines
/// match and a vector of file ids and the starting line in the file.
#[derive(Debug)]
pub struct Collision {
    pub key: u64,
    pub num_lines: u32,
    pub start_lines: Vec<LineId>,
    pub sig: u64,
}

/// Used to convert a collision in our results to JSON for it.
impl Serialize for Collision {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let file_lookup_lock = FILE_LOOKUP.lock().map_err(|e| {
            serde::ser::Error::custom(format!("Failed to acquire FILE_LOOKUP lock: {}", e))
        })?;
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
    pub fn scrub(&mut self) {
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
pub fn overlap(left: &LineId, right: &LineId, end: u32) -> bool {
    left.file_id == right.file_id
        && (left.line_number == right.line_number
            || (right.line_number >= left.line_number
                && right.line_number <= (left.line_number + end))
            || (left.line_number >= right.line_number
                && left.line_number <= (right.line_number + end)))
}

/// Find the largest number of matching lines by going line by line from a known duplication point
/// and recording it if it's bigger than the default number of matching lines
pub fn maximize_collision(
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
fn print_dup_text(filename: &str, start_line: usize, count: usize) -> Result<()> {
    let file = File::open(filename)?;
    let mut reader = BufReader::new(file);
    let mut line_number = 0;
    let end = start_line + count;

    while line_number < end {
        let mut buf: Vec<u8> = vec![];
        let num_bytes = reader.read_until(0xA, &mut buf)?;
        if num_bytes == 0 {
            break;
        } else if line_number >= start_line {
            print!("{}", String::from_utf8_lossy(&buf));
        }
        line_number += 1;
    }

    Ok(())
}

/// Display the output as text or structured JSON.
pub fn print_report(
    printable_results: &[Collision],
    opts: &Options,
    ignore_hashes: &HashMap<u64, bool>,
) -> Result<()> {
    let mut num_lines: u64 = 0;
    let mut ignored: u64 = 0;
    let file_lookup_locked = FILE_LOOKUP
        .lock()
        .map_err(|e| DupliError::Mutex(format!("Failed to acquire lock on FILE_LOOKUP: {}", e)))?;

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
                    if let Err(e) = print_dup_text(
                        &file_lookup_locked.id_to_name(p.start_lines[0usize].file_id),
                        p.start_lines[0usize].line_number as usize,
                        p.num_lines as usize,
                    ) {
                        eprintln!("WARNING: Failed to print duplicate text: {}", e);
                    }
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
        let json = serde_json::to_string_pretty(&r)?;
        println!("{}", json);
    }

    Ok(())
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
pub fn find_collisions(
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
pub fn process_report(
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

    if let Err(e) = print_report(&printable_results, opts, ignore_hashes) {
        eprintln!("ERROR: Failed to print report: {}", e);
        process::exit(1);
    }
}

/// Open the user supplied file which contains the hash signatures for text that we don't
/// want to report on.
pub fn get_ignore_hashes(file_name: &str) -> Result<HashMap<u64, bool>> {
    let mut ignores: HashMap<u64, bool> = HashMap::new();
    let fh = File::open(file_name)?;
    let buf = BufReader::new(fh);

    for line in buf.lines() {
        let t = line?;
        let l = t.trim();

        if !l.is_empty() && !l.starts_with('#') {
            match l.parse::<u64>() {
                Ok(hv) => {
                    ignores.insert(hv, true);
                }
                Err(_) => {
                    eprintln!("WARNING: Ignore file contains invalid hash value \"{}\"", l);
                }
            }
        }
    }

    Ok(ignores)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LineId {
    pub file_id: u32,
    pub line_number: u32,
}

/// Data structure which we use to store the count of how many files we have processed,
/// a vector of file name strings and a hash map which maps file name to integer.  We do this so
/// that we only have one copy of the file names in memory and use an integer to identify the
/// files though out the source code.  This reduces memory consumption significantly and also
/// results in file name compares becoming integer comparisons.
#[derive(Debug)]
pub struct FileId {
    pub num_files: u32,
    pub index_to_name: Vec<Arc<String>>,
    pub name_to_index: HashMap<Arc<String>, u32>,
}

impl Default for FileId {
    fn default() -> Self {
        Self::new()
    }
}

impl FileId {
    pub fn new() -> FileId {
        FileId {
            num_files: 0,
            index_to_name: vec![],
            name_to_index: HashMap::new(),
        }
    }

    /// Given a file name, if it doesn't already exist we will store the information about which
    /// index it is stored in and it's value.
    pub fn register_file(&mut self, file_name: Arc<String>) -> Option<u32> {
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
    pub fn id_to_name(&self, index: u32) -> Arc<String> {
        self.index_to_name[index as usize].clone()
    }

    /// Number of files we have information for.
    pub fn number_files(&self) -> u32 {
        self.num_files
    }
}

/// Get all files matching `file_globs` and update the global `FILE_LOOKUP`
pub fn files_to_process(file_globs: &[String]) -> Result<Vec<(u32, Arc<String>)>> {
    let mut files_to_process = Vec::new();
    // Hold the lock on FILE_LOOKUP for the duration as we are single threaded here.
    let mut file_lookup_locked = FILE_LOOKUP
        .lock()
        .map_err(|e| DupliError::Mutex(format!("Failed to acquire lock on FILE_LOOKUP: {}", e)))?;

    for g in file_globs {
        let entries = glob(g)?;
        for filename in entries {
            let specific_file = filename?;
            if !specific_file.is_file() {
                continue;
            }
            let file_str_name = specific_file.to_string_lossy().to_string();

            match canonicalize(&file_str_name) {
                Ok(fn_ok) => {
                    // Normalize path separators for cross-platform compatibility
                    let c_name_str = fn_ok.to_string_lossy();
                    let normalized_path = normalize_path_separators(&c_name_str);
                    let name = Arc::new(normalized_path);

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

    Ok(files_to_process)
}

/// Command line options.
#[derive(Debug)]
pub struct Options {
    pub lines: u32,
    pub print: bool,
    pub json: bool,
    pub file_globs: Vec<String>,
    pub ignore: String,
    pub threads: usize,
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

/// Process files and find duplicate sections
pub fn process_files(opts: &Options) -> Result<DashMap<u64, Collision>> {
    let files_to_process = files_to_process(&opts.file_globs)?;

    let collision_hashes: DashMap<u64, Vec<LineId>> = DashMap::new();
    let file_hashes: Mutex<Vec<Vec<u64>>> = Mutex::new(vec![vec![0; 0]; files_to_process.len()]);

    // Process files in parallel and log any errors
    files_to_process.par_iter().for_each(|e| {
        if let Err(err) = process_file(
            e.0,
            &e.1,
            opts.lines as usize,
            &file_hashes,
            &collision_hashes,
        ) {
            eprintln!("WARNING: Failed to process file {}: {}", e.1, err);
        }
    });

    let mut hashes = file_hashes
        .lock()
        .map_err(|e| DupliError::Mutex(format!("Failed to acquire lock on file_hashes: {}", e)))?;

    Ok(find_collisions(collision_hashes, &mut hashes, opts))
}
