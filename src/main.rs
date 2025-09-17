// SPDX-License-Identifier: GPL-3.0-only
//
// Copyright (C) 2019-2023 Tony Asleson <tony.asleson@gmail.com>

use duplihere::*;
use rags_rs::argparse;
use rayon::ThreadPoolBuilder;

static LONG_DESC: &str = "Find duplicate lines of text in one or more text files.

The duplicated text can be at different levels of indention,
but otherwise needs to be identical.

More information: https://github.com/tasleson/duplihere";

fn main() -> std::result::Result<(), rags_rs::Error> {
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
        let mut ignore_hash = std::collections::HashMap::new();

        // Dashmap scales well through ~3-4 threads, then stalls for our use case.
        if opts.threads != 0 {
            ThreadPoolBuilder::new()
                .num_threads(opts.threads)
                .build_global()
                .expect("Failed to build global thread pool");
        }

        if !opts.ignore.is_empty() {
            ignore_hash = match get_ignore_hashes(&opts.ignore) {
                Ok(hashes) => hashes,
                Err(e) => {
                    eprintln!("ERROR: Failed to load ignore hashes: {}", e);
                    std::process::exit(2);
                }
            };
        }

        let results_hash = match process_files(&opts) {
            Ok(results) => results,
            Err(e) => {
                eprintln!("ERROR: Failed to process files: {}", e);
                std::process::exit(1);
            }
        };
        process_report(results_hash, &opts, &ignore_hash);
    }

    Ok(())
}
