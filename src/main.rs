use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use rayon::iter::ParallelBridge;
use rayon::prelude::*;
use walkdir::WalkDir;

#[derive(Debug, Clone, Copy)]
pub struct ScanResult {
    pub files: u64,
    pub dirs: u64,
    pub skipped_errors: u64,
    pub elapsed: Duration,
}

pub fn scan_tree_parallel(root: &Path) -> io::Result<ScanResult> {
    let started = Instant::now();

    let walker = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .par_bridge();

    let (files, dirs, skipped_errors) = walker
        .map(|entry_res| match entry_res {
            Ok(entry) => {
                let ft = entry.file_type();
                if ft.is_file() {
                    (1_u64, 0_u64, 0_u64)
                } else if ft.is_dir() {
                    if entry.depth() == 0 {
                        (0_u64, 0_u64, 0_u64)
                    } else {
                        (0_u64, 1_u64, 0_u64)
                    }
                } else {
                    (1_u64, 0_u64, 0_u64)
                }
            }
            Err(_) => (0_u64, 0_u64, 1_u64),
        })
        .reduce(
            || (0_u64, 0_u64, 0_u64),
            |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2),
        );

    Ok(ScanResult {
        files,
        dirs,
        skipped_errors,
        elapsed: started.elapsed(),
    })
}

fn parse_root() -> PathBuf {
    let mut args = env::args_os();
    let _ = args.next();
    args.next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn main() {
    let root = parse_root();

    match scan_tree_parallel(&root) {
        Ok(result) => {
            let total = result.files + result.dirs;
            println!("root: {}", root.display());
            println!("files: {}", result.files);
            println!("dirs: {}", result.dirs);
            println!("total: {}", total);
            println!("skipped_errors: {}", result.skipped_errors);
            println!("elapsed_ms: {:.3}", result.elapsed.as_secs_f64() * 1_000.0);
        }
        Err(err) => {
            eprintln!("scan failed for {}: {err}", root.display());
            std::process::exit(1);
        }
    }
}
