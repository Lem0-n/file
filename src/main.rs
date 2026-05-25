use std::env;
use std::fs::{self, ReadDir};
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct ScanResult {
    pub files: u64,
    pub dirs: u64,
    pub elapsed: Duration,
}

/// Iterative DFS to minimize allocator pressure and recursion overhead.
///
/// Notes on performance:
/// - Uses `DirEntry::file_type()` to avoid extra `metadata` syscalls.
/// - Keeps traversal iterative with a `Vec<ReadDir>` stack.
/// - Counts every discovered file and directory under `root`.
pub fn scan_tree(root: &Path) -> io::Result<ScanResult> {
    let started = Instant::now();

    let mut files = 0_u64;
    let mut dirs = 0_u64;

    let mut stack: Vec<ReadDir> = Vec::with_capacity(64);
    stack.push(fs::read_dir(root)?);

    while let Some(top) = stack.last_mut() {
        match top.next() {
            Some(Ok(entry)) => {
                let ft = entry.file_type()?;
                if ft.is_dir() {
                    dirs += 1;
                    stack.push(fs::read_dir(entry.path())?);
                } else {
                    files += 1;
                }
            }
            Some(Err(e)) => return Err(e),
            None => {
                stack.pop();
            }
        }
    }

    Ok(ScanResult {
        files,
        dirs,
        elapsed: started.elapsed(),
    })
}

fn parse_root() -> PathBuf {
    let mut args = env::args_os();
    let _bin = args.next();
    match args.next() {
        Some(path) => PathBuf::from(path),
        None => PathBuf::from("."),
    }
}

fn main() {
    let root = parse_root();

    match scan_tree(&root) {
        Ok(result) => {
            let total = result.files + result.dirs;
            println!("root: {}", root.display());
            println!("files: {}", result.files);
            println!("dirs: {}", result.dirs);
            println!("total: {}", total);
            println!("elapsed_ms: {:.3}", result.elapsed.as_secs_f64() * 1_000.0);
        }
        Err(err) => {
            eprintln!("scan failed for {}: {err}", root.display());
            std::process::exit(1);
        }
    }
}
