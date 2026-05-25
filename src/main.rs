use std::collections::VecDeque;
use std::env;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use futures::stream::{FuturesUnordered, StreamExt};

#[derive(Debug, Clone, Copy)]
pub struct ScanResult {
    pub files: u64,
    pub dirs: u64,
    pub skipped_errors: u64,
    pub elapsed: Duration,
}

#[derive(Default)]
struct BatchResult {
    files: u64,
    dirs: u64,
    next_dirs: Vec<PathBuf>,
    skipped_errors: u64,
}

fn scan_single_dir(dir: PathBuf) -> BatchResult {
    let mut out = BatchResult::default();

    let read_dir = match fs::read_dir(&dir) {
        Ok(rd) => rd,
        Err(_) => {
            out.skipped_errors += 1;
            return out;
        }
    };

    for entry_res in read_dir {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(_) => {
                out.skipped_errors += 1;
                continue;
            }
        };

        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => {
                out.skipped_errors += 1;
                continue;
            }
        };

        if ft.is_dir() {
            out.dirs += 1;
            out.next_dirs.push(entry.path());
        } else {
            out.files += 1;
        }
    }

    out
}

pub async fn scan_tree_async(root: &Path, concurrency: usize) -> io::Result<ScanResult> {
    let started = Instant::now();

    let mut files = 0_u64;
    let mut dirs = 0_u64;
    let mut skipped_errors = 0_u64;

    let mut pending_dirs = VecDeque::with_capacity(256);
    pending_dirs.push_back(root.to_path_buf());

    let mut inflight = FuturesUnordered::new();
    let max_inflight = concurrency.max(1);

    loop {
        while inflight.len() < max_inflight {
            let Some(dir) = pending_dirs.pop_front() else {
                break;
            };

            inflight.push(tokio_uring::spawn(async move { scan_single_dir(dir) }));
        }

        let Some(joined) = inflight.next().await else {
            break;
        };

        let batch = joined.map_err(|e| io::Error::other(format!("task join failed: {e}")))?;

        files += batch.files;
        dirs += batch.dirs;
        skipped_errors += batch.skipped_errors;
        pending_dirs.extend(batch.next_dirs);
    }

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

fn parse_concurrency() -> usize {
    env::var("FASTSCAN_CONCURRENCY")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(64)
        })
}

fn main() {
    let root = parse_root();
    let concurrency = parse_concurrency();

    let outcome = tokio_uring::start(async {
        let result = scan_tree_async(&root, concurrency).await;
        (root, result)
    });

    match outcome {
        (root, Ok(result)) => {
            let total = result.files + result.dirs;
            println!("root: {}", root.display());
            println!("concurrency: {}", concurrency);
            println!("files: {}", result.files);
            println!("dirs: {}", result.dirs);
            println!("total: {}", total);
            println!("skipped_errors: {}", result.skipped_errors);
            println!("elapsed_ms: {:.3}", result.elapsed.as_secs_f64() * 1_000.0);
        }
        (root, Err(err)) => {
            eprintln!("scan failed for {}: {err}", root.display());
            std::process::exit(1);
        }
    }
}
