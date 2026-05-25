use std::collections::VecDeque;
use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub struct ScanResult {
    pub files: u64,
    pub dirs: u64,
    pub elapsed: Duration,
}

pub async fn scan_tree_async(root: &Path) -> io::Result<ScanResult> {
    let started = Instant::now();
    let mut files = 0_u64;
    let mut dirs = 0_u64;

    let mut queue = VecDeque::with_capacity(256);
    queue.push_back(root.to_path_buf());

    while let Some(dir) = queue.pop_front() {
        let mut rd = tokio::fs::read_dir(&dir).await?;

        while let Some(entry) = rd.next_entry().await? {
            let ft = entry.file_type().await?;
            if ft.is_dir() {
                dirs += 1;
                queue.push_back(entry.path());
            } else {
                files += 1;
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
    let _ = args.next();
    args.next()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn main() {
    let root = parse_root();

    let outcome = tokio_uring::start(async {
        let result = scan_tree_async(&root).await;
        (root, result)
    });

    match outcome {
        (root, Ok(result)) => {
            let total = result.files + result.dirs;
            println!("root: {}", root.display());
            println!("files: {}", result.files);
            println!("dirs: {}", result.dirs);
            println!("total: {}", total);
            println!("elapsed_ms: {:.3}", result.elapsed.as_secs_f64() * 1_000.0);
        }
        (root, Err(err)) => {
            eprintln!("scan failed for {}: {err}", root.display());
            std::process::exit(1);
        }
    }
}
