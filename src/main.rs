use std::env;
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Clone, Copy)]
pub struct ScanResult {
    pub files: u64,
    pub dirs: u64,
    pub skipped_errors: u64,
    pub elapsed: Duration,
}

#[derive(Clone)]
struct ScanContext {
    files: Arc<AtomicU64>,
    dirs: Arc<AtomicU64>,
    skipped_errors: Arc<AtomicU64>,
    active_tasks: Arc<AtomicU64>,
    done: Arc<(Mutex<bool>, Condvar)>,
}

impl ScanContext {
    fn finish_task(&self) {
        if self.active_tasks.fetch_sub(1, Ordering::AcqRel) == 1 {
            let (lock, cv) = &*self.done;
            let mut done = lock.lock().expect("done mutex poisoned");
            *done = true;
            cv.notify_one();
        }
    }
}

fn scan_dir(path: PathBuf, ctx: ScanContext) {
    let mut local_files = 0_u64;
    let mut local_dirs = 0_u64;
    let mut local_skipped_errors = 0_u64;

    let entries = match fs::read_dir(&path) {
        Ok(entries) => entries,
        Err(_) => {
            local_skipped_errors += 1;
            ctx.skipped_errors
                .fetch_add(local_skipped_errors, Ordering::Relaxed);
            ctx.finish_task();
            return;
        }
    };

    for entry_res in entries {
        let entry = match entry_res {
            Ok(entry) => entry,
            Err(_) => {
                local_skipped_errors += 1;
                continue;
            }
        };

        let ft = match entry.file_type() {
            Ok(ft) => ft,
            Err(_) => {
                local_skipped_errors += 1;
                continue;
            }
        };

        if ft.is_dir() {
            local_dirs += 1;
            ctx.active_tasks.fetch_add(1, Ordering::Release);
            let child_ctx = ctx.clone();
            let child_path = entry.path();
            rayon::spawn(move || scan_dir(child_path, child_ctx));
        } else {
            local_files += 1;
        }
    }

    if local_files > 0 {
        ctx.files.fetch_add(local_files, Ordering::Relaxed);
    }
    if local_dirs > 0 {
        ctx.dirs.fetch_add(local_dirs, Ordering::Relaxed);
    }
    if local_skipped_errors > 0 {
        ctx.skipped_errors
            .fetch_add(local_skipped_errors, Ordering::Relaxed);
    }

    ctx.finish_task();
}

pub fn scan_tree_max_speed(root: PathBuf) -> ScanResult {
    let started = Instant::now();

    let ctx = ScanContext {
        files: Arc::new(AtomicU64::new(0)),
        dirs: Arc::new(AtomicU64::new(0)),
        skipped_errors: Arc::new(AtomicU64::new(0)),
        active_tasks: Arc::new(AtomicU64::new(1)),
        done: Arc::new((Mutex::new(false), Condvar::new())),
    };

    let root_ctx = ctx.clone();
    rayon::spawn(move || scan_dir(root, root_ctx));

    let (lock, cv) = &*ctx.done;
    let mut done = lock.lock().expect("done mutex poisoned");
    while !*done {
        done = cv.wait(done).expect("done mutex poisoned while waiting");
    }

    ScanResult {
        files: ctx.files.load(Ordering::Relaxed),
        dirs: ctx.dirs.load(Ordering::Relaxed),
        skipped_errors: ctx.skipped_errors.load(Ordering::Relaxed),
        elapsed: started.elapsed(),
    }
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

    let result = scan_tree_max_speed(root.clone());
    let total = result.files + result.dirs;

    println!("root: {}", root.display());
    println!("files: {}", result.files);
    println!("dirs: {}", result.dirs);
    println!("total: {}", total);
    println!("skipped_errors: {}", result.skipped_errors);
    println!("elapsed_ms: {:.3}", result.elapsed.as_secs_f64() * 1_000.0);
}
