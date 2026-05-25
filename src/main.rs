use std::env;
use std::ffi::CString;
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const READ_BUF_SIZE: usize = 1 << 20;
const INLINE_SUBDIR_THRESHOLD: usize = 4;

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

#[inline]
fn open_dir(path: &PathBuf) -> Option<RawFd> {
    let bytes = path.as_os_str().as_encoded_bytes();
    let c_path = CString::new(bytes).ok()?;
    let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC;
    let fd = unsafe { libc::open(c_path.as_ptr(), flags) };
    (fd >= 0).then_some(fd)
}

fn scan_dir(path: PathBuf, ctx: ScanContext) {
    let mut local_files = 0_u64;
    let mut local_dirs = 0_u64;
    let mut local_skipped_errors = 0_u64;
    let mut discovered_subdirs: Vec<PathBuf> = Vec::with_capacity(16);

    let Some(fd) = open_dir(&path) else {
        ctx.skipped_errors.fetch_add(1, Ordering::Relaxed);
        ctx.finish_task();
        return;
    };

    let mut buf = vec![0_u8; READ_BUF_SIZE];

    loop {
        let nread = unsafe {
            libc::syscall(
                libc::SYS_getdents64,
                fd,
                buf.as_mut_ptr() as *mut libc::c_void,
                buf.len(),
            )
        };

        if nread == 0 {
            break;
        }
        if nread < 0 {
            local_skipped_errors += 1;
            break;
        }

        let nread = nread as usize;
        let mut bpos = 0_usize;
        while bpos < nread {
            let base = unsafe { buf.as_ptr().add(bpos) };
            let reclen = unsafe { *(base.add(16) as *const u16) } as usize;
            if reclen == 0 || bpos + reclen > nread {
                local_skipped_errors += 1;
                break;
            }

            let d_type = unsafe { *base.add(18) };
            let name_ptr = unsafe { base.add(19) };
            let name_len = reclen.saturating_sub(19);
            let name_slice = unsafe { std::slice::from_raw_parts(name_ptr, name_len) };
            let nul_pos = name_slice
                .iter()
                .position(|&c| c == 0)
                .unwrap_or(name_slice.len());
            let name = &name_slice[..nul_pos];

            if name != b"." && name != b".." {
                match d_type {
                    libc::DT_DIR => {
                        local_dirs += 1;
                        let mut child = path.clone();
                        child.push(std::ffi::OsStr::from_bytes(name));
                        discovered_subdirs.push(child);
                    }
                    libc::DT_LNK => {
                        local_files += 1;
                    }
                    libc::DT_UNKNOWN => {
                        local_files += 1;
                    }
                    _ => {
                        local_files += 1;
                    }
                }
            }

            bpos += reclen;
        }
    }

    unsafe {
        libc::close(fd);
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

    if discovered_subdirs.len() <= INLINE_SUBDIR_THRESHOLD {
        for p in discovered_subdirs {
            ctx.active_tasks.fetch_add(1, Ordering::Release);
            scan_dir(p, ctx.clone());
        }
    } else {
        for p in discovered_subdirs {
            ctx.active_tasks.fetch_add(1, Ordering::Release);
            let child_ctx = ctx.clone();
            rayon::spawn(move || scan_dir(p, child_ctx));
        }
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
