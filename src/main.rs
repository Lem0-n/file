use std::env;
use std::ffi::CString;
use std::io::{self, BufWriter, Write};
use std::os::fd::RawFd;
use std::os::unix::ffi::OsStrExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crossbeam_channel::Sender;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

const READ_BUF_SIZE: usize = 1 << 20;
const INLINE_SUBDIR_THRESHOLD: usize = 4;
const EMIT_FLUSH_THRESHOLD: usize = 256 * 1024;
const EMIT_BUF_CAP: usize = 512 * 1024;

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
    tx: Option<Sender<Vec<u8>>>,
}

impl ScanContext {
    #[inline]
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

#[inline]
fn enqueue_line(
    buf: &mut Vec<u8>,
    tx: &Sender<Vec<u8>>,
    base: &[u8],
    has_trailing: bool,
    name: &[u8],
) {
    buf.extend_from_slice(base);
    if !has_trailing {
        buf.push(b'/');
    }
    buf.extend_from_slice(name);
    buf.push(b'\n');

    if buf.len() >= EMIT_FLUSH_THRESHOLD {
        let mut out = Vec::with_capacity(EMIT_BUF_CAP);
        std::mem::swap(buf, &mut out);
        let _ = tx.send(out);
    }
}

fn scan_dir(path: PathBuf, ctx: ScanContext) {
    let mut local_files = 0_u64;
    let mut local_dirs = 0_u64;
    let mut local_skipped_errors = 0_u64;
    let mut discovered_subdirs: Vec<PathBuf> = Vec::with_capacity(16);
    let mut emit_buf = if ctx.tx.is_some() {
        Vec::with_capacity(EMIT_BUF_CAP)
    } else {
        Vec::new()
    };

    let base_bytes = path.as_os_str().as_bytes();
    let has_trailing = base_bytes.ends_with(b"/");

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
                if let Some(tx) = &ctx.tx {
                    enqueue_line(&mut emit_buf, tx, base_bytes, has_trailing, name);
                }

                match d_type {
                    libc::DT_DIR => {
                        local_dirs += 1;
                        let mut child = path.clone();
                        child.push(std::ffi::OsStr::from_bytes(name));
                        discovered_subdirs.push(child);
                    }
                    _ => local_files += 1,
                }
            }

            bpos += reclen;
        }
    }

    unsafe {
        libc::close(fd);
    }

    if let Some(tx) = &ctx.tx {
        if !emit_buf.is_empty() {
            let _ = tx.send(emit_buf);
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

pub fn scan_tree_max_speed(root: PathBuf, emit_paths: bool) -> ScanResult {
    let started = Instant::now();

    let (tx, writer_thread) = if emit_paths {
        let (tx, rx) = crossbeam_channel::unbounded::<Vec<u8>>();
        let t = std::thread::spawn(move || {
            let stdout = io::stdout();
            let mut out = BufWriter::with_capacity(4 * 1024 * 1024, stdout.lock());
            while let Ok(chunk) = rx.recv() {
                let _ = out.write_all(&chunk);
            }
            let _ = out.flush();
        });
        (Some(tx), Some(t))
    } else {
        (None, None)
    };

    let files = Arc::new(AtomicU64::new(0));
    let dirs = Arc::new(AtomicU64::new(0));
    let skipped_errors = Arc::new(AtomicU64::new(0));

    let ctx = ScanContext {
        files: files.clone(),
        dirs: dirs.clone(),
        skipped_errors: skipped_errors.clone(),
        active_tasks: Arc::new(AtomicU64::new(1)),
        done: Arc::new((Mutex::new(false), Condvar::new())),
        tx,
    };

    let root_ctx = ctx.clone();
    rayon::spawn(move || scan_dir(root, root_ctx));

    {
        let (lock, cv) = &*ctx.done;
        let mut done = lock.lock().expect("done mutex poisoned");
        while !*done {
            done = cv.wait(done).expect("done mutex poisoned while waiting");
        }
    }

    drop(ctx);
    if let Some(t) = writer_thread {
        let _ = t.join();
    }

    ScanResult {
        files: files.load(Ordering::Relaxed),
        dirs: dirs.load(Ordering::Relaxed),
        skipped_errors: skipped_errors.load(Ordering::Relaxed),
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

fn parse_emit_paths() -> bool {
    env::var_os("FASTSCAN_PRINT_PATHS").is_some()
}

fn main() {
    let root = parse_root();
    let emit_paths = parse_emit_paths();

    let result = scan_tree_max_speed(root.clone(), emit_paths);
    let total = result.files + result.dirs;

    eprintln!("root: {}", root.display());
    eprintln!("files: {}", result.files);
    eprintln!("dirs: {}", result.dirs);
    eprintln!("total: {}", total);
    eprintln!("skipped_errors: {}", result.skipped_errors);
    eprintln!("elapsed_ms: {:.3}", result.elapsed.as_secs_f64() * 1_000.0);
}
