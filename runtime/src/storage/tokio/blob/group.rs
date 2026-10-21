//! Filesystem group commit: one `syncfs` covers every blob barrier requested while it waited.
//!
//! Enabled with `COMMONWARE_STORAGE_SYNC=syncfs` (Linux only). A barrier must be covered by a
//! `syncfs` that starts after the barrier was requested (so it covers the caller's completed
//! writes). At most one `syncfs` per filesystem runs at a time; barriers that arrive while one
//! is in flight share the next one. This turns many per-file flushes per consensus step into
//! one device flush, at the cost of flushing every dirty file on the filesystem.
//!
//! A failed `syncfs` poisons the filesystem's group permanently: the kernel may already have
//! consumed the writeback error, so no later barrier can claim durability.

use std::{
    collections::HashMap,
    fs::File,
    io,
    os::{fd::AsRawFd, unix::fs::MetadataExt},
    sync::{Arc, Condvar, Mutex, OnceLock},
};

/// Whether barriers use filesystem group commit instead of per-file `fdatasync`.
pub(super) fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("COMMONWARE_STORAGE_SYNC").as_deref() == Ok("syncfs"))
}

#[derive(Default)]
struct State {
    /// `syncfs` calls started.
    started: u64,
    /// Highest started call that has completed.
    done: u64,
    in_flight: bool,
    failed: Option<io::ErrorKind>,
}

#[derive(Default)]
struct Group {
    state: Mutex<State>,
    finished: Condvar,
}

fn group(file: &File) -> io::Result<Arc<Group>> {
    static GROUPS: OnceLock<Mutex<HashMap<u64, Arc<Group>>>> = OnceLock::new();
    let device = file.metadata()?.dev();
    Ok(GROUPS.get_or_init(Default::default).lock().unwrap().entry(device).or_default().clone())
}

/// Make every completed write to `file` durable through a shared `syncfs`.
pub(super) fn barrier(file: &File) -> io::Result<()> {
    let group = group(file)?;
    let mut state = group.state.lock().unwrap();
    // A call already in flight may have started before our writes completed.
    let needed = state.started + 1;
    loop {
        if let Some(kind) = state.failed {
            return Err(io::Error::new(kind, "filesystem group commit failed"));
        }
        if state.done >= needed {
            return Ok(());
        }
        if state.in_flight {
            state = group.finished.wait(state).unwrap();
            continue;
        }
        state.in_flight = true;
        state.started += 1;
        let call = state.started;
        drop(state);
        // SAFETY: `file` owns a valid fd for the duration of the call; `syncfs` reads no memory.
        let result = if unsafe { libc::syncfs(file.as_raw_fd()) } == -1 { Err(io::Error::last_os_error()) } else { Ok(()) };
        state = group.state.lock().unwrap();
        state.in_flight = false;
        state.done = call;
        if let Err(error) = &result {
            state.failed = Some(error.kind());
        }
        group.finished.notify_all();
        if result.is_err() {
            return result;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{io::Write, thread};

    #[test]
    fn concurrent_barriers_share_calls_and_complete() {
        let dir = std::env::temp_dir().join(format!("group-commit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let handles: Vec<_> = (0..16)
            .map(|i| {
                let path = dir.join(format!("f{i}"));
                thread::spawn(move || {
                    let mut file = File::create(path).unwrap();
                    for _ in 0..20 {
                        file.write_all(b"data").unwrap();
                        barrier(&file).unwrap();
                    }
                })
            })
            .collect();
        for handle in handles {
            handle.join().unwrap();
        }
        let file = File::open(dir.join("f0")).unwrap();
        let group = group(&file).unwrap();
        let state = group.state.lock().unwrap();
        assert!(!state.in_flight);
        assert_eq!(state.started, state.done);
        assert!(state.started <= 16 * 20);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
