// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! `shutdown()` must remove a temporary database's scratch directory.
//!
//! An `in_memory()` database is only in-memory from the caller's side: it is
//! backed by a `uni_mem_*` directory. Removal was left entirely to `TempDir`'s
//! `Drop`, which runs `remove_dir_all` and **discards the error** — and
//! `remove_dir_all` is not atomic, so a background manifest write landing
//! between the directory walk and the final `rmdir` fails with `ENOTEMPTY` and
//! the directory survives silently.
//!
//! Measured downstream at ~3-6% of shutdowns. The survivors never expire, so a
//! suite opening thousands of databases strands tens per run; on a tmpfs
//! `TMPDIR` that exhausts *inodes* rather than space, and then presents as
//! unrelated "failed to create temporary directory" errors while `df -h` shows
//! the disk nearly empty.
//!
//! Asserted on the observable property — nothing left behind — rather than on an
//! internal path, because that is the thing that actually costs a user.

use uni_db::Uni;

#[tokio::test]
async fn shutdown_leaves_no_scratch_directory_behind() -> anyhow::Result<()> {
    // A private TMPDIR, so a concurrent test's databases cannot be miscounted.
    // Safe here: nextest runs each test in its own process.
    let root = tempfile::tempdir()?;
    unsafe { std::env::set_var("TMPDIR", root.path()) };

    // Enough cycles that a few-percent leak is overwhelmingly likely to show.
    for _ in 0..40 {
        let db = Uni::in_memory().build().await?;
        db.shutdown().await?;
    }

    let stranded: Vec<String> = std::fs::read_dir(root.path())?
        .filter_map(std::result::Result::ok)
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("uni_mem_"))
        .collect();
    assert!(
        stranded.is_empty(),
        "shutdown left {} scratch directories behind: {stranded:?}",
        stranded.len()
    );
    Ok(())
}
