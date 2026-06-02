// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Atomic JSON sidecar for `_system/` state files.
//!
//! Several uni-db subsystems persist a small document under
//! `<data_path>/_system/<name>.json` — the CDC checkpoint table, the deferred-
//! trigger queue, the background-job scheduler (in `uni-plugin-host`), and the
//! declared-plugins registry (in `uni-plugin-custom`). They all want the same
//! thing: load the whole document at startup (treating a missing or empty file
//! as "nothing yet"), and replace it atomically on every write. This crate is
//! the single, correct implementation of that pattern, shared so no subsystem
//! re-rolls it.
//!
//! "Atomic" here means write-to-temp, `fsync` the temp file, then `rename` over
//! the target. Crucially it also `fsync`s the *parent directory* after the
//! rename: on POSIX a rename is not crash-durable until the directory entry is
//! flushed, so without it a power loss can leave the file reverted to its
//! pre-rename contents even though the data was synced.
//!
//! [`SystemSidecar`] handles only the IO. Higher-level concerns — write
//! serialization (a mutex spanning read-modify-write), a best-effort Cypher
//! mirror, per-row re-binding — stay with the callers, which compose them
//! around [`SystemSidecar::load`] / [`SystemSidecar::store`].

// Rust guideline compliant

use std::fs::File;
use std::io::Write;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde::de::DeserializeOwned;

/// Error raised by [`SystemSidecar`] load/store operations.
///
/// Each variant carries the path it was operating on so callers can surface a
/// useful diagnostic when they convert into their own error type.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum SidecarIoError {
    /// Creating the `_system/` parent directory failed.
    #[error("system sidecar create dir {path:?}: {source}")]
    CreateDir {
        /// Directory whose creation failed.
        path: PathBuf,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// Reading the sidecar file failed.
    #[error("system sidecar read {path:?}: {source}")]
    Read {
        /// File being read.
        path: PathBuf,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// Writing the temp file, renaming, or fsyncing failed.
    #[error("system sidecar write {path:?}: {source}")]
    Write {
        /// File or directory being written / synced.
        path: PathBuf,
        /// Underlying IO error.
        source: std::io::Error,
    },
    /// Serializing the document to JSON failed.
    #[error("system sidecar encode {path:?}: {source}")]
    Encode {
        /// Target file the document was destined for.
        path: PathBuf,
        /// Underlying serialization error.
        source: serde_json::Error,
    },
    /// Parsing the sidecar file as JSON failed.
    #[error("system sidecar decode {path:?}: {source}")]
    Decode {
        /// File being parsed.
        path: PathBuf,
        /// Underlying deserialization error.
        source: serde_json::Error,
    },
}

/// Build a [`SidecarIoError::Write`] closure for `path`.
///
/// The temp-file write / fsync / rename / dir-fsync steps all map their IO error
/// to `Write { path, source }`; this returns the `map_err` closure so each call
/// site is `.map_err(write_err(&p))?` instead of a repeated 4-line literal.
fn write_err(path: &Path) -> impl FnOnce(std::io::Error) -> SidecarIoError + '_ {
    move |source| SidecarIoError::Write {
        path: path.to_path_buf(),
        source,
    }
}

/// Atomic JSON persistence for one `_system/` document of type `T`.
///
/// `T` is the *whole* document — callers parameterize with the collection they
/// store (`SystemSidecar<Vec<Row>>`). Persisting `T` rather than a hardwired
/// `Vec` leaves room for a future versioned envelope without changing this IO
/// layer.
pub struct SystemSidecar<T> {
    path: PathBuf,
    // `fn() -> T` so the sidecar is unconditionally `Send`/`Sync`/`Clone`:
    // it produces and consumes `T` but never owns one.
    _marker: PhantomData<fn() -> T>,
}

impl<T> Clone for SystemSidecar<T> {
    fn clone(&self) -> Self {
        Self {
            path: self.path.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for SystemSidecar<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SystemSidecar")
            .field("path", &self.path)
            .finish()
    }
}

impl<T> SystemSidecar<T> {
    /// Construct a sidecar rooted at `<data_path>/_system/<file_name>`.
    ///
    /// `file_name` must match the on-disk name the subsystem has always used —
    /// changing it would orphan existing state across an upgrade.
    pub fn new(data_path: impl AsRef<Path>, file_name: &str) -> Self {
        let mut path = data_path.as_ref().to_path_buf();
        path.push("_system");
        path.push(file_name);
        Self {
            path,
            _marker: PhantomData,
        }
    }

    /// Construct a sidecar at an exact `path`, bypassing the `_system/`
    /// convention.
    ///
    /// For callers that already own the full target path (e.g. an embedder that
    /// supplies it directly) and must keep that exact location for on-disk
    /// compatibility.
    pub fn at_path(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            _marker: PhantomData,
        }
    }

    /// Borrow the resolved sidecar path (for diagnostics).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Load the persisted document, or `T::default()` if absent or empty.
    ///
    /// A missing file and a zero-byte file both mean "nothing persisted yet"
    /// and yield the default — first-boot callers rely on this.
    ///
    /// # Errors
    ///
    /// Returns [`SidecarIoError::Read`] on IO failure or
    /// [`SidecarIoError::Decode`] when the file is present but not valid JSON.
    pub fn load(&self) -> Result<T, SidecarIoError>
    where
        T: DeserializeOwned + Default,
    {
        // Read directly: a missing file (`NotFound`) and a zero-byte file both
        // mean "nothing persisted yet" and yield the default. Reading once
        // (rather than `exists()` + `read`) avoids a double stat and the race
        // window where a file deleted between the two calls would surface as a
        // `Read` error instead of the intended default.
        let bytes = match std::fs::read(&self.path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(T::default()),
            Err(source) => {
                return Err(SidecarIoError::Read {
                    path: self.path.clone(),
                    source,
                });
            }
        };
        if bytes.is_empty() {
            return Ok(T::default());
        }
        serde_json::from_slice(&bytes).map_err(|source| SidecarIoError::Decode {
            path: self.path.clone(),
            source,
        })
    }

    /// Atomically replace the persisted document with `value`.
    ///
    /// Writes a temp file, fsyncs it, renames it over the target, then fsyncs
    /// the parent directory so the rename itself is crash-durable.
    ///
    /// # Errors
    ///
    /// Returns [`SidecarIoError::Encode`] if serialization fails,
    /// [`SidecarIoError::CreateDir`] if the `_system/` directory cannot be
    /// created, or [`SidecarIoError::Write`] on any write / fsync / rename
    /// failure.
    pub fn store(&self, value: &T) -> Result<(), SidecarIoError>
    where
        T: Serialize,
    {
        self.store_value(value)
    }

    /// Atomically replace the persisted document with any value that serializes
    /// to the same JSON as `T`.
    ///
    /// Identical to [`Self::store`] but accepts a borrowed view (e.g. a `&[Row]`
    /// for a `SystemSidecar<Vec<Row>>`) so callers that already own a slice can
    /// persist it without cloning into an owned `T` first. `S` must serialize to
    /// the same shape `T` deserializes from — the caller guarantees this.
    ///
    /// # Errors
    ///
    /// Same as [`Self::store`].
    pub fn store_value<S>(&self, value: &S) -> Result<(), SidecarIoError>
    where
        S: Serialize + ?Sized,
    {
        // Resolve the parent dir once: `None` when the path is a bare file name
        // (or `/`), in which case the sidecar lives in the current directory.
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty());
        if let Some(parent) = parent {
            std::fs::create_dir_all(parent).map_err(|source| SidecarIoError::CreateDir {
                path: parent.to_path_buf(),
                source,
            })?;
        }
        let json = serde_json::to_vec_pretty(value).map_err(|source| SidecarIoError::Encode {
            path: self.path.clone(),
            source,
        })?;
        let tmp = self.path.with_extension("tmp");
        {
            let mut f = File::create(&tmp).map_err(write_err(&tmp))?;
            f.write_all(&json).map_err(write_err(&tmp))?;
            // Flush the file's data before the rename promotes it.
            f.sync_all().map_err(write_err(&tmp))?;
        }
        std::fs::rename(&tmp, &self.path).map_err(write_err(&self.path))?;
        Self::sync_dir(parent.unwrap_or_else(|| Path::new(".")))?;
        Ok(())
    }

    /// Fsync `dir` so a preceding `rename` into it is durable across a crash.
    ///
    /// No-op on non-Unix targets, where there is no portable directory-fsync;
    /// Windows makes the rename durable through different mechanics.
    fn sync_dir(dir: &Path) -> Result<(), SidecarIoError> {
        #[cfg(unix)]
        {
            let f = File::open(dir).map_err(write_err(dir))?;
            f.sync_all().map_err(write_err(dir))?;
        }
        #[cfg(not(unix))]
        let _ = dir;
        Ok(())
    }
}

/// Atomic JSON sidecar specialized to a `Vec<T>` document — the shape every
/// `_system/` row table uses (CDC checkpoints, scheduler jobs, deferred
/// triggers, declared plugins).
///
/// Wraps [`SystemSidecar`] and adds the two conveniences every row-table caller
/// re-rolled by hand: [`Self::load`] returns an empty `Vec` for an absent /
/// empty file, and [`Self::store`] takes a borrowed `&[T]` so a caller that
/// already owns a `Vec<T>` (or a sub-slice) persists it without an intermediate
/// clone.
#[derive(Clone, Debug)]
pub struct VecSidecar<T> {
    inner: SystemSidecar<Vec<T>>,
}

impl<T> VecSidecar<T> {
    /// Construct rooted at `<data_path>/_system/<file_name>`.
    ///
    /// `file_name` must match the on-disk name the subsystem has always used —
    /// changing it would orphan existing state across an upgrade.
    pub fn new(data_path: impl AsRef<Path>, file_name: &str) -> Self {
        Self {
            inner: SystemSidecar::new(data_path, file_name),
        }
    }

    /// Borrow the resolved sidecar path (for diagnostics).
    pub fn path(&self) -> &Path {
        self.inner.path()
    }

    /// Load the persisted rows, or an empty `Vec` if the file is absent or empty.
    ///
    /// # Errors
    ///
    /// Returns [`SidecarIoError::Read`] on IO failure or
    /// [`SidecarIoError::Decode`] when the file is present but not valid JSON.
    pub fn load(&self) -> Result<Vec<T>, SidecarIoError>
    where
        T: DeserializeOwned,
    {
        self.inner.load()
    }

    /// Atomically replace the persisted rows with `rows`.
    ///
    /// Takes a borrowed slice so callers that already own a `Vec<T>` persist it
    /// without cloning.
    ///
    /// # Errors
    ///
    /// Same as [`SystemSidecar::store`].
    pub fn store(&self, rows: &[T]) -> Result<(), SidecarIoError>
    where
        T: Serialize,
    {
        self.inner.store_value(rows)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_file_loads_default() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar: SystemSidecar<Vec<String>> = SystemSidecar::new(dir.path(), "missing.json");
        assert_eq!(sidecar.load().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn empty_file_loads_default() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar: SystemSidecar<Vec<String>> = SystemSidecar::new(dir.path(), "empty.json");
        std::fs::create_dir_all(sidecar.path().parent().unwrap()).unwrap();
        std::fs::write(sidecar.path(), b"").unwrap();
        assert_eq!(sidecar.load().unwrap(), Vec::<String>::new());
    }

    #[test]
    fn store_then_load_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar: SystemSidecar<Vec<String>> = SystemSidecar::new(dir.path(), "rows.json");
        let rows = vec!["a".to_owned(), "b".to_owned()];
        sidecar.store(&rows).unwrap();
        assert_eq!(sidecar.load().unwrap(), rows);
        // Resolved path follows the `_system/<file_name>` convention.
        assert!(sidecar.path().ends_with("_system/rows.json"));
    }

    #[test]
    fn at_path_uses_exact_location() {
        let dir = tempfile::tempdir().unwrap();
        let exact = dir.path().join("declared_plugins.json");
        let sidecar: SystemSidecar<Vec<u32>> = SystemSidecar::at_path(&exact);
        sidecar.store(&vec![7]).unwrap();
        assert_eq!(sidecar.path(), exact);
        assert!(exact.exists());
        assert_eq!(sidecar.load().unwrap(), vec![7]);
    }

    #[test]
    fn vec_sidecar_stores_borrowed_slice_and_loads_empty_default() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar: VecSidecar<String> = VecSidecar::new(dir.path(), "rows.json");
        // Absent file → empty vec (no `Default` bound needed on the element).
        assert_eq!(sidecar.load().unwrap(), Vec::<String>::new());
        let rows = vec!["a".to_owned(), "b".to_owned()];
        // `store` takes `&[T]`; the owned `rows` is untouched (no clone).
        sidecar.store(&rows).unwrap();
        assert_eq!(sidecar.load().unwrap(), rows);
        assert!(sidecar.path().ends_with("_system/rows.json"));
    }

    #[test]
    fn store_replaces_previous_and_leaves_no_temp() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar: SystemSidecar<Vec<u32>> = SystemSidecar::new(dir.path(), "nums.json");
        sidecar.store(&vec![1, 2, 3]).unwrap();
        sidecar.store(&vec![9]).unwrap();
        assert_eq!(sidecar.load().unwrap(), vec![9]);
        // The temp file must not survive a successful write.
        let tmp = sidecar.path().with_extension("tmp");
        assert!(!tmp.exists(), "temp file leaked: {tmp:?}");
    }
}
