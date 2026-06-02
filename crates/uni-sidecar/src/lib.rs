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
        if !self.path.exists() {
            return Ok(T::default());
        }
        let bytes = std::fs::read(&self.path).map_err(|source| SidecarIoError::Read {
            path: self.path.clone(),
            source,
        })?;
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
        if let Some(parent) = self.path.parent()
            && !parent.as_os_str().is_empty()
        {
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
            let mut f = File::create(&tmp).map_err(|source| SidecarIoError::Write {
                path: tmp.clone(),
                source,
            })?;
            f.write_all(&json).map_err(|source| SidecarIoError::Write {
                path: tmp.clone(),
                source,
            })?;
            // Flush the file's data before the rename promotes it.
            f.sync_all().map_err(|source| SidecarIoError::Write {
                path: tmp.clone(),
                source,
            })?;
        }
        std::fs::rename(&tmp, &self.path).map_err(|source| SidecarIoError::Write {
            path: self.path.clone(),
            source,
        })?;
        self.sync_parent_dir()?;
        Ok(())
    }

    /// Fsync the directory containing the sidecar so the preceding `rename` is
    /// durable across a crash.
    ///
    /// No-op on non-Unix targets, where there is no portable directory-fsync;
    /// Windows makes the rename durable through different mechanics.
    fn sync_parent_dir(&self) -> Result<(), SidecarIoError> {
        #[cfg(unix)]
        {
            let parent = self.path.parent().unwrap_or(Path::new(""));
            let dir = if parent.as_os_str().is_empty() {
                Path::new(".")
            } else {
                parent
            };
            let f = File::open(dir).map_err(|source| SidecarIoError::Write {
                path: dir.to_path_buf(),
                source,
            })?;
            f.sync_all().map_err(|source| SidecarIoError::Write {
                path: dir.to_path_buf(),
                source,
            })?;
        }
        Ok(())
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
