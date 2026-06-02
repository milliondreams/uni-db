//! Path normalization for filesystem capability matching.
//!
//! Capability allow-lists (`Capability::Filesystem { read, write }`) are globs
//! matched by [`crate::capability`]'s path-opaque `wildcard_match`, where `*`
//! and `**` both span `/`. That matcher is correct for opaque strings (URLs,
//! key ids), but a filesystem path carries `.`/`..` semantics the matcher does
//! not understand: a guest granted `read: ["/data/**"]` could pass
//! `"/data/../../etc/passwd"`, which textually matches `/data/**` while the
//! kernel resolves it to `/etc/passwd` — a sandbox escape.
//!
//! The fix is to make the *checked* path identical to the *acted-upon* path.
//! [`normalize_capability_path`] is the deterministic, IO-free first layer: it
//! requires an absolute path and lexically resolves `.`/`..`, rejecting any path
//! that would escape above the filesystem root. Loaders match the **normalized**
//! path against the allow-list, and additionally canonicalize (resolving
//! symlinks) before the syscall for defense in depth — see the loader fs host
//! fns (e.g. `uni-plugin-rhai`'s `host_fn_impls::fs`).

// Rust guideline compliant

use std::path::{Component, Path, PathBuf};

/// Lexically normalize an absolute capability path for allow-list matching.
///
/// Resolves `.` and `..` components purely textually (no filesystem access, so
/// it works for not-yet-created write targets) and collapses redundant
/// separators. The result is the canonical lexical form a loader should match
/// against a `Capability::Filesystem` allow-list and then act on.
///
/// Returns `None` when the path is unsafe to admit:
/// - it is **relative** (capability paths must be absolute), or
/// - a `..` component would escape **above the filesystem root**, or
/// - it contains a platform prefix (e.g. a Windows drive prefix), which the
///   capability model does not model.
///
/// A `..` that stays within the root is resolved, not rejected — e.g.
/// `/data/../etc` normalizes to `/etc`; admitting it here is safe because the
/// allow-list match then rejects `/etc` for a `/data/**` grant. Only true
/// root escapes are refused outright.
///
/// # Examples
/// ```
/// use std::path::PathBuf;
/// use uni_plugin::normalize_capability_path as norm;
///
/// assert_eq!(norm("/data/./sub/f"), Some(PathBuf::from("/data/sub/f")));
/// assert_eq!(norm("/data/../etc"), Some(PathBuf::from("/etc")));
/// assert_eq!(norm("/data/../../etc/passwd"), None); // escapes above root
/// assert_eq!(norm("data/x"), None); // relative
/// ```
#[must_use]
pub fn normalize_capability_path(path: &str) -> Option<PathBuf> {
    let mut out = PathBuf::new();
    let mut has_root = false;
    let mut depth: usize = 0; // count of Normal components above root

    for comp in Path::new(path).components() {
        match comp {
            Component::RootDir => {
                has_root = true;
                out.push(comp.as_os_str());
            }
            // Drop `.` and any redundant separators (components() already
            // collapses the latter).
            Component::CurDir => {}
            Component::ParentDir => {
                // Pop one Normal component. If there is none above the root, the
                // path is trying to escape — refuse.
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                out.pop();
            }
            Component::Normal(c) => {
                depth += 1;
                out.push(c);
            }
            // Windows drive/UNC prefixes are not part of the capability model.
            Component::Prefix(_) => return None,
        }
    }

    // Capability paths must be absolute; a relative path has no anchored root to
    // compare against an allow-list.
    if !has_root {
        return None;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_relative_paths() {
        assert_eq!(normalize_capability_path("data/x"), None);
        assert_eq!(normalize_capability_path("./data/x"), None);
        assert_eq!(normalize_capability_path(""), None);
        assert_eq!(normalize_capability_path("../etc/passwd"), None);
    }

    #[test]
    fn rejects_escape_above_root() {
        // The reported exploit: two `..` from depth-1 pops past the root.
        assert_eq!(normalize_capability_path("/data/../../etc/passwd"), None);
        assert_eq!(normalize_capability_path("/.."), None);
        assert_eq!(normalize_capability_path("/data/../.."), None);
    }

    #[test]
    fn resolves_dot_and_redundant_separators() {
        assert_eq!(
            normalize_capability_path("/data/./sub/f"),
            Some(PathBuf::from("/data/sub/f"))
        );
        assert_eq!(
            normalize_capability_path("/data//sub///f"),
            Some(PathBuf::from("/data/sub/f"))
        );
        assert_eq!(
            normalize_capability_path("/data/sub/f"),
            Some(PathBuf::from("/data/sub/f"))
        );
    }

    #[test]
    fn resolves_in_root_parent_without_rejecting() {
        // `..` that stays within the root resolves; the allow-list (not this
        // function) is responsible for rejecting the resulting `/etc`.
        assert_eq!(
            normalize_capability_path("/data/../etc"),
            Some(PathBuf::from("/etc"))
        );
        assert_eq!(
            normalize_capability_path("/data/.."),
            Some(PathBuf::from("/"))
        );
    }
}
