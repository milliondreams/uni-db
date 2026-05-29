//! The core `Plugin` trait and supporting types.
//!
//! Every uni-db extension implements [`Plugin`]. The trait is deliberately
//! tiny: a [`PluginManifest`] accessor, a `register` method that calls into a
//! `PluginRegistrar`, and optional `init` / `shutdown` hooks. The heavy
//! lifting lives in the per-surface capability traits in [`crate::traits`].

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use smol_str::SmolStr;

use crate::errors::PluginError;
use crate::manifest::PluginManifest;
use crate::registrar::PluginRegistrar;

/// Reverse-DNS plugin identifier — e.g. `"ai.dragonscale.geo"`.
///
/// Used as the namespace component of every [`crate::QName`] the plugin
/// registers. Must be unique across all plugins loaded into one Uni
/// instance.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PluginId(SmolStr);

impl PluginId {
    /// Construct a `PluginId` from a string.
    ///
    /// # Panics
    ///
    /// Panics if `s` is empty — a programming error, since plugin ids are
    /// determined at plugin-author time.
    #[must_use]
    pub fn new(s: impl Into<SmolStr>) -> Self {
        let s = s.into();
        assert!(!s.is_empty(), "PluginId must not be empty");
        Self(s)
    }

    /// Parse a plugin id from a string slice.
    ///
    /// Currently the same as [`PluginId::new`] but returns a `Result` to
    /// keep the API forward-compatible with future validation rules
    /// (reserved prefixes, character restrictions, length caps).
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::Internal`] if the input is empty.
    pub fn parse(s: impl AsRef<str>) -> Result<Self, PluginError> {
        let s = s.as_ref();
        if s.is_empty() {
            return Err(PluginError::internal("PluginId must not be empty"));
        }
        Ok(Self(SmolStr::new(s)))
    }

    /// Returns the string representation.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for PluginId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A handle returned by `Uni::add_plugin` and similar APIs.
///
/// Carries the plugin id plus a *generation* counter that bumps on every
/// hot-reload. Holding a `PluginHandle` does not keep the plugin alive
/// against `remove_plugin` — the handle simply identifies which plugin's
/// registrations are being targeted.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct PluginHandle {
    /// Plugin id at registration.
    pub id: PluginId,
    /// Hot-reload generation (`0` on initial load, increments on each
    /// successful reload).
    pub generation: u64,
}

impl PluginHandle {
    /// Construct a handle.
    #[must_use]
    pub fn new(id: PluginId, generation: u64) -> Self {
        Self { id, generation }
    }
}

/// Init-time context provided to [`Plugin::init`].
///
/// Currently carries the effective capability set; future fields may add
/// host-version information, configured workspace paths, etc.
#[derive(Debug)]
#[non_exhaustive]
pub struct PluginInitContext<'a> {
    /// The capability set after intersecting manifest-requested with
    /// host-granted.
    pub effective_caps: &'a crate::CapabilitySet,
}

/// Marker trait for plugin-side extension state.
///
/// Mostly an implementation hint: plugin-shared mutable state should be held
/// behind `Arc<…>` and `Send + Sync`. This trait does not enforce anything
/// but documents the expectation.
pub trait PluginState: Send + Sync + 'static {}

impl<T: Send + Sync + 'static> PluginState for T {}

/// The trait every uni-db extension implements.
///
/// A `Plugin` is a *bundle* of capability registrations: scalar functions,
/// aggregates, procedures, hooks, etc. The trait itself is small; all
/// per-surface detail lives in the capability traits in [`crate::traits`].
///
/// # Examples
///
/// ```no_run
/// use std::sync::Arc;
/// use uni_plugin::{Plugin, PluginManifest, PluginRegistrar, PluginError};
///
/// pub struct NoopPlugin {
///     manifest: PluginManifest,
/// }
///
/// impl Plugin for NoopPlugin {
///     fn manifest(&self) -> &PluginManifest { &self.manifest }
///     fn register(&self, _r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
///         Ok(())
///     }
/// }
/// ```
pub trait Plugin: Send + Sync + 'static {
    /// Static plugin description.
    ///
    /// Implementations typically store the manifest in a field and return a
    /// borrow. The manifest is read at load time to compute the effective
    /// capability set before [`Plugin::register`] is invoked.
    fn manifest(&self) -> &PluginManifest;

    /// Register extension points with the host.
    ///
    /// Called exactly once at load time, after capability negotiation. The
    /// plugin uses the registrar's typed builder methods to claim qualified
    /// names for each kind of extension.
    ///
    /// # Errors
    ///
    /// Returns [`PluginError::DuplicateRegistration`] if two registrations
    /// claim the same `QName`, or [`PluginError::CapabilityRequired`] if a
    /// registration requires a capability not in the effective set.
    fn register(&self, r: &mut PluginRegistrar<'_>) -> Result<(), PluginError>;

    /// Optional initialization callback.
    ///
    /// Called once after registration, in dependency order over the
    /// manifest's `depends_on` list. The default no-op is sufficient for
    /// plugins that don't need init-time setup.
    ///
    /// # Errors
    ///
    /// Plugins that fail initialization should return a [`PluginError`];
    /// the host will then unregister this plugin's registrations and
    /// propagate the error to the caller of `Uni::add_plugin`.
    fn init(&self, _cx: &PluginInitContext<'_>) -> Result<(), PluginError> {
        Ok(())
    }

    /// Optional shutdown callback.
    ///
    /// Called once at instance teardown, in reverse dependency order. The
    /// default does nothing; plugins holding external resources (open
    /// files, network connections) override this for graceful cleanup.
    fn shutdown(&self) {}
}

/// A type-erased plugin reference suitable for storing in collections.
pub type DynPlugin = Arc<dyn Plugin>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plugin_id_round_trip() {
        let id = PluginId::new("ai.dragonscale.geo");
        assert_eq!(id.as_str(), "ai.dragonscale.geo");
        assert_eq!(id.to_string(), "ai.dragonscale.geo");
    }

    #[test]
    #[should_panic(expected = "PluginId must not be empty")]
    fn plugin_id_empty_panics() {
        let _ = PluginId::new("");
    }

    #[test]
    fn plugin_id_parse_rejects_empty() {
        assert!(PluginId::parse("").is_err());
    }

    #[test]
    fn plugin_handle_construction() {
        let h = PluginHandle::new(PluginId::new("foo"), 0);
        assert_eq!(h.id.as_str(), "foo");
        assert_eq!(h.generation, 0);
    }
}
