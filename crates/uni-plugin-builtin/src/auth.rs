//! Built-in `AuthProvider` and `AuthzPolicy` reference implementations.
//!
//! M5 surface coverage: ships one of each auth surface with realistic
//! semantics. Real deployments register their own providers; these
//! built-ins are useful for tests, examples, and embedded single-user
//! deployments where authentication is nominal.
//!
//! - [`BasicAuthProvider`] — username:password authentication against
//!   an in-memory table of `(username → hashed_password)`. Hash is
//!   Blake3 (not PBKDF2 / Argon2 — adequate for the reference impl,
//!   inadequate for production).
//! - [`AllowGroupAuthzPolicy`] — group-membership ACL. A principal in
//!   the configured allow-group is granted any action; otherwise
//!   denied. Real RBAC/ABAC policies ship as user plugins.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use uni_plugin::traits::connector::{
    Action, AuthError, AuthProvider, AuthzError, AuthzPolicy, Credentials, Decision, Principal,
    Resource,
};
use uni_plugin::{PluginError, PluginRegistrar};

/// Register the built-in auth providers + authz policies.
///
/// # Errors
///
/// Returns [`PluginError`] on registration failure.
pub fn register_into(_r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    // Reference impls (`BasicAuthProvider`, `AllowGroupAuthzPolicy`)
    // are exported from this crate as building blocks but are NOT
    // auto-registered. Auto-registering an `AllowGroupAuthzPolicy`
    // here would silently require every host to grant principals
    // membership in the chosen group — a security-policy decision
    // that belongs to the host. Hosts that want the references opt
    // in explicitly via `r.auth_provider(BasicAuthProvider::new())`
    // and `r.authz_policy(AllowGroupAuthzPolicy::new("admin"))`.
    Ok(())
}

/// HTTP Basic-style username:password authentication.
///
/// Stores `(username → blake3-hashed-password)` pairs. The hash is
/// salt-free Blake3, which is **not** suitable for production password
/// storage (no work factor, no per-user salt). Real auth providers
/// must use a slow KDF (Argon2id / PBKDF2 / scrypt) with per-user salt.
///
/// # Example
///
/// ```ignore
/// let p = BasicAuthProvider::new();
/// p.add_user("alice", "supersecret");
/// match p.authenticate(&Credentials::Basic {
///     username: "alice".into(),
///     password: "supersecret".into(),
/// }) {
///     Ok(principal) => println!("authenticated: {}", principal.id),
///     Err(e) => println!("denied: {e}"),
/// }
/// ```
#[derive(Debug)]
pub struct BasicAuthProvider {
    /// Username → blake3 hash. RwLock for cheap reads on the hot path.
    users: Arc<RwLock<HashMap<String, [u8; 32]>>>,
}

impl Default for BasicAuthProvider {
    fn default() -> Self {
        Self::new()
    }
}

impl BasicAuthProvider {
    /// Construct an empty provider — no users.
    #[must_use]
    pub fn new() -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a user with the given password. Replaces any existing entry.
    pub fn add_user(&self, username: &str, password: &str) {
        let hash = blake3::hash(password.as_bytes());
        self.users
            .write()
            .insert(username.to_owned(), *hash.as_bytes());
    }

    /// Remove a user.
    pub fn remove_user(&self, username: &str) {
        self.users.write().remove(username);
    }

    /// Number of registered users.
    #[must_use]
    pub fn user_count(&self) -> usize {
        self.users.read().len()
    }
}

impl AuthProvider for BasicAuthProvider {
    fn scheme(&self) -> &str {
        "basic"
    }

    fn authenticate(&self, credentials: &Credentials) -> Result<Principal, AuthError> {
        let (username, password) = match credentials {
            Credentials::Basic { username, password } => (username, password),
            Credentials::Bearer(_) => {
                return Err(AuthError(
                    "basic auth provider rejects bearer credentials".to_owned(),
                ));
            }
            Credentials::MtlsCert(_) => {
                return Err(AuthError(
                    "basic auth provider rejects mTLS credentials".to_owned(),
                ));
            }
        };

        let users = self.users.read();
        let stored = users
            .get(username)
            .ok_or_else(|| AuthError(format!("unknown user `{username}`")))?;

        let supplied = blake3::hash(password.as_bytes());
        // Constant-time comparison to avoid timing oracles.
        if constant_time_eq(stored, supplied.as_bytes()) {
            Ok(Principal {
                id: username.clone(),
                groups: vec![],
                capabilities: uni_plugin::CapabilitySet::new(),
            })
        } else {
            Err(AuthError(format!("invalid password for user `{username}`")))
        }
    }
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Authz policy that allows every action for principals in a configured
/// group, denies everything else.
///
/// Useful as a reference for the `AuthzPolicy` trait surface and as a
/// pragmatic single-tenant policy where "admins can do everything,
/// others can do nothing." Real-world policies should compose multiple
/// `AuthzPolicy` impls (RBAC, ABAC, row-level security) — the host
/// chains them in registration order and denies on first deny.
#[derive(Debug)]
pub struct AllowGroupAuthzPolicy {
    allow_group: String,
}

impl AllowGroupAuthzPolicy {
    /// Allow principals whose `groups` includes `group`.
    #[must_use]
    pub fn new(group: impl Into<String>) -> Self {
        Self {
            allow_group: group.into(),
        }
    }
}

impl AuthzPolicy for AllowGroupAuthzPolicy {
    fn check(
        &self,
        principal: &Principal,
        action: &Action,
        resource: &Resource,
    ) -> Result<Decision, AuthzError> {
        if principal.groups.iter().any(|g| g == &self.allow_group) {
            Ok(Decision::Allow)
        } else {
            Ok(Decision::Deny {
                reason: format!(
                    "principal `{}` not in allow-group `{}` (action={} resource={})",
                    principal.id, self.allow_group, action.verb, resource.path
                ),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_auth_provider_scheme_is_basic() {
        assert_eq!(BasicAuthProvider::new().scheme(), "basic");
    }

    #[test]
    fn basic_auth_unknown_user_denied() {
        let p = BasicAuthProvider::new();
        let err = p
            .authenticate(&Credentials::Basic {
                username: "ghost".into(),
                password: "hunter2".into(),
            })
            .unwrap_err();
        assert!(err.0.contains("unknown user"));
    }

    #[test]
    fn basic_auth_valid_credentials_allowed() {
        let p = BasicAuthProvider::new();
        p.add_user("alice", "supersecret");
        let principal = p
            .authenticate(&Credentials::Basic {
                username: "alice".into(),
                password: "supersecret".into(),
            })
            .unwrap();
        assert_eq!(principal.id, "alice");
    }

    #[test]
    fn basic_auth_wrong_password_denied() {
        let p = BasicAuthProvider::new();
        p.add_user("alice", "supersecret");
        let err = p
            .authenticate(&Credentials::Basic {
                username: "alice".into(),
                password: "wrong".into(),
            })
            .unwrap_err();
        assert!(err.0.contains("invalid password"));
    }

    #[test]
    fn basic_auth_rejects_bearer_credentials() {
        let p = BasicAuthProvider::new();
        let err = p
            .authenticate(&Credentials::Bearer("token".to_owned()))
            .unwrap_err();
        assert!(err.0.contains("bearer"));
    }

    #[test]
    fn user_count_tracks_adds_and_removes() {
        let p = BasicAuthProvider::new();
        assert_eq!(p.user_count(), 0);
        p.add_user("a", "x");
        p.add_user("b", "y");
        assert_eq!(p.user_count(), 2);
        p.remove_user("a");
        assert_eq!(p.user_count(), 1);
    }

    #[test]
    fn authz_admin_group_allowed_any_action() {
        let policy = AllowGroupAuthzPolicy::new("admin");
        let principal = Principal {
            id: "alice".into(),
            groups: vec!["admin".to_owned()],
            capabilities: uni_plugin::CapabilitySet::new(),
        };
        let action = Action {
            verb: "delete".into(),
        };
        let resource = Resource {
            path: "/sensitive".into(),
            ..Default::default()
        };
        let decision = policy.check(&principal, &action, &resource).unwrap();
        assert!(matches!(decision, Decision::Allow));
    }

    #[test]
    fn authz_non_admin_denied() {
        let policy = AllowGroupAuthzPolicy::new("admin");
        let principal = Principal {
            id: "bob".into(),
            groups: vec!["users".to_owned()],
            capabilities: uni_plugin::CapabilitySet::new(),
        };
        let decision = policy
            .check(
                &principal,
                &Action {
                    verb: "read".into(),
                },
                &Resource {
                    path: "/anything".into(),
                    ..Default::default()
                },
            )
            .unwrap();
        assert!(
            matches!(decision, Decision::Deny { ref reason } if reason.contains("not in allow-group"))
        );
    }
}
