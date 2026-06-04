//! Wire-protocol connector plugins.

use crate::errors::FnError;

/// Free-form connector configuration (JSON-encoded).
#[derive(Clone, Debug, Default)]
pub struct ConnectorConfig {
    /// JSON config payload.
    pub config_json: String,
}

/// Opaque handle returned by [`Connector::start`].
#[derive(Clone, Copy, Debug)]
pub struct ConnectorHandle(pub u64);

/// A wire-protocol connector — Bolt, GraphQL, REST, etc.
pub trait Connector: Send + Sync {
    /// Protocol name (`"bolt"`, `"graphql"`, …).
    fn protocol(&self) -> &str;

    /// Start the connector with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the connector cannot start (bind failure,
    /// missing dependency).
    fn start(&self, cfg: ConnectorConfig) -> Result<ConnectorHandle, FnError>;

    /// Stop the connector.
    ///
    /// # Errors
    ///
    /// Returns [`FnError`] if the shutdown fails.
    fn stop(&self, handle: ConnectorHandle) -> Result<(), FnError>;
}

/// Authentication credentials presented to an `AuthProvider`.
#[derive(Clone, Debug)]
pub enum Credentials {
    /// Username + password pair.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Bearer token.
    Bearer(String),
    /// mTLS client cert (DER-encoded).
    MtlsCert(Vec<u8>),
}

/// Successfully-authenticated identity.
#[derive(Clone, Debug)]
pub struct Principal {
    /// Identity string (subject id, username, etc.).
    pub id: String,
    /// Group memberships.
    pub groups: Vec<String>,
    /// Capabilities held by this principal.
    ///
    /// Populated by the host's authentication/authorization layer at
    /// principal-construction time — typically the
    /// [`AuthProvider`] / [`AuthzPolicy`] pair resolves group
    /// memberships to a [`crate::CapabilitySet`]. Procedure invocation
    /// paths (e.g.
    /// `uni.plugin.declareProcedure` for `WRITE` mode) gate on
    /// `principal.capabilities.contains_variant(&Capability::...)`.
    pub capabilities: crate::CapabilitySet,
}

impl Principal {
    /// Construct an anonymous principal with no capabilities — the
    /// safe default for unauthenticated paths.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            id: "anonymous".to_owned(),
            groups: Vec::new(),
            capabilities: crate::CapabilitySet::new(),
        }
    }
}

/// Authentication failure cause.
#[derive(Clone, Debug, thiserror::Error)]
#[error("authentication failure: {0}")]
pub struct AuthError(pub String);

/// Authentication provider — `AuthProvider::authenticate(creds) -> Principal`.
pub trait AuthProvider: Send + Sync {
    /// Authentication scheme name (`"basic"`, `"bearer"`, `"mtls"`).
    fn scheme(&self) -> &str;

    /// Authenticate the given credentials.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError`] if the credentials are invalid.
    fn authenticate(&self, credentials: &Credentials) -> Result<Principal, AuthError>;
}

/// Authorization action under check.
#[derive(Clone, Debug)]
pub struct Action {
    /// Action verb (`"read"`, `"write"`, `"delete"`, …).
    pub verb: String,
}

/// Authorization resource under check.
#[derive(Clone, Debug)]
pub struct Resource {
    /// Resource path / identifier.
    pub path: String,
}

/// Authorization decision.
#[derive(Clone, Debug)]
pub enum Decision {
    /// Permit the action.
    Allow,
    /// Deny with reason.
    Deny {
        /// Human-readable reason.
        reason: String,
    },
}

/// Authorization failure (policy errored out, not "denied").
#[derive(Clone, Debug, thiserror::Error)]
#[error("authorization policy failure: {0}")]
pub struct AuthzError(pub String);

/// Authorization policy plugin.
pub trait AuthzPolicy: Send + Sync {
    /// Check whether `principal` may perform `action` on `resource`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthzError`] if the policy fails to evaluate (e.g.,
    /// external policy server unreachable).
    fn check(
        &self,
        principal: &Principal,
        action: &Action,
        resource: &Resource,
    ) -> Result<Decision, AuthzError>;
}
