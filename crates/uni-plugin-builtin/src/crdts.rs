//! Built-in CRDT-kind registrations.
//!
//! M5b scaffolding: registers `lww-register` and `or-set` placeholders
//! demonstrating the [`CrdtKindProvider`] registration pattern. Real
//! `uni-crdt` bridge integration lands in M5b cutover commits.

use std::sync::Arc;

use datafusion::scalar::ScalarValue;
use uni_plugin::traits::crdt::{CrdtKind, CrdtKindProvider, CrdtOp, CrdtState};
use uni_plugin::{FnError, PluginError, PluginRegistrar};

/// Register the built-in CRDT-kind providers.
///
/// **M5d:** All 5 built-in CRDT kinds are real (no placeholders):
/// - `lww-register` — Last-Writer-Wins register over `String`.
/// - `or-set` — Observed-Remove Set over `String`, add-bias semantics.
/// - `g-counter` — Grow-only Counter (per-replica monotonic increments).
/// - `mv-register` — Multi-Value Register with vector-clock-style conflict
///   retention.
/// - `rga` — Replicated Growable Array over `String`, wrapping
///   `uni_crdt::Rga<String>`; ops carry pre-generated UUIDs so peer
///   replicas converge under `CrdtMerge::merge`.
///
/// Note: the `PluginRegistry` is not yet consulted from `uni-crdt` mutation
/// paths (host wiring is a separate, larger refactor tracked as a follow-up).
/// The 5 registrations are exercised by `uni-plugin-builtin` unit tests and
/// the conformance harness.
///
/// # Errors
///
/// Returns [`PluginError`] on duplicate kind registration.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.crdt_kind(CrdtKind::new("lww-register"), Arc::new(LwwRegisterProvider))?;
    r.crdt_kind(CrdtKind::new("or-set"), Arc::new(OrSetProvider))?;
    r.crdt_kind(CrdtKind::new("g-counter"), Arc::new(GCounterProvider))?;
    r.crdt_kind(CrdtKind::new("mv-register"), Arc::new(MvRegisterProvider))?;
    r.crdt_kind(CrdtKind::new("rga"), Arc::new(RgaProvider::new()))?;
    Ok(())
}

/// LWW (Last-Writer-Wins) register CRDT-kind provider.
#[derive(Debug)]
pub struct LwwRegisterProvider;

impl CrdtKindProvider for LwwRegisterProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("lww-register")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(LwwState { ts: 0, value: None })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let (ts, value): (u64, Option<String>) = serde_json::from_slice(bytes)
            .map_err(|e| FnError::new(0x800, format!("lww deserialize: {e}")))?;
        Ok(Box::new(LwwState { ts, value }))
    }
}

#[derive(Debug)]
struct LwwState {
    ts: u64,
    value: Option<String>,
}

impl CrdtState for LwwState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError> {
        let (ts, value): (u64, Option<String>) = serde_json::from_slice(&op.bytes)
            .map_err(|e| FnError::new(0x801, format!("lww apply: {e}")))?;
        if ts >= self.ts {
            self.ts = ts;
            self.value = value;
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        // LWW merge: persist `other`'s (ts, value) and re-apply it. The
        // `apply` path keeps whichever timestamp is greater-or-equal, so
        // the later writer wins regardless of merge direction.
        let serialized = other.persist()?;
        self.apply(&CrdtOp { bytes: serialized })
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        Ok(match &self.value {
            Some(s) => ScalarValue::Utf8(Some(s.clone())),
            None => ScalarValue::Utf8(None),
        })
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        serde_json::to_vec(&(self.ts, &self.value))
            .map_err(|e| FnError::new(0x802, format!("lww persist: {e}")))
    }
}

/// OR-Set (Observed-Remove Set) CRDT-kind provider.
///
/// Add-bias semantics: each `add` records a unique tag; `remove` only
/// retires tags it has observed. Concurrent add+remove of the same
/// element keeps the element (add wins). Persisted shape:
/// `(adds: Vec<(elem, tag)>, tombstones: Vec<tag>)`.
#[derive(Debug)]
pub struct OrSetProvider;

impl CrdtKindProvider for OrSetProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("or-set")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(OrSetState::default())
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let (adds, tombstones): (Vec<(String, u64)>, Vec<u64>) = serde_json::from_slice(bytes)
            .map_err(|e| FnError::new(0x810, format!("or-set deserialize: {e}")))?;
        Ok(Box::new(OrSetState {
            adds: adds.into_iter().collect(),
            tombstones: tombstones.into_iter().collect(),
        }))
    }
}

#[derive(Debug, Default)]
struct OrSetState {
    /// (element, unique-tag) pairs observed via `add`.
    adds: std::collections::BTreeSet<(String, u64)>,
    /// Tags retired via `remove`. Survives as a tombstone set.
    tombstones: std::collections::BTreeSet<u64>,
}

/// Op shape on the wire: `("add", elem, tag)` or `("remove", "", tag)`.
type OrSetOp = (String, String, u64);

impl CrdtState for OrSetState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError> {
        let (action, elem, tag): OrSetOp = serde_json::from_slice(&op.bytes)
            .map_err(|e| FnError::new(0x811, format!("or-set apply: {e}")))?;
        match action.as_str() {
            "add" => {
                self.adds.insert((elem, tag));
            }
            "remove" => {
                self.tombstones.insert(tag);
            }
            other => {
                return Err(FnError::new(
                    0x811,
                    format!("or-set: unknown action `{other}`; want add|remove"),
                ));
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let o = other
            .as_any()
            .downcast_ref::<OrSetState>()
            .ok_or_else(|| FnError::new(0x812, "or-set: merge type mismatch"))?;
        self.adds.extend(o.adds.iter().cloned());
        self.tombstones.extend(o.tombstones.iter().copied());
        Ok(())
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        // Materialize the set: element present iff some add(tag) for it
        // is not in tombstones.
        let mut alive: std::collections::BTreeSet<&str> = std::collections::BTreeSet::new();
        for (elem, tag) in &self.adds {
            if !self.tombstones.contains(tag) {
                alive.insert(elem.as_str());
            }
        }
        let v: Vec<&str> = alive.into_iter().collect();
        Ok(ScalarValue::Utf8(Some(serde_json::to_string(&v).map_err(
            |e| FnError::new(0x813, format!("or-set value: {e}")),
        )?)))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        let adds: Vec<(String, u64)> = self.adds.iter().cloned().collect();
        let tombs: Vec<u64> = self.tombstones.iter().copied().collect();
        serde_json::to_vec(&(adds, tombs))
            .map_err(|e| FnError::new(0x814, format!("or-set persist: {e}")))
    }
}

// =========================================================================
// G-Counter — grow-only counter; per-replica monotonic increments.
// =========================================================================

/// Grow-only Counter (G-Counter) CRDT.
///
/// Each replica increments its own slot; the merge is per-slot `max`;
/// the value is the sum across slots. Replicas are identified by an
/// integer id encoded in the op.
#[derive(Debug)]
pub struct GCounterProvider;

impl CrdtKindProvider for GCounterProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("g-counter")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(GCounterState::default())
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let slots: std::collections::BTreeMap<u64, i64> = serde_json::from_slice(bytes)
            .map_err(|e| FnError::new(0x820, format!("g-counter deserialize: {e}")))?;
        Ok(Box::new(GCounterState { slots }))
    }
}

#[derive(Debug, Default)]
struct GCounterState {
    slots: std::collections::BTreeMap<u64, i64>,
}

/// Op shape: `(replica_id: u64, delta: i64)` where `delta >= 0`.
type GCounterOp = (u64, i64);

impl CrdtState for GCounterState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError> {
        let (replica, delta): GCounterOp = serde_json::from_slice(&op.bytes)
            .map_err(|e| FnError::new(0x821, format!("g-counter apply: {e}")))?;
        if delta < 0 {
            return Err(FnError::new(
                0x821,
                "g-counter: delta must be non-negative (grow-only)",
            ));
        }
        *self.slots.entry(replica).or_insert(0) += delta;
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let o = other
            .as_any()
            .downcast_ref::<GCounterState>()
            .ok_or_else(|| FnError::new(0x822, "g-counter: merge type mismatch"))?;
        for (replica, val) in &o.slots {
            let entry = self.slots.entry(*replica).or_insert(0);
            if val > entry {
                *entry = *val;
            }
        }
        Ok(())
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Int64(Some(self.slots.values().sum())))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        serde_json::to_vec(&self.slots)
            .map_err(|e| FnError::new(0x823, format!("g-counter persist: {e}")))
    }
}

// =========================================================================
// MV-Register — multi-value register; concurrent writes preserved.
// =========================================================================

/// Multi-Value Register CRDT.
///
/// On concurrent writes (vector-clock incomparable), retains all of
/// them; the consumer reconciles. Persisted shape:
/// `entries: Vec<(clock: u64, value: String)>` sorted by clock desc.
/// The simple variant here uses a scalar Lamport-like clock; full
/// vector-clock semantics is a follow-up.
#[derive(Debug)]
pub struct MvRegisterProvider;

impl CrdtKindProvider for MvRegisterProvider {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new("mv-register")
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(MvRegisterState::default())
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let entries: Vec<(u64, String)> = serde_json::from_slice(bytes)
            .map_err(|e| FnError::new(0x830, format!("mv-register deserialize: {e}")))?;
        Ok(Box::new(MvRegisterState { entries }))
    }
}

#[derive(Debug, Default)]
struct MvRegisterState {
    entries: Vec<(u64, String)>,
}

type MvOp = (u64, String);

impl CrdtState for MvRegisterState {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError> {
        let (clock, value): MvOp = serde_json::from_slice(&op.bytes)
            .map_err(|e| FnError::new(0x831, format!("mv-register apply: {e}")))?;
        // Drop any entries whose clock is strictly less than the new
        // op's clock (the new op observed them). Keep entries with
        // clock >= or incomparable. With a scalar Lamport clock, this
        // is "drop if strictly less".
        self.entries.retain(|(c, _)| *c >= clock);
        self.entries.push((clock, value));
        // Order desc by clock for stable iteration.
        self.entries.sort_by_key(|b| std::cmp::Reverse(b.0));
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let o = other
            .as_any()
            .downcast_ref::<MvRegisterState>()
            .ok_or_else(|| FnError::new(0x832, "mv-register: merge type mismatch"))?;
        // Union; keep entries whose clock isn't strictly dominated.
        let max_clock = self
            .entries
            .iter()
            .chain(o.entries.iter())
            .map(|(c, _)| *c)
            .max()
            .unwrap_or(0);
        let mut combined: Vec<(u64, String)> = self
            .entries
            .iter()
            .chain(o.entries.iter())
            .filter(|(c, _)| *c == max_clock)
            .cloned()
            .collect();
        combined.sort_by_key(|b| std::cmp::Reverse(b.0));
        combined.dedup();
        self.entries = combined;
        Ok(())
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        // Surface as JSON array of values (potentially multi-valued).
        let vs: Vec<&str> = self.entries.iter().map(|(_, v)| v.as_str()).collect();
        Ok(ScalarValue::Utf8(Some(
            serde_json::to_string(&vs)
                .map_err(|e| FnError::new(0x833, format!("mv-register value: {e}")))?,
        )))
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        serde_json::to_vec(&self.entries)
            .map_err(|e| FnError::new(0x834, format!("mv-register persist: {e}")))
    }
}

// ============================================================================
// RGA — Replicated Growable Array, wraps `uni_crdt::Rga<String>`.
// ============================================================================

/// RGA (Replicated Growable Array) CRDT-kind provider over `String`.
///
/// The built-in `rga` kind is the `String` instantiation of the generic
/// [`TypedRgaProvider`]: `String`'s [`RgaElement`] impl pins the kind id
/// to `"rga"` and renders the live sequence as the concatenated text
/// (the typical "collaborative text" use case). Ops are JSON-encoded
/// [`RgaOp`]s carrying pre-generated [`uuid::Uuid`]s, so two replicas
/// applying the same op sequence reach byte-identical state via
/// [`uni_crdt::CrdtMerge::merge`].
pub type RgaProvider = TypedRgaProvider<String>;

/// Operation type for [`RgaProvider`] — the `String` instantiation of
/// [`TypedRgaOp`].
pub type RgaOp = TypedRgaOp<String>;

// ============================================================================
// M5d — Generic `Rga<T>` registration.
//
// `TypedRgaProvider<T: RgaElement>` registers `Rga<T>` for arbitrary
// element types (String, i64, f64, custom user types); the built-in
// `rga` kind (`RgaProvider`) is just the `String` instantiation. The
// `RgaElement` trait bundles the
// serde bounds with two host-shaped operations: a stable `kind_id` for
// `CrdtKind` lookup, and a `values_to_scalar` adapter that maps a
// `Vec<T>` (the materialised RGA sequence) to a Cypher `ScalarValue`.
//
// The original `Insert`-op JSON-snippet trick generalises directly:
// `serde_json::to_value(&elem)` produces a JSON value for any
// `T: Serialize`, which round-trips back through `Rga<T>::deserialize`
// in `serde_json::from_value`.
// ============================================================================

/// Plug-in for `Rga<T>` element types.
///
/// Each impl declares (a) a stable [`CrdtKind`] identifier so registrar
/// dispatch can find the right provider, and (b) how to turn the
/// materialised sequence into a Cypher [`ScalarValue`] for `value()`
/// queries.
pub trait RgaElement:
    Clone + Send + Sync + 'static + serde::Serialize + serde::de::DeserializeOwned
{
    /// Stable [`CrdtKind`] identifier, e.g. `"rga.int64"`. Must be unique
    /// across `RgaElement` impls.
    fn kind_id() -> &'static str;
    /// Render the materialised sequence as a Cypher [`ScalarValue`].
    fn values_to_scalar(values: &[Self]) -> Result<ScalarValue, FnError>;
}

impl RgaElement for i64 {
    fn kind_id() -> &'static str {
        "rga.int64"
    }
    fn values_to_scalar(values: &[Self]) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Utf8(Some(
            serde_json::to_string(values)
                .map_err(|e| FnError::new(0x850, format!("typed-rga<i64> value: {e}")))?,
        )))
    }
}

impl RgaElement for f64 {
    fn kind_id() -> &'static str {
        "rga.float64"
    }
    fn values_to_scalar(values: &[Self]) -> Result<ScalarValue, FnError> {
        Ok(ScalarValue::Utf8(Some(
            serde_json::to_string(values)
                .map_err(|e| FnError::new(0x851, format!("typed-rga<f64> value: {e}")))?,
        )))
    }
}

impl RgaElement for String {
    fn kind_id() -> &'static str {
        // The original built-in `rga` kind; backed by `Rga<String>`.
        "rga"
    }
    fn values_to_scalar(values: &[Self]) -> Result<ScalarValue, FnError> {
        // Concatenate the live elements — matches the typical
        // "collaborative text" RGA use case.
        Ok(ScalarValue::Utf8(Some(values.concat())))
    }
}

/// Generic `Rga<T>` CRDT-kind provider for any [`RgaElement`].
///
/// Use instead of [`RgaProvider`] when the element type is not `String`.
/// Register with the host registrar:
///
/// ```ignore
/// r.crdt_kind(
///     CrdtKind::new(<i64 as RgaElement>::kind_id()),
///     Arc::new(TypedRgaProvider::<i64>::new()),
/// )?;
/// ```
#[derive(Debug)]
pub struct TypedRgaProvider<T: RgaElement> {
    _phantom: std::marker::PhantomData<fn() -> T>,
}

impl<T: RgaElement> TypedRgaProvider<T> {
    /// Construct a typed Rga provider.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: RgaElement> Default for TypedRgaProvider<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Operation payload for [`TypedRgaProvider`]. The `elem` field is
/// generic; serialization is the same JSON shape as [`RgaOp`] but with
/// a typed element value instead of a hard-coded `String`.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum TypedRgaOp<T> {
    /// Insert `elem` after `prev_id` (or at the head if `None`).
    Insert {
        /// Stable identifier for the inserted node.
        id: uuid::Uuid,
        /// Identifier of the node this insertion follows; `None` for head.
        prev_id: Option<uuid::Uuid>,
        /// Typed element value.
        elem: T,
        /// Lamport-style logical timestamp.
        timestamp: i64,
    },
    /// Tombstone the node with `id`.
    Delete {
        /// Identifier of the node to tombstone.
        id: uuid::Uuid,
    },
}

impl<T: RgaElement> CrdtKindProvider for TypedRgaProvider<T> {
    fn kind(&self) -> CrdtKind {
        CrdtKind::new(T::kind_id())
    }
    fn empty(&self) -> Box<dyn CrdtState> {
        Box::new(TypedRgaState::<T> {
            inner: uni_crdt::Rga::<T>::new(),
        })
    }
    fn from_persisted(&self, bytes: &[u8]) -> Result<Box<dyn CrdtState>, FnError> {
        let inner: uni_crdt::Rga<T> = serde_json::from_slice(bytes)
            .map_err(|e| FnError::new(0x852, format!("typed-rga deserialize: {e}")))?;
        Ok(Box::new(TypedRgaState::<T> { inner }))
    }
}

#[derive(Debug)]
struct TypedRgaState<T: RgaElement> {
    inner: uni_crdt::Rga<T>,
}

impl<T: RgaElement> CrdtState for TypedRgaState<T> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn apply(&mut self, op: &CrdtOp) -> Result<(), FnError> {
        let op: TypedRgaOp<T> = serde_json::from_slice(&op.bytes)
            .map_err(|e| FnError::new(0x853, format!("typed-rga apply: {e}")))?;
        match op {
            TypedRgaOp::Insert {
                id,
                prev_id,
                elem,
                timestamp,
            } => {
                // Synthesize a one-node Rga via JSON round-trip — `Rga`'s
                // `insert()` generates a fresh Uuid, which would diverge
                // across replicas; the merge path is the convergent route.
                // `serde_json::to_value` works for any `T: Serialize`, so
                // the round-trip into `Rga<T>::deserialize` lands a valid
                // one-node Rga.
                let elem_value = serde_json::to_value(&elem)
                    .map_err(|e| FnError::new(0x854, format!("typed-rga elem encode: {e}")))?;
                let snippet = serde_json::json!({
                    "nodes": {
                        id.to_string(): {
                            "id": id,
                            "elem": elem_value,
                            "origin_left": prev_id,
                            "tombstone": false,
                            "timestamp": timestamp,
                        }
                    }
                });
                let one: uni_crdt::Rga<T> = serde_json::from_value(snippet)
                    .map_err(|e| FnError::new(0x855, format!("typed-rga insert encode: {e}")))?;
                uni_crdt::CrdtMerge::merge(&mut self.inner, &one);
            }
            TypedRgaOp::Delete { id } => {
                self.inner.delete(id);
            }
        }
        Ok(())
    }
    fn merge(&mut self, other: &dyn CrdtState) -> Result<(), FnError> {
        let o = other
            .as_any()
            .downcast_ref::<TypedRgaState<T>>()
            .ok_or_else(|| FnError::new(0x856, "typed-rga: merge type mismatch"))?;
        uni_crdt::CrdtMerge::merge(&mut self.inner, &o.inner);
        Ok(())
    }
    fn value(&self) -> Result<ScalarValue, FnError> {
        let live: Vec<T> = self.inner.to_vec();
        T::values_to_scalar(&live)
    }
    fn persist(&self) -> Result<Vec<u8>, FnError> {
        serde_json::to_vec(&self.inner)
            .map_err(|e| FnError::new(0x857, format!("typed-rga persist: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lww_apply_keeps_later_timestamp() {
        let provider = LwwRegisterProvider;
        let mut state = provider.empty();
        let op1 = CrdtOp {
            bytes: serde_json::to_vec(&(5_u64, Some("hello"))).unwrap(),
        };
        let op2 = CrdtOp {
            bytes: serde_json::to_vec(&(10_u64, Some("world"))).unwrap(),
        };
        // Out-of-order delivery — LWW resolves by timestamp.
        state.apply(&op2).unwrap();
        state.apply(&op1).unwrap();
        match state.value().unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "world"),
            other => panic!("expected Utf8(Some('world')), got {other:?}"),
        }
    }

    #[test]
    fn or_set_add_remove_value() {
        let p = OrSetProvider;
        let mut s = p.empty();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&("add", "alpha", 1_u64)).unwrap(),
        })
        .unwrap();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&("add", "beta", 2_u64)).unwrap(),
        })
        .unwrap();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&("remove", "", 1_u64)).unwrap(),
        })
        .unwrap();
        match s.value().unwrap() {
            ScalarValue::Utf8(Some(json)) => {
                let v: Vec<String> = serde_json::from_str(&json).unwrap();
                assert_eq!(v, vec!["beta".to_string()]);
            }
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn or_set_persist_round_trip() {
        let p = OrSetProvider;
        let mut s = p.empty();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&("add", "x", 7_u64)).unwrap(),
        })
        .unwrap();
        let bytes = s.persist().unwrap();
        let restored = p.from_persisted(&bytes).unwrap();
        match restored.value().unwrap() {
            ScalarValue::Utf8(Some(json)) => {
                assert!(json.contains("\"x\""));
            }
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn g_counter_sums_per_replica() {
        let p = GCounterProvider;
        let mut s = p.empty();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(1_u64, 5_i64)).unwrap(),
        })
        .unwrap();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(2_u64, 3_i64)).unwrap(),
        })
        .unwrap();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(1_u64, 2_i64)).unwrap(),
        })
        .unwrap();
        match s.value().unwrap() {
            ScalarValue::Int64(Some(n)) => assert_eq!(n, 10),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn g_counter_merge_takes_max_per_replica() {
        let p = GCounterProvider;
        let mut a = p.empty();
        a.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(1_u64, 5_i64)).unwrap(),
        })
        .unwrap();
        let mut b = p.empty();
        b.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(1_u64, 7_i64)).unwrap(),
        })
        .unwrap();
        b.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(2_u64, 4_i64)).unwrap(),
        })
        .unwrap();
        a.merge(b.as_ref()).unwrap();
        match a.value().unwrap() {
            // replica 1 → max(5,7)=7; replica 2 → 4. Total 11.
            ScalarValue::Int64(Some(n)) => assert_eq!(n, 11),
            other => panic!("expected Int64, got {other:?}"),
        }
    }

    #[test]
    fn g_counter_rejects_negative_delta() {
        let p = GCounterProvider;
        let mut s = p.empty();
        let err = s
            .apply(&CrdtOp {
                bytes: serde_json::to_vec(&(1_u64, -1_i64)).unwrap(),
            })
            .unwrap_err();
        assert_eq!(err.code, 0x821);
    }

    #[test]
    fn mv_register_concurrent_writes_preserved() {
        let p = MvRegisterProvider;
        let mut a = p.empty();
        let mut b = p.empty();
        // Both replicas write at the same clock — concurrent.
        a.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(5_u64, "alice")).unwrap(),
        })
        .unwrap();
        b.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(5_u64, "bob")).unwrap(),
        })
        .unwrap();
        a.merge(b.as_ref()).unwrap();
        match a.value().unwrap() {
            ScalarValue::Utf8(Some(json)) => {
                let vs: Vec<String> = serde_json::from_str(&json).unwrap();
                assert_eq!(vs.len(), 2);
                assert!(vs.contains(&"alice".to_string()));
                assert!(vs.contains(&"bob".to_string()));
            }
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn mv_register_later_write_dominates() {
        let p = MvRegisterProvider;
        let mut s = p.empty();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(1_u64, "first")).unwrap(),
        })
        .unwrap();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&(2_u64, "second")).unwrap(),
        })
        .unwrap();
        match s.value().unwrap() {
            ScalarValue::Utf8(Some(json)) => {
                let vs: Vec<String> = serde_json::from_str(&json).unwrap();
                assert_eq!(vs, vec!["second".to_string()]);
            }
            other => panic!("expected Utf8, got {other:?}"),
        }
    }

    #[test]
    fn lww_persist_round_trip() {
        let provider = LwwRegisterProvider;
        let mut state = provider.empty();
        state
            .apply(&CrdtOp {
                bytes: serde_json::to_vec(&(7_u64, Some("v"))).unwrap(),
            })
            .unwrap();
        let bytes = state.persist().unwrap();
        let restored = provider.from_persisted(&bytes).unwrap();
        match restored.value().unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "v"),
            other => panic!("expected v, got {other:?}"),
        }
    }

    #[test]
    fn or_set_kind_identifier() {
        assert_eq!(OrSetProvider.kind(), CrdtKind::new("or-set"));
    }

    #[test]
    fn rga_two_replicas_converge_under_concurrent_inserts() {
        // Replicas A and B start from the same root insert, then
        // concurrently append. After cross-merge (apply each other's
        // ops) both surface byte-identical state, regardless of order.
        let p = RgaProvider::new();
        let id_root = uuid::Uuid::new_v4();
        let id_a = uuid::Uuid::new_v4();
        let id_b = uuid::Uuid::new_v4();
        let root_op = CrdtOp {
            bytes: serde_json::to_vec(&RgaOp::Insert {
                id: id_root,
                prev_id: None,
                elem: "A".to_owned(),
                timestamp: 1,
            })
            .unwrap(),
        };
        let a_op = CrdtOp {
            bytes: serde_json::to_vec(&RgaOp::Insert {
                id: id_a,
                prev_id: Some(id_root),
                elem: "B".to_owned(),
                timestamp: 2,
            })
            .unwrap(),
        };
        let b_op = CrdtOp {
            bytes: serde_json::to_vec(&RgaOp::Insert {
                id: id_b,
                prev_id: Some(id_root),
                elem: "C".to_owned(),
                timestamp: 3,
            })
            .unwrap(),
        };

        let mut a = p.empty();
        a.apply(&root_op).unwrap();
        a.apply(&a_op).unwrap();
        a.apply(&b_op).unwrap();

        let mut b = p.empty();
        b.apply(&root_op).unwrap();
        b.apply(&b_op).unwrap();
        b.apply(&a_op).unwrap();

        assert_eq!(a.value().unwrap(), b.value().unwrap());
        // timestamp DESC ordering puts C (ts=3) before B (ts=2).
        match a.value().unwrap() {
            ScalarValue::Utf8(Some(s)) => assert_eq!(s, "ACB"),
            other => panic!("expected ACB, got {other:?}"),
        }
    }

    #[test]
    fn rga_persist_round_trip() {
        let p = RgaProvider::new();
        let id = uuid::Uuid::new_v4();
        let mut s = p.empty();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&RgaOp::Insert {
                id,
                prev_id: None,
                elem: "hi".to_owned(),
                timestamp: 1,
            })
            .unwrap(),
        })
        .unwrap();
        let bytes = s.persist().unwrap();
        let restored = p.from_persisted(&bytes).unwrap();
        assert_eq!(restored.value().unwrap(), s.value().unwrap());
    }

    #[test]
    fn typed_rga_i64_insert_value_and_delete() {
        let p = TypedRgaProvider::<i64>::new();
        assert_eq!(p.kind(), CrdtKind::new("rga.int64"));
        let mut s = p.empty();
        let id_a = uuid::Uuid::new_v4();
        let id_b = uuid::Uuid::new_v4();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&TypedRgaOp::<i64>::Insert {
                id: id_a,
                prev_id: None,
                elem: 42,
                timestamp: 1,
            })
            .unwrap(),
        })
        .unwrap();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&TypedRgaOp::<i64>::Insert {
                id: id_b,
                prev_id: Some(id_a),
                elem: 7,
                timestamp: 2,
            })
            .unwrap(),
        })
        .unwrap();

        match s.value().unwrap() {
            ScalarValue::Utf8(Some(json)) => {
                let v: Vec<i64> = serde_json::from_str(&json).unwrap();
                assert!(v.contains(&42));
                assert!(v.contains(&7));
                assert_eq!(v.len(), 2);
            }
            other => panic!("expected Utf8(Some(...)), got {other:?}"),
        }

        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&TypedRgaOp::<i64>::Delete { id: id_a }).unwrap(),
        })
        .unwrap();
        match s.value().unwrap() {
            ScalarValue::Utf8(Some(json)) => {
                let v: Vec<i64> = serde_json::from_str(&json).unwrap();
                assert_eq!(v, vec![7], "tombstoned 42 must not appear");
            }
            other => panic!("expected Utf8 with single i64, got {other:?}"),
        }
    }

    #[test]
    fn typed_rga_persist_round_trip() {
        let p = TypedRgaProvider::<i64>::new();
        let id = uuid::Uuid::new_v4();
        let mut s = p.empty();
        s.apply(&CrdtOp {
            bytes: serde_json::to_vec(&TypedRgaOp::<i64>::Insert {
                id,
                prev_id: None,
                elem: 99,
                timestamp: 1,
            })
            .unwrap(),
        })
        .unwrap();
        let bytes = s.persist().unwrap();
        let restored = p.from_persisted(&bytes).unwrap();
        assert_eq!(restored.value().unwrap(), s.value().unwrap());
    }

    #[test]
    fn typed_rga_f64_distinct_kind_id() {
        let pi = TypedRgaProvider::<i64>::new();
        let pf = TypedRgaProvider::<f64>::new();
        assert_ne!(
            pi.kind(),
            pf.kind(),
            "RgaElement impls must surface distinct CrdtKinds"
        );
        assert_eq!(pf.kind(), CrdtKind::new("rga.float64"));
    }
}
