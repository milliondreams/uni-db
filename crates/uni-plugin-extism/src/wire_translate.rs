//! Wire → internal type translation.
//!
//! The plugin's `register` export ships JSON wire types
//! ([`WireArgType`], [`WireFnSignature`]) that are stable across host
//! versions; the in-process plugin framework uses internal types
//! ([`uni_plugin::traits::scalar::ArgType`],
//! [`uni_plugin::traits::scalar::FnSignature`]) tied to Arrow's
//! `DataType`. This module bridges the two — once, at load time, when
//! the adapter is constructed.
//!
//! Keeping the translation here (rather than inside the adapter)
//! means the JSON contract is documented in one place, with one set
//! of supported `arrow:` primitive names that round-trips
//! deterministically.

// Rust guideline compliant

use arrow_schema::{DataType, Field};
use datafusion::logical_expr::Volatility;
use uni_plugin::adapter_common::arrow_types::argtype_to_arrow;
use uni_plugin::capability::SideEffects;
use uni_plugin::traits::aggregate::AggSignature;
use uni_plugin::traits::procedure::{NamedArgType, ProcedureMode, ProcedureSignature};
use uni_plugin::traits::scalar::{ArgType, FnSignature, NullHandling};

use crate::error::ExtismError;
use crate::exports::{WireArgType, WireFnSignature};

/// Map an Arrow primitive name (lowercase, as plugins write it) to
/// `arrow_schema::DataType`.
///
/// Supported names: `int32`, `int64`, `float32`, `float64`, `boolean`,
/// `utf8`, `binary`, `largebinary`, `date64`, `timestamp_ms`.
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] for any name outside the
///   supported set. We deliberately enumerate rather than accept
///   arbitrary Arrow names so the wire contract evolves consciously.
pub fn arrow_name_to_datatype(name: &str) -> Result<DataType, ExtismError> {
    uni_plugin::adapter_common::arrow_types::arrow_name_to_datatype(name).ok_or_else(|| {
        ExtismError::ManifestInvalid(format!("unsupported arrow primitive name: `{name}`"))
    })
}

/// Translate a wire [`WireArgType`] into the internal [`ArgType`].
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] if any primitive name is
///   unsupported (see [`arrow_name_to_datatype`]).
pub fn wire_arg_to_internal(wire: &WireArgType) -> Result<ArgType, ExtismError> {
    Ok(match wire {
        WireArgType::Primitive { arrow } => ArgType::Primitive(arrow_name_to_datatype(arrow)?),
        WireArgType::CypherValue => ArgType::CypherValue,
        WireArgType::Vector { len, element } => ArgType::Vector {
            len: *len,
            element: arrow_name_to_datatype(element)?,
        },
        WireArgType::Variadic { inner } => {
            ArgType::Variadic(Box::new(wire_arg_to_internal(inner)?))
        }
    })
}

/// Translate a wire volatility string into the DataFusion enum.
///
/// Recognized values: `"immutable"`, `"stable"`, `"volatile"`.
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] for any other input.
pub fn wire_volatility_to_internal(s: &str) -> Result<Volatility, ExtismError> {
    Ok(match s {
        "immutable" => Volatility::Immutable,
        "stable" => Volatility::Stable,
        "volatile" => Volatility::Volatile,
        other => {
            return Err(ExtismError::ManifestInvalid(format!(
                "unsupported volatility: `{other}`"
            )));
        }
    })
}

/// Translate a wire null-handling string into the internal enum.
///
/// Recognized values: `"propagate"`, `"user_handled"`.
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] for any other input.
pub fn wire_null_handling_to_internal(s: &str) -> Result<NullHandling, ExtismError> {
    Ok(match s {
        "propagate" => NullHandling::PropagateNulls,
        "user_handled" => NullHandling::UserHandled,
        other => {
            return Err(ExtismError::ManifestInvalid(format!(
                "unsupported null_handling: `{other}`"
            )));
        }
    })
}

/// Translate a full wire scalar/window signature into the internal
/// [`FnSignature`].
///
/// # Errors
///
/// Bubbles up [`ExtismError::ManifestInvalid`] from any sub-translation.
pub fn wire_fn_sig_to_internal(wire: &WireFnSignature) -> Result<FnSignature, ExtismError> {
    let args: Vec<ArgType> = wire
        .args
        .iter()
        .map(wire_arg_to_internal)
        .collect::<Result<_, _>>()?;
    let returns = wire_arg_to_internal(&wire.returns)?;
    let volatility = wire_volatility_to_internal(&wire.volatility)?;
    let null_handling = wire_null_handling_to_internal(&wire.null_handling)?;
    Ok(FnSignature {
        args,
        returns,
        volatility,
        null_handling,
    })
}

/// Translate a wire `state` [`WireArgType`] into an Arrow [`Field`] for
/// the aggregate's partial state.
///
/// The state field is always named `state` on the wire (a single opaque
/// column per partial accumulator). Only `Primitive` types are
/// supported as state; aggregates that need richer state should pack it
/// into `binary` / `largebinary` bytes.
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] for non-`Primitive` state types
///   or unsupported Arrow primitive names.
pub fn wire_state_to_field(wire: &WireArgType) -> Result<Field, ExtismError> {
    match wire {
        WireArgType::Primitive { arrow } => {
            Ok(Field::new("state", arrow_name_to_datatype(arrow)?, true))
        }
        other => Err(ExtismError::ManifestInvalid(format!(
            "aggregate state must be a Primitive Arrow type; got: {other:?}"
        ))),
    }
}

/// Translate a wire aggregate signature (per-row sig + opaque state)
/// into the internal [`AggSignature`].
///
/// The host's [`AggSignature::state_fields`] always carries a single
/// `state` field; multi-field state is packed by the plugin into the
/// state's Arrow primitive (typically `binary`).
///
/// # Errors
///
/// Bubbles up [`ExtismError::ManifestInvalid`] from any sub-translation.
pub fn wire_agg_sig_to_internal(
    wire_sig: &WireFnSignature,
    wire_state: &WireArgType,
) -> Result<AggSignature, ExtismError> {
    let args: Vec<ArgType> = wire_sig
        .args
        .iter()
        .map(wire_arg_to_internal)
        .collect::<Result<_, _>>()?;
    let returns = wire_arg_to_internal(&wire_sig.returns)?;
    let volatility = wire_volatility_to_internal(&wire_sig.volatility)?;
    let state_fields = vec![wire_state_to_field(wire_state)?];
    Ok(AggSignature {
        args,
        returns,
        state_fields,
        volatility,
        supports_partial: true,
    })
}

/// Translate a wire procedure-mode string into [`ProcedureMode`].
///
/// Recognized values: `"read"`, `"write"`, `"schema"`, `"dbms"`.
///
/// # Errors
///
/// - [`ExtismError::ManifestInvalid`] for any other input.
pub fn wire_proc_mode_to_internal(s: &str) -> Result<ProcedureMode, ExtismError> {
    Ok(match s {
        "read" => ProcedureMode::Read,
        "write" => ProcedureMode::Write,
        "schema" => ProcedureMode::Schema,
        "dbms" => ProcedureMode::Dbms,
        other => {
            return Err(ExtismError::ManifestInvalid(format!(
                "unsupported procedure mode: `{other}`"
            )));
        }
    })
}

/// Translate a wire procedure signature into [`ProcedureSignature`].
///
/// Arg and yield names are synthesized positionally (`arg0..argN`,
/// `yield0..yieldN`). A future wire revision can add named yields per
/// proposal §6.5.2; until then, plugins reference yields by position
/// (`CALL fn(x) YIELD yield0 AS result`).
///
/// # Errors
///
/// Bubbles up [`ExtismError::ManifestInvalid`] from any sub-translation.
pub fn wire_proc_sig_to_internal(
    args: &[WireArgType],
    yields: &[WireArgType],
    mode: &str,
) -> Result<ProcedureSignature, ExtismError> {
    let named_args: Vec<NamedArgType> = args
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ty = wire_arg_to_internal(w)?;
            Ok::<NamedArgType, ExtismError>(NamedArgType {
                name: format!("arg{i}").into(),
                ty,
                default: None,
                doc: String::new(),
            })
        })
        .collect::<Result<_, _>>()?;
    let yield_fields: Vec<Field> = yields
        .iter()
        .enumerate()
        .map(|(i, w)| {
            let ty = wire_arg_to_internal(w)?;
            Ok::<Field, ExtismError>(Field::new(format!("yield{i}"), argtype_to_arrow(&ty), true))
        })
        .collect::<Result<_, _>>()?;
    let mode = wire_proc_mode_to_internal(mode)?;
    Ok(ProcedureSignature {
        args: named_args,
        yields: yield_fields,
        mode,
        side_effects: SideEffects::default(),
        retry_contract: None,
        batch_input: None,
        docs: String::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn translates_primitive_names() {
        assert_eq!(arrow_name_to_datatype("int64").unwrap(), DataType::Int64);
        assert_eq!(
            arrow_name_to_datatype("float64").unwrap(),
            DataType::Float64
        );
        assert_eq!(arrow_name_to_datatype("utf8").unwrap(), DataType::Utf8);
    }

    #[test]
    fn rejects_unknown_primitive_name() {
        let err = arrow_name_to_datatype("super_int").unwrap_err();
        assert!(matches!(err, ExtismError::ManifestInvalid(_)));
    }

    #[test]
    fn translates_argtype_variants() {
        let cv = wire_arg_to_internal(&WireArgType::CypherValue).unwrap();
        assert!(matches!(cv, ArgType::CypherValue));

        let p = wire_arg_to_internal(&WireArgType::Primitive {
            arrow: "float64".to_owned(),
        })
        .unwrap();
        assert!(matches!(p, ArgType::Primitive(DataType::Float64)));

        let v = wire_arg_to_internal(&WireArgType::Vector {
            len: 128,
            element: "float32".to_owned(),
        })
        .unwrap();
        match v {
            ArgType::Vector { len, element } => {
                assert_eq!(len, 128);
                assert_eq!(element, DataType::Float32);
            }
            _ => unreachable!(),
        }

        let var = wire_arg_to_internal(&WireArgType::Variadic {
            inner: Box::new(WireArgType::Primitive {
                arrow: "int64".to_owned(),
            }),
        })
        .unwrap();
        match var {
            ArgType::Variadic(inner) => {
                assert!(matches!(*inner, ArgType::Primitive(DataType::Int64)));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn translates_volatility() {
        assert!(matches!(
            wire_volatility_to_internal("immutable").unwrap(),
            Volatility::Immutable
        ));
        assert!(matches!(
            wire_volatility_to_internal("stable").unwrap(),
            Volatility::Stable
        ));
        assert!(matches!(
            wire_volatility_to_internal("volatile").unwrap(),
            Volatility::Volatile
        ));
        assert!(wire_volatility_to_internal("immortal").is_err());
    }

    #[test]
    fn translates_null_handling() {
        assert!(matches!(
            wire_null_handling_to_internal("propagate").unwrap(),
            NullHandling::PropagateNulls
        ));
        assert!(matches!(
            wire_null_handling_to_internal("user_handled").unwrap(),
            NullHandling::UserHandled
        ));
        assert!(wire_null_handling_to_internal("zombies").is_err());
    }

    #[test]
    fn translates_full_signature() {
        let wire = WireFnSignature {
            args: vec![
                WireArgType::Primitive {
                    arrow: "float64".to_owned(),
                },
                WireArgType::Primitive {
                    arrow: "float64".to_owned(),
                },
            ],
            returns: WireArgType::Primitive {
                arrow: "float64".to_owned(),
            },
            volatility: "immutable".to_owned(),
            null_handling: "propagate".to_owned(),
        };
        let sig = wire_fn_sig_to_internal(&wire).unwrap();
        assert_eq!(sig.args.len(), 2);
        assert!(matches!(sig.returns, ArgType::Primitive(DataType::Float64)));
        assert!(matches!(sig.volatility, Volatility::Immutable));
        assert!(matches!(sig.null_handling, NullHandling::PropagateNulls));
    }
}
