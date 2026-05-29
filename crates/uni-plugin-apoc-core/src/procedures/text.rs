// Rust guideline compliant
//! `apoc.text.*` analogue — text manipulation procedures.
//!
//! Mirrors Neo4j's `apoc.text.*` namespace. These are perf-critical
//! enough to live in `uni-plugin-apoc-core` (Rust) rather than the Lua
//! `uni-plugin-apoc-ext` companion — string transformations called per
//! row in large scans would be throughput-fatal across a Lua-host
//! boundary.
//!
//! Initial set: `text.toUpper`, `text.toLower`, `text.replace`,
//! `text.reverse`. Additional procedures (`text.split`, `text.regexGroups`,
//! `text.distance`, `text.fuzzyMatch`, etc.) land as user demand surfaces.

use std::sync::{Arc, OnceLock};

use arrow_array::{Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::scalar::ScalarValue;
use futures::stream;
use uni_plugin::traits::procedure::{
    NamedArgType, ProcedureContext, ProcedureMode, ProcedurePlugin, ProcedureSignature,
};
use uni_plugin::traits::scalar::ArgType;
use uni_plugin::{FnError, PluginError, PluginRegistrar, QName, SideEffects};

/// Register `uni.text.*` procedures into `r`.
///
/// # Errors
///
/// Returns [`PluginError::DuplicateRegistration`] if a qname is taken.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    for proc in TextProc::ALL {
        r.procedure(
            proc.qname(),
            proc.signature_cached().clone(),
            Arc::new(*proc),
        )?;
    }
    Ok(())
}

fn binary_str_bool_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Input string.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("substring"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "String to test against.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Boolean, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn unary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "Input string.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn ternary_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Input string.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("search"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Substring to find.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("replacement"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Replacement string.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn length_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![NamedArgType {
            name: smol_str::SmolStr::new("text"),
            ty: ArgType::Primitive(DataType::Utf8),
            default: None,
            doc: "Input string.".to_owned(),
        }],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn repeat_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "Input string.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("count"),
                ty: ArgType::Primitive(DataType::Int64),
                default: None,
                doc: "How many times to repeat.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Utf8, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

fn index_of_sig(docs: &str) -> ProcedureSignature {
    ProcedureSignature {
        args: vec![
            NamedArgType {
                name: smol_str::SmolStr::new("text"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "String to search in.".to_owned(),
            },
            NamedArgType {
                name: smol_str::SmolStr::new("substring"),
                ty: ArgType::Primitive(DataType::Utf8),
                default: None,
                doc: "String to find.".to_owned(),
            },
        ],
        yields: vec![Field::new("result", DataType::Int64, false)],
        mode: ProcedureMode::Read,
        side_effects: SideEffects::ReadOnly,
        retry_contract: None,
        batch_input: None,
        docs: docs.to_owned(),
    }
}

/// All text procedures via one discriminant.
#[derive(Debug, Clone, Copy)]
enum TextProc {
    ToUpper,
    ToLower,
    Replace,
    Reverse,
    Trim,
    LTrim,
    RTrim,
    Contains,
    StartsWith,
    EndsWith,
    Length,
    Repeat,
    IndexOf,
}

impl TextProc {
    const ALL: &'static [Self] = &[
        Self::ToUpper,
        Self::ToLower,
        Self::Replace,
        Self::Reverse,
        Self::Trim,
        Self::LTrim,
        Self::RTrim,
        Self::Contains,
        Self::StartsWith,
        Self::EndsWith,
        Self::Length,
        Self::Repeat,
        Self::IndexOf,
    ];

    fn qname(&self) -> QName {
        match self {
            Self::ToUpper => QName::new("apoc-core", "text.toUpper"),
            Self::ToLower => QName::new("apoc-core", "text.toLower"),
            Self::Replace => QName::new("apoc-core", "text.replace"),
            Self::Reverse => QName::new("apoc-core", "text.reverse"),
            Self::Trim => QName::new("apoc-core", "text.trim"),
            Self::LTrim => QName::new("apoc-core", "text.lTrim"),
            Self::RTrim => QName::new("apoc-core", "text.rTrim"),
            Self::Contains => QName::new("apoc-core", "text.contains"),
            Self::StartsWith => QName::new("apoc-core", "text.startsWith"),
            Self::EndsWith => QName::new("apoc-core", "text.endsWith"),
            Self::Length => QName::new("apoc-core", "text.length"),
            Self::Repeat => QName::new("apoc-core", "text.repeat"),
            Self::IndexOf => QName::new("apoc-core", "text.indexOf"),
        }
    }

    /// Canonical docstring per variant. The `register_into` strings
    /// were descriptive (e.g. "Uppercase a string.") while the
    /// `OnceLock` placeholders were terse ("Uppercase."). We keep the
    /// descriptive form.
    fn docs(&self) -> &'static str {
        match self {
            Self::ToUpper => "Uppercase a string.",
            Self::ToLower => "Lowercase a string.",
            Self::Replace => "Replace every occurrence of `search` in `text` with `replacement`.",
            Self::Reverse => "Reverse a string by Unicode scalar values.",
            Self::Trim => "Trim Unicode whitespace from both ends of a string.",
            Self::LTrim => "Trim Unicode whitespace from the left end of a string.",
            Self::RTrim => "Trim Unicode whitespace from the right end of a string.",
            Self::Contains => "Returns true if `text` contains `substring`.",
            Self::StartsWith => "Returns true if `text` starts with `prefix`.",
            Self::EndsWith => "Returns true if `text` ends with `suffix`.",
            Self::Length => "Length of a string in Unicode scalar values (chars).",
            Self::Repeat => "Repeat `text` `count` times.",
            Self::IndexOf => {
                "Byte index of the first occurrence of `substring` in `text`, or -1 if not present."
            }
        }
    }

    fn build_signature(&self) -> ProcedureSignature {
        match self {
            Self::ToUpper
            | Self::ToLower
            | Self::Reverse
            | Self::Trim
            | Self::LTrim
            | Self::RTrim => unary_sig(self.docs()),
            Self::Replace => ternary_sig(self.docs()),
            Self::Contains | Self::StartsWith | Self::EndsWith => binary_str_bool_sig(self.docs()),
            Self::Length => length_sig(self.docs()),
            Self::Repeat => repeat_sig(self.docs()),
            Self::IndexOf => index_of_sig(self.docs()),
        }
    }

    fn signature_cached(&self) -> &'static ProcedureSignature {
        static UPPER_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static LOWER_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static REPLACE_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static REVERSE_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static TRIM_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static LTRIM_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static RTRIM_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static CONTAINS_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static STARTS_WITH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static ENDS_WITH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static LENGTH_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static REPEAT_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        static INDEX_OF_SIG: OnceLock<ProcedureSignature> = OnceLock::new();
        match self {
            Self::ToUpper => UPPER_SIG.get_or_init(|| self.build_signature()),
            Self::ToLower => LOWER_SIG.get_or_init(|| self.build_signature()),
            Self::Replace => REPLACE_SIG.get_or_init(|| self.build_signature()),
            Self::Reverse => REVERSE_SIG.get_or_init(|| self.build_signature()),
            Self::Trim => TRIM_SIG.get_or_init(|| self.build_signature()),
            Self::LTrim => LTRIM_SIG.get_or_init(|| self.build_signature()),
            Self::RTrim => RTRIM_SIG.get_or_init(|| self.build_signature()),
            Self::Contains => CONTAINS_SIG.get_or_init(|| self.build_signature()),
            Self::StartsWith => STARTS_WITH_SIG.get_or_init(|| self.build_signature()),
            Self::EndsWith => ENDS_WITH_SIG.get_or_init(|| self.build_signature()),
            Self::Length => LENGTH_SIG.get_or_init(|| self.build_signature()),
            Self::Repeat => REPEAT_SIG.get_or_init(|| self.build_signature()),
            Self::IndexOf => INDEX_OF_SIG.get_or_init(|| self.build_signature()),
        }
    }
}

impl ProcedurePlugin for TextProc {
    fn signature(&self) -> &ProcedureSignature {
        self.signature_cached()
    }

    fn invoke(
        &self,
        _ctx: ProcedureContext<'_>,
        args: &[ColumnarValue],
    ) -> Result<SendableRecordBatchStream, FnError> {
        // Each branch builds a 1-row batch with the procedure's declared
        // yield schema. Boolean/Int outputs avoid the string path.
        let (schema, array): (SchemaRef, Arc<dyn Array>) = match self {
            Self::ToUpper => string_result(extract_string(args, 0)?.to_uppercase()),
            Self::ToLower => string_result(extract_string(args, 0)?.to_lowercase()),
            Self::Reverse => string_result(extract_string(args, 0)?.chars().rev().collect()),
            Self::Replace => {
                let text = extract_string(args, 0)?;
                let search = extract_string(args, 1)?;
                let replacement = extract_string(args, 2)?;
                string_result(text.replace(&search, &replacement))
            }
            Self::Trim => string_result(extract_string(args, 0)?.trim().to_owned()),
            Self::LTrim => string_result(extract_string(args, 0)?.trim_start().to_owned()),
            Self::RTrim => string_result(extract_string(args, 0)?.trim_end().to_owned()),
            Self::Contains => {
                bool_result(extract_string(args, 0)?.contains(extract_string(args, 1)?.as_str()))
            }
            Self::StartsWith => {
                bool_result(extract_string(args, 0)?.starts_with(extract_string(args, 1)?.as_str()))
            }
            Self::EndsWith => {
                bool_result(extract_string(args, 0)?.ends_with(extract_string(args, 1)?.as_str()))
            }
            Self::Length => int_result(extract_string(args, 0)?.chars().count() as i64),
            Self::Repeat => {
                let s = extract_string(args, 0)?;
                let count = extract_i64_text(args, 1)?;
                if count < 0 {
                    return Err(FnError::new(
                        FnError::CODE_TYPE_COERCION,
                        "text.repeat: count must be non-negative",
                    ));
                }
                // Cap to a sane limit to prevent OOM on pathological cases.
                let capped = (count as usize).min(1_000_000);
                string_result(s.repeat(capped))
            }
            Self::IndexOf => {
                let haystack = extract_string(args, 0)?;
                let needle = extract_string(args, 1)?;
                let idx = haystack
                    .find(needle.as_str())
                    .map(|p| p as i64)
                    .unwrap_or(-1);
                int_result(idx)
            }
        };

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array])
            .map_err(|e| FnError::new(0x701, format!("text: {e}")))?;
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(batch)]),
        )))
    }
}

fn string_result(s: String) -> (SchemaRef, Arc<dyn Array>) {
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        false,
    )]));
    let arr = Arc::new(StringArray::from(vec![s])) as Arc<dyn Array>;
    (schema, arr)
}

fn bool_result(b: bool) -> (SchemaRef, Arc<dyn Array>) {
    use arrow_array::BooleanArray;
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Boolean,
        false,
    )]));
    let arr = Arc::new(BooleanArray::from(vec![b])) as Arc<dyn Array>;
    (schema, arr)
}

fn int_result(n: i64) -> (SchemaRef, Arc<dyn Array>) {
    use arrow_array::Int64Array;
    let schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int64,
        false,
    )]));
    let arr = Arc::new(Int64Array::from(vec![n])) as Arc<dyn Array>;
    (schema, arr)
}

fn extract_i64_text(args: &[ColumnarValue], idx: usize) -> Result<i64, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("text: expected integer at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Int64(Some(v))) => Ok(*v),
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "text: integer argument required",
        )),
    }
}

fn extract_string(args: &[ColumnarValue], idx: usize) -> Result<String, FnError> {
    let arg = args.get(idx).ok_or_else(|| {
        FnError::new(
            FnError::CODE_TYPE_COERCION,
            format!("text: expected argument at position {idx}"),
        )
    })?;
    match arg {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Ok(s.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Ok(s.clone()),
        ColumnarValue::Array(arr) => {
            if let Some(a) = arr.as_any().downcast_ref::<StringArray>() {
                if a.is_empty() || a.is_null(0) {
                    Err(FnError::new(
                        FnError::CODE_UNEXPECTED_NULL,
                        "text: string argument must not be null",
                    ))
                } else {
                    Ok(a.value(0).to_owned())
                }
            } else {
                Err(FnError::new(
                    FnError::CODE_TYPE_COERCION,
                    "text: expected StringArray",
                ))
            }
        }
        _ => Err(FnError::new(
            FnError::CODE_TYPE_COERCION,
            "text: string argument required",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    async fn invoke_one(proc: TextProc, args: Vec<&str>) -> String {
        let cols: Vec<ColumnarValue> = args
            .into_iter()
            .map(|v| ColumnarValue::Scalar(ScalarValue::Utf8(Some(v.to_owned()))))
            .collect();
        let mut stream = proc.invoke(ProcedureContext::default(), &cols).unwrap();
        let batch = stream.next().await.unwrap().unwrap();
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        col.value(0).to_owned()
    }

    #[tokio::test]
    async fn to_upper_uppercases() {
        assert_eq!(invoke_one(TextProc::ToUpper, vec!["hello"]).await, "HELLO");
    }

    #[tokio::test]
    async fn to_lower_lowercases() {
        assert_eq!(invoke_one(TextProc::ToLower, vec!["HELLO"]).await, "hello");
    }

    #[tokio::test]
    async fn reverse_reverses() {
        assert_eq!(invoke_one(TextProc::Reverse, vec!["abc"]).await, "cba");
    }

    #[tokio::test]
    async fn replace_replaces_substring() {
        assert_eq!(
            invoke_one(TextProc::Replace, vec!["foo bar foo", "foo", "baz"]).await,
            "baz bar baz"
        );
    }

    #[tokio::test]
    async fn reverse_handles_unicode() {
        // Reversing by chars (Unicode scalars), not bytes.
        assert_eq!(invoke_one(TextProc::Reverse, vec!["café"]).await, "éfac");
    }
}
