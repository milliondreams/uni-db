# Proposal: Migrate Cypher UDFs to Columnar Arrow Execution

## Status Quo

Uni routes **all** Cypher operators and built-in functions through DataFusion as
`ScalarUDF` implementations. The majority go through `invoke_cypher_udf()` in
`crates/uni-query/src/query/df_udfs.rs`, which:

1. Iterates rows `0..batch_len`
2. Calls `get_value_from_array()` per row to extract `uni_common::Value` from Arrow arrays
3. Calls the function closure `Fn(&[Value]) -> Result<Value>` per row
4. Calls `values_to_array()` to collect results back into an Arrow array

This means every operation — even `a + b` on two `Int64` columns — pays
deserialize→dispatch→serialize overhead per row, defeating Arrow's columnar
advantage.

### What already works

Several UDFs bypass `invoke_cypher_udf`:

| UDF | Current Approach |
|-----|-----------------|
| `IdUdf` | Pure pass-through (`args[0].clone()`) |
| `RangeUdf` | Direct `ListBuilder<Int64Builder>` construction |
| `HasNullUdf` | Direct `null_count()` on list arrays |
| `CvToBoolUdf` | Tag-only decode from `LargeBinary` bytes |
| `CypherSortKeyUdf` | Per-row encode but direct array output |
| `StartsWithUdf/EndsWithUdf/ContainsUdf` | `invoke_cypher_string_op` — array-aware string dispatch |
| `Bitwise*Udf` | `invoke_binary/unary_bitwise_op` — direct `Int64Array` kernels |
| `CypherCompareUdf` | `try_fast_compare` fast path for typed arrays; falls back to `invoke_cypher_udf` |
| `CypherArithmeticUdf` | `try_fast_arithmetic` fast path for typed arrays; falls back to `invoke_cypher_udf` |

### Why `LargeBinary` exists

The type coercion system (`cypher_type_coerce.rs`) classifies expression pairs:

- **Known types** (both `Int64`, both `Utf8`, etc.) → native `BinaryExpr` with Arrow kernels
- **`LargeBinary` involved** → `TypeCompat::Dynamic` → routes to `_cypher_*` UDF

`LargeBinary` columns appear when:
- Properties lack a schema definition (schemaless mode)
- `Duration` values (Lance can't store `Interval(MonthDayNano)`)
- `CypherValue` typed properties
- UDF return types for polymorphic operations (e.g. `_cypher_add` returns `LargeBinary` because the result type depends on runtime values)

Properties with schema definitions get **native Arrow types** (`Int64`, `Utf8`,
`Float64`, `Boolean`, `Date32`, etc.) via `resolve_property_type()` →
`DataType::to_arrow()`.

---

### Key insight: LargeBinary columns are homogeneously typed

A `LargeBinary` column is **not** a bag of randomly mixed types. It comes from a
specific source — a single property (`n.score`), a single UDF output
(`_cypher_add(a, b)` where both inputs are Int64), or a single temporal type
(Duration). All values in the column share the same CypherValue tag byte.

The codec format is `[tag: u8][msgpack_payload: bytes]`. The tag byte gives O(1)
type identification via `peek_tag()`. This means we can:

1. **Peek the first non-null element's tag** to determine the column's homogeneous type
2. **Batch-decode the entire column** into a native typed Arrow array in a single pass
3. **Apply columnar Arrow kernels** on the native array
4. **Batch-re-encode** back to LargeBinary only if the output signature requires it

This eliminates the per-row `Value` construction/destruction overhead even for
LargeBinary columns. The only true fallback case is a genuinely heterogeneous
column (mixed tags), which is rare in practice.

---

## Foundation: `cv_batch_decode` — Columnar LargeBinary → Typed Array

Before migrating individual UDFs, we need a shared batch decoder utility.

**New function in `df_udfs.rs`** (or a new `cv_columnar.rs` module):

```rust
/// Result of batch-decoding a LargeBinary column.
enum DecodedColumn {
    Int64(Int64Array),
    Float64(Float64Array),
    Utf8(StringArray),
    Boolean(BooleanArray),
    /// Column has mixed tags or unsupported types — fall back to per-row.
    Mixed,
}

/// Batch-decode a LargeBinary array into a native typed Arrow array.
///
/// Peeks the first non-null tag to determine the homogeneous type, then
/// decodes all elements in a single pass using fast typed decoders.
/// Returns `Mixed` if any element has a different tag (rare).
fn cv_batch_decode(arr: &LargeBinaryArray) -> DFResult<DecodedColumn> {
    // 1. Find the dominant tag
    let tag = (0..arr.len())
        .find_map(|i| if arr.is_null(i) { None } else { peek_tag(arr.value(i)) })
        .unwrap_or(TAG_NULL);

    match tag {
        TAG_NULL => {
            // All nulls — return typed null array (Int64 as default)
            Ok(DecodedColumn::Int64(Int64Array::new_null(arr.len())))
        }
        TAG_INT => {
            let mut builder = Int64Builder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) { builder.append_null(); }
                else {
                    let bytes = arr.value(i);
                    if bytes[0] != TAG_INT { return Ok(DecodedColumn::Mixed); }
                    builder.append_value(decode_int(bytes).unwrap_or(0));
                }
            }
            Ok(DecodedColumn::Int64(builder.finish()))
        }
        TAG_FLOAT => {
            let mut builder = Float64Builder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) { builder.append_null(); }
                else {
                    let bytes = arr.value(i);
                    if bytes[0] != TAG_FLOAT { return Ok(DecodedColumn::Mixed); }
                    builder.append_value(decode_float(bytes).unwrap_or(0.0));
                }
            }
            Ok(DecodedColumn::Float64(builder.finish()))
        }
        TAG_STRING => {
            let mut builder = StringBuilder::with_capacity(arr.len(), arr.len() * 32);
            for i in 0..arr.len() {
                if arr.is_null(i) { builder.append_null(); }
                else {
                    let bytes = arr.value(i);
                    if bytes[0] != TAG_STRING { return Ok(DecodedColumn::Mixed); }
                    match decode_string(bytes) {
                        Some(s) => builder.append_value(&s),
                        None => builder.append_null(),
                    }
                }
            }
            Ok(DecodedColumn::Utf8(builder.finish()))
        }
        TAG_BOOL => {
            let mut builder = BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                if arr.is_null(i) { builder.append_null(); }
                else {
                    let bytes = arr.value(i);
                    if bytes[0] != TAG_BOOL { return Ok(DecodedColumn::Mixed); }
                    builder.append_value(decode_bool(bytes).unwrap_or(false));
                }
            }
            Ok(DecodedColumn::Boolean(builder.finish()))
        }
        _ => Ok(DecodedColumn::Mixed), // Node, Edge, Path, List, Map, Temporal, Vector
    }
}
```

And the inverse:

```rust
/// Batch-encode a native typed array back to LargeBinary.
fn cv_batch_encode(arr: &dyn Array) -> DFResult<LargeBinaryArray> {
    let mut builder = LargeBinaryBuilder::with_capacity(arr.len(), arr.len() * 16);
    match arr.data_type() {
        DataType::Int64 => {
            let typed = arr.as_primitive::<Int64Type>();
            for i in 0..arr.len() {
                if arr.is_null(i) { builder.append_value(encode_null()); }
                else { builder.append_value(encode_int(typed.value(i))); }
            }
        }
        DataType::Float64 => { /* encode_float */ }
        DataType::Utf8 => { /* encode_string */ }
        DataType::Boolean => { /* encode_bool */ }
        _ => { /* full Value round-trip fallback */ }
    }
    Ok(builder.finish())
}
```

This pair enables every UDF to follow a simple pattern:

```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    // 1. Batch-decode LargeBinary inputs to native arrays
    // 2. Apply columnar kernel on native arrays
    // 3. Batch-encode output if return type is LargeBinary
}
```

**File**: New module `crates/uni-query/src/query/cv_columnar.rs`, or add to
`df_udfs.rs`.

**Extended decode for temporal tags**: The `DecodedColumn` enum should also
include `Date(Date32Array)`, `LocalDateTime(TimestampNanosecondArray)`, etc.
for TAG_DATE, TAG_LOCALDATETIME, TAG_DATETIME. These use the same single-pass
decode pattern with `rmp_serde::from_slice` on `bytes[1..]` to extract the
numeric payload.

---

## Migration Strategy

The migration has three complementary tracks:

**Track A — Planner bypass**: When the planner knows both operand types at
compile time, emit native Arrow expressions instead of UDF calls. This is the
highest-impact change because it eliminates the UDF overhead entirely for
schema-typed data.

**Track B — Batch decode/encode for LargeBinary**: Build the `cv_batch_decode`
/ `cv_batch_encode` foundation, then update each UDF to: decode LargeBinary
columns into native arrays, apply columnar logic, re-encode if needed. This
handles schemaless properties at near-native speed.

**Track C — Direct Arrow kernels in UDF internals**: For UDFs already receiving
native-typed arrays (from schema-typed properties), use Arrow compute kernels
directly instead of going through `invoke_cypher_udf`.

### Priority tiers

| Tier | Description | Impact |
|------|------------|--------|
| **P0** | `cv_batch_decode` / `cv_batch_encode` foundation + Planner bypass for typed columns | Unlocks everything else; highest impact on typed data |
| **P1** | Arithmetic & comparison UDFs (batch-decode LargeBinary + extend fast paths) | Affects every WHERE/ORDER BY/expression |
| **P2** | Type conversion & math UDFs (columnar internals) | Affects TOINTEGER/TOFLOAT/ABS/CEIL/etc. |
| **P3** | String UDFs | Affects string operations |
| **P4** | List UDFs | Affects list operations |
| **P5** | Temporal extraction UDFs | Affects YEAR()/MONTH()/etc. |
| **P6** | Graph structural UDFs | Lowest impact; partially columnar via struct field extraction |

---

## P0: Planner Bypass for Typed Columns

### Current flow (typed columns still go through UDFs)

The planner in `cypher_type_coerce.rs` already handles `Same`, `NumericWidening`,
`StringCompat`, etc. with native `binary_expr`. But arithmetic operators in
`build_cypher_plus/minus/times/div` fall through to UDF calls even for known
numeric types in certain edge cases. The main opportunity is ensuring the
**arithmetic and comparison planner paths** are complete and cover all
type-compatible combinations.

### Changes

**File: `crates/uni-query/src/query/cypher_type_coerce.rs`**

Audit `build_cypher_plus`, `build_cypher_minus`, `build_cypher_times`,
`build_cypher_div`, `build_cypher_mod` to ensure they emit native
`BinaryExpr(+/-/*/÷/%)` when both sides resolve to numeric Arrow types.
Currently `build_cypher_plus` handles numeric + numeric natively, but defers to
`_cypher_add` UDF for some mixed-type cases that could be handled with `cast()`.

Specific changes:
- `build_cypher_plus`: When one side is `Int64` and the other is `Float64`, emit
  `cast(int_side, Float64) + float_side` instead of `_cypher_add` UDF.
- Same for `build_cypher_minus`, `build_cypher_times`, `build_cypher_div`,
  `build_cypher_mod`.
- For `build_cypher_comparison`: Currently correct for typed columns. Verify
  that `NumericWidening` path covers all cases (Int32 + Int64, Float32 + Float64,
  etc.).

**File: `crates/uni-query/src/query/df_expr.rs`**

In `cypher_expr_to_df`, when translating `Expr::BinaryOp`, check if both sides'
resolved DataTypes are native Arrow types before falling through to the UDF path.
The `ExprSchemable::get_type()` on the translated DfExpr can provide this info.

### Estimated impact

For queries on schema-typed data (the common case), arithmetic and comparisons
would use zero-copy Arrow kernels with SIMD autovectorization. This is the
single highest-impact change.

---

## P2: Type Conversion & Math UDFs

All UDFs in this tier follow the same pattern: handle native Arrow types
directly, and use `cv_batch_decode` for LargeBinary inputs to get a native
array, then apply the same logic.

### `ToIntegerUdf` → Columnar

**Current**: `invoke_cypher_udf` → match on `Value::Int/Float/String/Null`.

**Approach**:

```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    let arr = columnar_to_array(&args.args[0])?;
    let native = match arr.data_type() {
        DataType::Int64   => return Ok(args.args[0].clone()),  // already target type
        DataType::Float64 => arrow::compute::cast(&arr, &DataType::Int64)?,
        DataType::Utf8    => { /* iterate StringArray, str::parse::<i64>, build Int64Array */ },
        DataType::LargeBinary => {
            let lb = arr.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
            match cv_batch_decode(lb)? {
                DecodedColumn::Int64(a)   => return Ok(ColumnarValue::Array(Arc::new(a))),
                DecodedColumn::Float64(a) => arrow::compute::cast(&a, &DataType::Int64)?,
                DecodedColumn::Utf8(a)    => { /* parse string array */ },
                DecodedColumn::Boolean(a) => { /* false→0, true→1 */ },
                DecodedColumn::Mixed      => { /* rare: per-row fallback */ },
            }
        }
        _ => { /* error: unsupported type */ }
    };
    Ok(ColumnarValue::Array(native))
}
```

**Touchpoints**: `df_udfs.rs` — `ToIntegerUdf::invoke_with_args`.

**Arrow kernel**: `arrow::compute::cast(&arr, &DataType::Int64)` for Float64→Int64.
For String→Int64, iterate `StringArray` with `str::parse`.

### `ToFloatUdf` → Columnar

Same pattern as `ToIntegerUdf` but target type is `Float64`.

**Arrow kernel**: `arrow::compute::cast(&arr, &DataType::Float64)`.

**Touchpoints**: `df_udfs.rs` — `ToFloatUdf::invoke_with_args`.

### `ToBooleanUdf` → Columnar

**Current**: `invoke_cypher_udf` → match on `Value::Bool/String/Int/Null`.

**Approach**:
```
- Bool array → clone
- Int64 array → arrow_ord::cmp::neq(&arr, &Int64Array::new_null(0))  // nonzero = true
- Utf8 array → iterate, "true"→true, "false"→false, else null
- LargeBinary → cv_batch_decode → recurse on decoded type
```

**Touchpoints**: `df_udfs.rs` — `ToBooleanUdf::invoke_with_args`.

### `CypherAbsUdf` → Columnar

**Current**: `invoke_cypher_udf` → `Value::Int(i.abs())` / `Value::Float(f.abs())`.

**Approach**:
```
- Int64 array → unary map: checked_abs per element (overflow → error)
- Float64 array → unary map: f.abs()
- LargeBinary → cv_batch_decode → recurse on decoded Int64/Float64 array
```

The output type should match input type (Int64 → Int64, Float64 → Float64),
**not** LargeBinary. If the return type is currently declared as LargeBinary,
the batch result needs `cv_batch_encode` back to LargeBinary. Better: change
the UDF's `return_type` to return the input's type when it's numeric.

**Touchpoints**: `df_udfs.rs` — `CypherAbsUdf::invoke_with_args`.

### `CypherXorUdf` → Columnar

**Current**: `invoke_cypher_udf` → `eval_binary_op` with 3-valued XOR.

**Approach**: When both inputs are `BooleanArray`:
```rust
// 3-valued XOR: (l OR r) AND NOT (l AND r), with Kleene null semantics
let l_or_r = arrow_arith::boolean::or_kleene(&left, &right)?;
let l_and_r = arrow_arith::boolean::and_kleene(&left, &right)?;
let not_both = arrow_arith::boolean::not(&l_and_r)?;
let result = arrow_arith::boolean::and_kleene(&l_or_r, &not_both)?;
```

When inputs are `LargeBinary`:
```rust
let left_decoded = cv_batch_decode(left_lb)?;
let right_decoded = cv_batch_decode(right_lb)?;
// Both should decode to Boolean → apply same kernel
```

**Touchpoints**: `df_udfs.rs` — `CypherXorUdf::invoke_with_args`.

---

## P2: String UDFs

### `CypherSubstringUdf` → Columnar

**Current**: `invoke_cypher_udf` → char-based substring extraction.

**Critical note**: Cypher's `substring()` uses **character indices** (Unicode
code points), not byte indices. Arrow's `substring` kernel is byte-based.
DataFusion's `substr` function handles this correctly for UTF-8.

**Approach**: When input is `Utf8` array and indices are `Int64`:
```rust
datafusion_functions::unicode::substr(string_arr, start_arr, length_arr)
```

When input is `LargeBinary`:
```rust
match cv_batch_decode(lb_arr)? {
    DecodedColumn::Utf8(str_arr) => {
        // Apply same substr logic on the decoded StringArray
    }
    DecodedColumn::Mixed => { /* per-row fallback — rare */ }
    _ => { /* type error */ }
}
```

**Touchpoints**: `df_udfs.rs` — `CypherSubstringUdf::invoke_with_args`.

**Arrow/DF function**: `datafusion_functions::unicode::substr`.

### `CypherSplitUdf` → Columnar

**Current**: `invoke_cypher_udf` → `s.split(delimiter)` → `Value::List(strings)`.

**Approach**: When both inputs are `Utf8`:
```rust
// No native split-to-list kernel. Iterate StringArray directly — avoids Value overhead.
let mut list_builder = ListBuilder::new(StringBuilder::new());
for i in 0..arr.len() {
    if arr.is_null(i) { list_builder.append_null(); continue; }
    for part in arr.value(i).split(delimiter) { list_builder.values().append_value(part); }
    list_builder.append(true);
}
```

When inputs are `LargeBinary`: `cv_batch_decode` both to `Utf8`, then apply
same logic.

**Touchpoints**: `df_udfs.rs` — `CypherSplitUdf::invoke_with_args`.

### `CypherReverseUdf` → Columnar

**Current**: `invoke_cypher_udf` → `String: chars().rev()` / `List: iter().rev()`.

**Approach**:
- `Utf8` array → DataFusion's `reverse()` function (handles Unicode correctly)
- `LargeBinary` → `cv_batch_decode` → if Utf8, apply `reverse()`; if List type,
  apply `array_reverse`
- List arrays → `datafusion_functions_array::array_reverse`

**Touchpoints**: `df_udfs.rs` — `CypherReverseUdf::invoke_with_args`.

**DF function**: `datafusion_functions::string::reverse`,
`datafusion_functions_array::array_reverse`.

### `StartsWithUdf / EndsWithUdf / ContainsUdf` — Already Partially Columnar

These already use `invoke_cypher_string_op` which handles `StringArray` and
`LargeStringArray` directly. The remaining gap is when both inputs are
`LargeBinary` (CypherValue-encoded strings).

**Approach**: In the Array×Array branch of `invoke_cypher_string_op`, when both
arrays are `LargeBinary`, use `cv_batch_decode` to get `StringArray`s, then
apply the existing string predicate logic on the decoded arrays.

**Touchpoints**: `df_udfs.rs` — `invoke_cypher_string_op` Array×Array branch.

---

## P3: List UDFs

### `CypherSizeUdf` → Columnar

**Current**: Custom dispatch on `ColumnarValue` but still uses `ScalarValue`
intermediate for array path.

**Approach**: Direct array-type dispatch:
```
- Utf8 array → arrow::compute::kernels::length::length(arr) (byte len)
              or iterate for char_count (Cypher semantics = char count)
- List/LargeList array → offsets diff gives per-row lengths
    let offsets = list_arr.offsets();
    let lengths: Int64Array = (0..len).map(|i| offsets[i+1] - offsets[i]).collect();
- LargeBinary → per-row decode + existing logic (unavoidable for dynamic types)
```

**Touchpoints**: `df_udfs.rs` — `CypherSizeUdf::invoke_with_args`.

**Arrow kernel**: `arrow::compute::kernels::length::length` for byte length.
For char count, iterate `StringArray` with `.chars().count()`.

### `CypherHeadUdf` / `CypherLastUdf` → Columnar

**Current**: `invoke_cypher_udf` → `list.first()` / `list.last()`.

**Approach**: When input is a native `ListArray`:
```rust
// Head: extract element at index 0
datafusion_functions_array::array_element(list_arr, lit(1))  // 1-indexed
// Last: extract element at index -1
datafusion_functions_array::array_element(list_arr, lit(-1))
```

For `LargeBinary`-encoded lists, fall back to per-row decode.

**Touchpoints**: `df_udfs.rs` — `CypherHeadUdf` and `CypherLastUdf`.

**DF function**: `datafusion_functions_array::expr_fn::array_element`.

### `CypherTailUdf` → Columnar

**Current**: `invoke_cypher_udf` → `list[1..]`.

**Approach**: When input is `ListArray`:
```rust
datafusion_functions_array::array_slice(list_arr, lit(2), lit(i64::MAX))
```

**Touchpoints**: `df_udfs.rs` — `CypherTailUdf`.

**DF function**: `datafusion_functions_array::expr_fn::array_slice`.

### `CypherListConcatUdf` → Columnar

**Current**: `invoke_cypher_udf` → concatenate two `Value::List`s.

**Approach**: When both inputs are `ListArray`:
```rust
datafusion_functions_array::array_concat(vec![left_arr, right_arr])
```

The complication: Cypher `+` on lists also handles `List + Scalar` (append) and
`Scalar + List` (prepend). These cases need `array_append` / `array_prepend`.
The type coercion layer already routes `List + Scalar` to `_cypher_list_append`,
so this UDF only handles `List + List`.

**Touchpoints**: `df_udfs.rs` — `CypherListConcatUdf::invoke_with_args`.

**DF function**: `datafusion_functions_array::expr_fn::array_concat`.

### `CypherListAppendUdf` → Columnar

**Current**: `invoke_cypher_udf` → append element to list.

**Approach**: When left is `ListArray` and right is a typed scalar/array:
```rust
datafusion_functions_array::array_append(list_arr, element_arr)
// or array_prepend for element + list
```

**Touchpoints**: `df_udfs.rs` — `CypherListAppendUdf`.

**DF function**: `datafusion_functions_array::expr_fn::array_append` /
`array_prepend`.

### `CypherListSliceUdf` → Columnar

**Current**: `invoke_cypher_udf` → `list[start..end]` with negative index support.

**Approach**: When input is `ListArray` and indices are `Int64`:
```rust
// DataFusion's array_slice supports negative indices natively
datafusion_functions_array::array_slice(list_arr, start_arr, end_arr)
```

Need to verify that DataFusion's negative index semantics match Cypher's
(Cypher: negative index counts from end, e.g. `list[-2..]`).

**Touchpoints**: `df_udfs.rs` — `CypherListSliceUdf`.

**DF function**: `datafusion_functions_array::expr_fn::array_slice`.

### `CypherInUdf` → Columnar

**Current**: `invoke_cypher_udf` → iterate list with `cypher_eq` (3-valued).

**Approach**: When the list is a native `ListArray` and element is a typed array:
```rust
datafusion_functions_array::array_has(list_arr, element_arr)
```

When the list is `LargeBinary`-encoded: `cv_batch_decode` will return `Mixed`
for TAG_LIST columns (lists aren't in the fast-decode set). For this UDF,
extend `DecodedColumn` with a `List(ListArray)` variant for TAG_LIST containing
homogeneous element types, or keep per-row for heterogeneous lists.

**Caveat**: `array_has` may not implement Cypher's 3-valued null semantics
(if any list element is null and no match found, Cypher returns NULL not FALSE).
Verify and add a null-fixup pass if needed.

**Touchpoints**: `df_udfs.rs` — `CypherInUdf`.

**DF function**: `datafusion_functions_array::expr_fn::array_has`.

### `CypherListCompareUdf` → Per-Row (No Change)

Lexicographic list comparison with recursive `cypher_eq` is inherently per-row.
No Arrow kernel exists. Keep `invoke_cypher_udf`.

### `MapProjectUdf` → Per-Row (No Change)

Constructs a `Value::Map` from variadic key-value args. The output is always a
map (which Arrow represents as LargeBinary/Struct), and the logic involves
entity expansion (`__all__` key). No columnar kernel applies. Keep
`invoke_cypher_udf`.

### `MakeCypherListUdf` → Columnar

**Current**: `invoke_cypher_udf` → `Value::List(args.to_vec())`.

**Approach**: When all args have the same Arrow type:
```rust
datafusion_functions_array::make_array(args)
```

When args are `LargeBinary`: `cv_batch_decode` each arg independently. If all
decode to the same type, use `make_array` on the decoded arrays. If mixed,
fall back to per-row.

**Touchpoints**: `df_udfs.rs` — `MakeCypherListUdf`.

**DF function**: `datafusion_functions_array::expr_fn::make_array`.

---

## P4: Temporal Extraction UDFs

### `TemporalUdf` for extractors (`year`, `month`, `day`, `hour`, `minute`, `second`)

**Current**: `invoke_cypher_udf` → `eval_datetime_function` → extract component
from `TemporalValue`.

**Approach**: When input is a native Arrow temporal type:
```
- Date32 array → arrow_arith::temporal::date_part(DatePart::Year/Month/Day, arr)
- Timestamp(ns, _) array → arrow_arith::temporal::date_part(DatePart::*, arr)
- Time64(ns) array → arrow_arith::temporal::date_part(DatePart::Hour/Minute/Second, arr)
- Struct (DateTime/Time with offset) → extract nanos field, apply date_part
```

When input is `LargeBinary`: extend `cv_batch_decode` with temporal tags:
```
- TAG_DATE (11) → decode i32 payload → Date32Array → date_part
- TAG_LOCALDATETIME (18) → decode i64 payload → TimestampNanosecondArray → date_part
- TAG_DATETIME (13) → decode {nanos, offset} → TimestampNanosecondArray → date_part
- TAG_LOCALTIME (17) → decode i64 payload → Time64NanosecondArray → date_part
- TAG_TIME (12) → decode {nanos, offset} → Time64NanosecondArray → date_part
```

All temporal tags carry numeric payloads that are cheap to batch-decode (i32 or
i64 via `rmp_serde::from_slice`). The resulting native temporal arrays feed
directly into `arrow_arith::temporal::date_part`.

**Arrow kernel**: `arrow_arith::temporal::date_part(part, arr)`.

**Touchpoints**: `df_udfs.rs` — `TemporalUdf::invoke_with_args`; extend
`cv_batch_decode` with `DecodedColumn::Date(Date32Array)`,
`DecodedColumn::Timestamp(TimestampNanosecondArray)`,
`DecodedColumn::Time(Time64NanosecondArray)`.

### `TemporalUdf` for constructors (`date()`, `time()`, `datetime()`, etc.)

These accept maps, strings, other temporals, and zero args. The constructor
logic in `datetime.rs` handles dozens of input combinations.

**Approach**: For the common case (string column):
```
- Utf8 array → cv_batch_decode gives StringArray → iterate + parse ISO 8601
  → build Date32Array / TimestampNanosecondArray
```

For LargeBinary input that's already a temporal type (e.g. `date(some_datetime)`
to extract the date part): `cv_batch_decode` to temporal array → apply
`arrow::compute::cast` to target temporal type.

The general case (map args, zero args) remains per-row but benefits from
`cv_batch_decode` on any LargeBinary args.

### `TemporalUdf` for duration math (`duration.between`, `duration.inmonths`, etc.)

Cypher's Duration is a calendar-aware type (months + days + nanos) that doesn't
map to a single Arrow type. However, LargeBinary TAG_DURATION columns are
homogeneous and can be batch-decoded to three parallel Int64 arrays
(months, days, nanos). Duration math could then operate on these arrays
columnar:

```
duration.indays(d) → d.months * 30 + d.days + (d.nanos / 86_400_000_000_000)
```

This is pure arithmetic on Int64 arrays. Extend `DecodedColumn` with
`Duration { months: Int64Array, days: Int64Array, nanos: Int64Array }`.

**Touchpoints**: `cv_batch_decode` extension; `df_udfs.rs` — `TemporalUdf`.

---

## P5: Graph Structural UDFs

These operate on entity maps/structs and are tied to Uni's graph representation.
Columnar optimization is possible but requires understanding the physical
layout of entity columns.

### `TypeUdf` → Columnar

**Current**: `invoke_cypher_udf` → extract `_type` from `Value::Map`.

**Approach**: When input is a `StructArray` with a `_type` field:
```rust
let type_col = struct_arr.column_by_name("_type")?;
// type_col is already a Utf8 array
Ok(ColumnarValue::Array(type_col.clone()))
```

When input is `LargeBinary`, fall back to per-row decode.

**Touchpoints**: `df_udfs.rs` — `TypeUdf::invoke_with_args`.

### `LabelsUdf` → Columnar

Same pattern as `TypeUdf` but extracts the `_labels` field (a `List<Utf8>`).

```rust
let labels_col = struct_arr.column_by_name("_labels")?;
Ok(ColumnarValue::Array(labels_col.clone()))
```

**Touchpoints**: `df_udfs.rs` — `LabelsUdf::invoke_with_args`.

### `KeysUdf` → Per-Row (No Change)

Requires filtering out `_`-prefixed internal keys and null values from entity
maps. The entity decomposition makes this complex — properties are spread across
multiple struct fields. Keep `invoke_cypher_udf`.

### `PropertiesUdf` → Per-Row (No Change)

Same issue as `KeysUdf` — must reconstruct a filtered property map from
decomposed struct fields. Keep `invoke_cypher_udf`.

### `IndexUdf` → Partially Columnar

**Approach**: When container is a `StructArray` and index is a `Scalar(Utf8)`:
```rust
// Property access on a known field name — direct column extraction
let field_name = index_scalar.as_str();
if let Some(col) = struct_arr.column_by_name(field_name) {
    return Ok(ColumnarValue::Array(col.clone()));
}
```

When container is a `ListArray` and index is `Int64`:
```rust
datafusion_functions_array::array_element(list_arr, index_arr)
```

When container is `LargeBinary`: `cv_batch_decode` — if it decodes to a
homogeneous type (e.g. all TAG_LIST), the decoded ListArray can use
`array_element`. If TAG_MAP, decode to a struct and extract field. If `Mixed`,
per-row fallback.

**Touchpoints**: `df_udfs.rs` — `IndexUdf::invoke_with_args`.

### `NodesUdf` / `RelationshipsUdf` → Partially Columnar

When input is a `StructArray` with `nodes`/`relationships` fields:
```rust
let col = struct_arr.column_by_name("nodes")?;  // or "relationships"
Ok(ColumnarValue::Array(col.clone()))
```

**Touchpoints**: `df_udfs.rs` — `NodesUdf` and `RelationshipsUdf`.

### `StartNodeUdf` / `EndNodeUdf` → Per-Row (No Change)

These search through variadic node arguments to find the one matching the
edge's `_src_vid`/`_dst_vid`. This is a join-like operation that doesn't map to
a columnar kernel. Keep `invoke_cypher_udf`.

---

## P1 (continued): Comparison and Arithmetic Fast Paths

### Extending `try_fast_compare`

**Current coverage**: Handles `LargeBinary × Int64`, `LargeBinary × Float64`,
`LargeBinary × Utf8`, `LargeBinary × LargeBinary` (with tag decode).

**Missing**: `Int64 × Float64` (should cast and compare natively), `Utf8 × Utf8`
(should already be native, but verify), `LargeBinary × Boolean`.

**Approach**: Add fast paths for:
```rust
// Both native numeric types — use arrow::compute::cast + cmp
(DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64) => {
    let left_f64 = arrow::compute::cast(left_arr, &DataType::Float64)?;
    let right_f64 = arrow::compute::cast(right_arr, &DataType::Float64)?;
    Some(ColumnarValue::Array(arrow_ord::cmp::eq(&left_f64, &right_f64)?))
}
```

**Touchpoints**: `df_udfs.rs` — `try_fast_compare` function.

### Extending `try_fast_arithmetic`

**Current coverage**: `LargeBinary × Int64`, `LargeBinary × Float64`,
`Int64 × Int64`.

**Missing**: `Int64 × Float64` (cast + native arith), `Float64 × Float64`
(already should be native, verify).

**Approach**: Same cast-and-operate pattern.

**Touchpoints**: `df_udfs.rs` — `try_fast_arithmetic` function.

---

## Similarity / Vector UDFs

### `SimilarToUdf` / `VectorSimilarityUdf` → Columnar

**Current**: `invoke_cypher_udf` → extract two `Value::List<f32>` vectors →
compute cosine similarity.

**Approach**: When both inputs are `FixedSizeList<Float32>` arrays:
```rust
let left_values = left_fsl.values().as_primitive::<Float32Type>();
let right_values = right_fsl.values().as_primitive::<Float32Type>();
let dim = left_fsl.value_length() as usize;

let mut results = Float64Builder::with_capacity(left_fsl.len());
for i in 0..left_fsl.len() {
    let l = &left_values[i*dim..(i+1)*dim];
    let r = &right_values[i*dim..(i+1)*dim];
    let dot: f64 = l.iter().zip(r).map(|(a,b)| *a as f64 * *b as f64).sum();
    let mag_l: f64 = l.iter().map(|a| (*a as f64).powi(2)).sum::<f64>().sqrt();
    let mag_r: f64 = r.iter().map(|a| (*a as f64).powi(2)).sum::<f64>().sqrt();
    results.append_value(if mag_l == 0.0 || mag_r == 0.0 { 0.0 } else { dot / (mag_l * mag_r) });
}
```

When inputs are `LargeBinary`: extend `cv_batch_decode` with
`DecodedColumn::Vector(FixedSizeListArray)` for TAG_VECTOR (16). The codec
stores vectors as msgpack arrays of f32, which can be batch-decoded into a
`FixedSizeList<Float32>` array, then the above kernel applies.

This avoids `Value` construction/destruction per row. Could further optimize
with SIMD via `std::simd` (nightly) for the dot product.

**Touchpoints**: `df_udfs.rs` — `SimilarToUdf` and `VectorSimilarityUdf`;
extend `cv_batch_decode` with TAG_VECTOR support.

---

## Encoding Bridge UDFs

### `ListToCvUdf` / `ScalarToCvUdf` → Use `cv_batch_encode`

**Current**: `invoke_cypher_udf` → `Ok(vals[0].clone())` (identity function).

These exist solely to coerce Arrow types to `LargeBinary` for downstream
consumption by polymorphic UDFs. The per-row decode→clone→encode is pure waste.

**Approach**: Use `cv_batch_encode` directly:
```rust
fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
    match &args.args[0] {
        // Already LargeBinary — pass through
        ColumnarValue::Array(arr) if arr.data_type() == &DataType::LargeBinary
            => Ok(args.args[0].clone()),

        // Native type → cv_batch_encode (uses fast encode_int/encode_float/etc.)
        ColumnarValue::Array(arr)
            => Ok(ColumnarValue::Array(Arc::new(cv_batch_encode(arr.as_ref())?))),

        ColumnarValue::Scalar(s) => { /* existing logic */ }
    }
}
```

This replaces the full `Value` round-trip with direct tag+msgpack encoding using
the fast typed encoders (`encode_int`, `encode_float`, `encode_string`,
`encode_bool`).

**Touchpoints**: `df_udfs.rs` — `ListToCvUdf` and `ScalarToCvUdf`.

---

## `CypherSortKeyUdf` → Optimized Encoding

**Current**: Per-row `get_value_from_array` → `encode_cypher_sort_key` → append
to `LargeBinaryArray`. Already avoids `invoke_cypher_udf` but still does full
`Value` deserialization.

**Approach**: For native Arrow types, encode sort keys directly from the typed
array without going through `Value`:
```rust
match arr.data_type() {
    DataType::Int64 => {
        // Sort key for Int64: type tag + big-endian XOR-flip bytes
        let int_arr = arr.as_primitive::<Int64Type>();
        for i in 0..len {
            if arr.is_null(i) { keys.push(Some(NULL_SORT_KEY)); }
            else { keys.push(Some(encode_int_sort_key(int_arr.value(i)))); }
        }
    }
    DataType::Utf8 => {
        let str_arr = arr.as_string::<i32>();
        for i in 0..len {
            if arr.is_null(i) { keys.push(Some(NULL_SORT_KEY)); }
            else { keys.push(Some(encode_string_sort_key(str_arr.value(i)))); }
        }
    }
    // ... Float64, Boolean
    _ => { /* existing Value-based path */ }
}
```

**Touchpoints**: `df_udfs.rs` — `CypherSortKeyUdf::invoke_with_args`.

---

## Implementation Order

```
Phase 1 (P0): cv_batch_decode / cv_batch_encode foundation
  Files: new cv_columnar.rs (or additions to df_udfs.rs)
  Contents: DecodedColumn enum, cv_batch_decode, cv_batch_encode
  Risk: Low — pure addition, no existing code changes
  Impact: Unlocks all subsequent phases
  Tags supported: TAG_NULL, TAG_INT, TAG_FLOAT, TAG_STRING, TAG_BOOL
  Extended later: TAG_DATE, TAG_LOCALTIME, TAG_TIME, TAG_LOCALDATETIME,
                  TAG_DATETIME, TAG_DURATION, TAG_VECTOR

Phase 2 (P0): Planner bypass for typed arithmetic/comparison
  Files: cypher_type_coerce.rs, df_expr.rs
  Risk: Low — extends existing native expression paths
  Impact: Highest — affects every query on schema-typed data

Phase 3 (P1): Arithmetic & comparison UDFs — batch decode LargeBinary
  Files: df_udfs.rs (try_fast_compare, try_fast_arithmetic)
  Approach: cv_batch_decode LargeBinary inputs → native arrays → existing
            fast-path kernels. Eliminates per-row fallback for homogeneous
            LargeBinary columns.
  Risk: Low — extends existing fast-path pattern
  Impact: Highest — affects every expression on schemaless data

Phase 4 (P2): Type conversion + abs + encoding bridge UDFs
  Files: df_udfs.rs (ToInteger, ToFloat, ToBoolean, CypherAbs, CypherXor,
         ListToCv, ScalarToCv, CypherSortKey)
  Approach: Array type dispatch + cv_batch_decode for LargeBinary
  Risk: Low — self-contained UDF changes with Mixed fallback
  Impact: Medium

Phase 5 (P3): String UDFs
  Files: df_udfs.rs (CypherSubstring, CypherSplit, CypherReverse,
         invoke_cypher_string_op LargeBinary branch)
  Approach: cv_batch_decode to StringArray → DataFusion string kernels
  Risk: Low-medium — Unicode semantics must match Cypher spec
  Impact: Medium

Phase 6 (P4): List UDFs
  Files: df_udfs.rs (CypherSize, CypherHead, CypherLast, CypherTail,
         CypherListConcat, CypherListAppend, CypherListSlice,
         CypherIn, MakeCypherList)
  Approach: Native ListArray → DataFusion array functions; extend
            cv_batch_decode with TAG_LIST for LargeBinary lists
  Risk: Medium — DataFusion array function semantics must match Cypher
  Impact: Medium

Phase 7 (P5): Temporal extraction + duration math
  Files: df_udfs.rs (TemporalUdf), cv_columnar.rs
  Approach: Extend cv_batch_decode with TAG_DATE → Date32Array,
            TAG_DATETIME → TimestampNanosecondArray, etc.
            Duration → three parallel Int64 arrays for arithmetic
  Risk: Medium — must handle struct-encoded DateTime/Time correctly
  Impact: Medium

Phase 8 (P6): Graph structural UDFs
  Files: df_udfs.rs (TypeUdf, LabelsUdf, IndexUdf, NodesUdf, RelationshipsUdf)
  Approach: StructArray field extraction; LargeBinary entity columns
            rare (entities usually decomposed to typed columns)
  Risk: Medium — depends on physical struct layout assumptions
  Impact: Low

Phase 9: Vector similarity UDFs
  Files: df_udfs.rs (SimilarToUdf, VectorSimilarityUdf), cv_columnar.rs
  Approach: Extend cv_batch_decode with TAG_VECTOR →
            FixedSizeList<Float32>; direct dot product on f32 slices
  Risk: Low — self-contained numeric computation
  Impact: High for vector search workloads
```

## UDFs Staying Per-Row (Only for `DecodedColumn::Mixed` fallback)

With `cv_batch_decode`, even LargeBinary columns get columnar treatment in the
common case (homogeneous tags). The only true per-row fallback is
`DecodedColumn::Mixed` — a column with genuinely mixed value types. This is rare
in practice (would require a schemaless property storing both integers and
strings in the same column).

UDFs that remain per-row **regardless of input type**:

| UDF | Reason |
|-----|--------|
| `KeysUdf` | Requires filtering internal `_`-prefixed keys from entity maps |
| `PropertiesUdf` | Must reconstruct filtered property map from decomposed struct |
| `StartNodeUdf` / `EndNodeUdf` | Join-like search across variadic node args |
| `CypherListCompareUdf` | Recursive lexicographic comparison with `cypher_eq` |
| `MapProjectUdf` | Entity expansion (`__all__` key) and map construction |
| `CustomScalarUdf` | User-registered `Fn(&[Value])` — signature is per-row by design |

## Verification Strategy

Each phase should:
1. Add benchmarks comparing before/after for the modified UDFs
2. Run the full Cypher TCK (Technology Compatibility Kit) test suite
3. Run `EXPLAIN` / `PROFILE` on representative queries to verify the planner
   emits native expressions where expected
4. Verify null-handling semantics match Cypher's 3-valued logic

## Files Modified Summary

| File | Phases | Nature of Changes |
|------|--------|------------------|
| `crates/uni-query/src/query/cv_columnar.rs` (new) | 1, 7, 9 | `cv_batch_decode`, `cv_batch_encode`, `DecodedColumn` enum |
| `crates/uni-query/src/query/cypher_type_coerce.rs` | 2 | Extend `build_cypher_*` functions for typed bypass |
| `crates/uni-query/src/query/df_expr.rs` | 2 | Type-check before UDF fallback |
| `crates/uni-query/src/query/df_udfs.rs` | 3–9 | Replace `invoke_with_args` bodies; extend fast paths |
| `crates/uni-common/src/cypher_value_codec.rs` | 1 | Possibly add batch-oriented helpers (e.g. `decode_int_raw(&[u8]) -> i64` without tag check) |
