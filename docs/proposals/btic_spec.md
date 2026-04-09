# BTIC Specification v1.1

## Binary Temporal Interval Codec — Complete Specification

**Status:** Draft
**Version:** 1.1.0
**Date:** 2026-04-08

---

## 1. Overview

BTIC (Binary Temporal Interval Codec) is a fixed-width binary data type for
representing temporal intervals with explicit granularity and epistemic metadata.
It is a first-class data type in Uni-DB, usable in schema definitions, Cypher
queries, and Locy rules.

BTIC provides:

- A total ordering via byte comparison (`memcmp`) for B-tree and LSM key storage
- Efficient vectorized execution through columnar decomposition into Arrow arrays
- Per-bound granularity preservation (the system remembers "this was a year, not
  365 days of milliseconds")
- Per-bound epistemic certainty (definite, approximate, uncertain, unknown)
- Allen's interval algebra as native query operators

Every temporal value in BTIC is an interval. There are no separate "instant" or
"timestamp" types. An instant is an interval of width one millisecond.

### 1.1 Role in the System

BTIC is a **valid-time** data type. It records when a fact is true in the real
world ("Alice worked at Acme from March 2020 to June 2024").

Transaction time (when the system recorded the fact) is handled separately by
Uni-DB's existing MVCC versioning and `VERSION AS OF` / `TIMESTAMP AS OF` queries.

Together, BTIC properties and Uni-DB's MVCC provide full bitemporal capability:

| Dimension | Mechanism | Managed by |
|-----------|-----------|------------|
| Valid time | BTIC property on nodes/edges | Application (explicitly set) |
| Transaction time | MVCC version + wall-clock timestamp | System (automatic) |

---

## 2. Definitions

**Interval.** An ordered pair `[lo, hi)` representing the half-open set of all
millisecond ticks `t` such that `lo <= t < hi`.

**Tick.** One millisecond on the canonical timeline. The atomic unit of time in BTIC.

**Epoch.** `1970-01-01T00:00:00.000Z` in the proleptic Gregorian calendar. Tick zero.

**Timeline value.** A signed 64-bit integer representing milliseconds relative to epoch.
Negative values represent times before epoch. The proleptic Gregorian calendar is used
for all date arithmetic, including dates before its historical adoption.

**Sentinel.** A reserved timeline value with special semantics:

| Sentinel      | Value        | Meaning                     |
|---------------|--------------|-----------------------------|
| `NEG_INF`     | `INT64_MIN`  | Negative infinity (-inf)    |
| `POS_INF`     | `INT64_MAX`  | Positive infinity (+inf)    |

**Finite value.** Any timeline value `v` satisfying `INT64_MIN < v < INT64_MAX`.

**Granularity.** A descriptor indicating the calendrical unit from which a bound was
derived. Granularity is metadata; it does not participate in comparison.

**Certainty.** A descriptor indicating the epistemic confidence in a bound value.
Certainty is metadata; it does not participate in comparison.

---

## 3. Timeline Model

### 3.1 Domain

The timeline is the set of all `int64` values, interpreted as milliseconds since epoch.

Representable range for finite values:

```
Minimum finite:  INT64_MIN + 1  =  -9_223_372_036_854_775_807
Maximum finite:  INT64_MAX - 1  =   9_223_372_036_854_775_806
```

This spans approximately +/-292.5 million years from epoch, which exceeds any
practical temporal requirement including deep geological and cosmological time.

### 3.2 Calendar

All conversions between human-readable dates and timeline values use the proleptic
Gregorian calendar with no leap second adjustments. UTC is the reference timezone.

Rationale: Leap seconds are unpredictable and non-monotonic. Since 1972, only 27 leap
seconds have been inserted, representing less than 27 seconds of cumulative drift.
Excluding them preserves the invariant that the timeline is a simple linear mapping
from `int64` to physical time at millisecond granularity. This is the same choice
made by POSIX `time_t`, Java `Instant`, and most database timestamp types. Systems
requiring leap-second awareness must handle the conversion at their boundary, outside
the BTIC encoding layer.

### 3.3 Precision

The timeline has millisecond precision. Sub-millisecond events are rounded to the
nearest millisecond at encoding time using round-half-even (banker's rounding).

### 3.4 Year Numbering

BTIC uses astronomical year numbering:

```
1 BCE  =  year  0
2 BCE  =  year -1
3 BCE  =  year -2
```

This avoids the discontinuity of historical year numbering (which has no year 0)
and ensures that year arithmetic is continuous.

---

## 4. Interval Semantics

### 4.1 Half-Open Convention

All BTIC intervals are half-open: `[lo, hi)`.

The interval contains every tick `t` such that `lo <= t < hi`.

### 4.2 Interval Classes

| Class              | Condition                                      | Example                        |
|--------------------|-------------------------------------------------|--------------------------------|
| Proper finite      | `lo` finite, `hi` finite, `lo < hi`            | `[t1, t2)` where `t1 < t2`    |
| Instant            | `lo` finite, `hi = lo + 1`                     | `[t, t+1)`                     |
| Left-unbounded     | `lo = NEG_INF`, `hi` finite                    | `[-inf, t)`                    |
| Right-unbounded    | `lo` finite, `hi = POS_INF`                    | `[t, +inf)`                    |
| Fully-unbounded    | `lo = NEG_INF`, `hi = POS_INF`                 | `[-inf, +inf)`                 |

An instant is a special case of a proper finite interval. It is not a distinct type.

### 4.3 Empty Intervals

Empty intervals (`lo >= hi`) are **not representable** in BTIC. Any operation that
would produce an empty interval (e.g., intersection of non-overlapping intervals)
returns NULL at the query layer. The BTIC encoding cannot represent emptiness.

Rationale: Allowing empty intervals would break the total ordering guarantee and
require special-case handling in every operator. The type represents *things that
happened* (or could have happened), not the absence of events.

---

## 5. Granularity

### 5.1 Purpose

Granularity records the calendrical unit from which a bound was derived. It enables
downstream systems to recover the original intent of an encoding (e.g., "this was
specified as a year, not a range of 31,536,000,000 arbitrary milliseconds").

Granularity is **metadata only**. It does not affect comparison, ordering, or any
operator defined in this specification. Two intervals with identical `lo` and `hi`
but different granularity are ordered by their meta field but are semantically
distinct values.

### 5.2 Encoding

Granularity is a 4-bit unsigned integer:

| Code | Granularity  | Expansion width                           |
|------|-------------|-------------------------------------------|
| 0x0  | millisecond | 1 ms                                      |
| 0x1  | second      | 1,000 ms                                  |
| 0x2  | minute      | 60,000 ms                                 |
| 0x3  | hour        | 3,600,000 ms                              |
| 0x4  | day         | Start of day to start of next day (UTC)   |
| 0x5  | month       | Start of month to start of next month     |
| 0x6  | quarter     | Start of quarter to start of next quarter |
| 0x7  | year        | Start of year to start of next year       |
| 0x8  | decade      | Start of decade to start of next decade   |
| 0x9  | century     | Start of century to start of next century |
| 0xA  | millennium  | Start of millennium to start of next       |
| 0xB-0xF | reserved | Must not be used in v1                  |

Note: For granularities day and coarser, the expansion width is variable because
calendar units are not uniform (months have 28-31 days, years have 365-366 days).
The expansion always uses the proleptic Gregorian calendar.

### 5.3 Expansion Rule

Granularity defines how a human-level temporal expression is converted to `[lo, hi)`.

Given a temporal expression with granularity `G` anchored at calendrical unit `U`:

```
lo = first millisecond tick of unit U
hi = first millisecond tick of the unit immediately following U at granularity G
```

**Examples:**

| Expression      | Granularity | lo (human-readable)           | hi (human-readable)           |
|-----------------|-------------|-------------------------------|-------------------------------|
| 1985            | year        | 1985-01-01T00:00:00.000Z     | 1986-01-01T00:00:00.000Z     |
| March 1985      | month       | 1985-03-01T00:00:00.000Z     | 1985-04-01T00:00:00.000Z     |
| 1985-03-15      | day         | 1985-03-15T00:00:00.000Z     | 1985-03-16T00:00:00.000Z     |
| 14:30 on a date | minute      | ...T14:30:00.000Z             | ...T14:31:00.000Z             |
| 500 BCE         | year        | -0499-01-01T00:00:00.000Z    | -0498-01-01T00:00:00.000Z    |

### 5.4 Per-Bound Granularity

Each bound carries its own granularity. This supports intervals where the two bounds
were specified at different precisions.

**Example:** "From March 1985 to 2020-07-15T14:30Z"

```
lo            = 1985-03-01T00:00:00.000Z
hi            = 2020-07-15T14:31:00.000Z
lo_granularity = month  (0x5)
hi_granularity = minute (0x2)
```

### 5.5 Granularity of Explicit Intervals

When a user specifies an explicit interval with two distinct endpoints (as opposed
to a single granular expression like "the year 1985"), the granularity of each bound
reflects the precision of that bound's specification.

When a user specifies a single granular expression, both bounds receive that
expression's granularity.

### 5.6 Granularity Inference from Literals

Granularity is determined by the least significant **non-omitted** component of
a temporal literal, not the least significant non-zero component:

| Literal              | Granularity  | Rationale                         |
|----------------------|-------------|-----------------------------------|
| `1985`               | year        | Only year specified               |
| `1985-03`            | month       | Month is least significant        |
| `1985-03-15`         | day         | Day is least significant          |
| `1985-03-15T14Z`     | hour        | Hour is least significant         |
| `1985-03-15T14:30Z`  | minute      | Minute is least significant       |
| `1985-03-15T14:30:00Z` | second   | Seconds explicitly stated         |
| `1985-03-15T14:30:00.000Z` | millisecond | Milliseconds explicitly stated |

Note: `T14:30:00Z` is second granularity even though the seconds value is zero,
because the seconds component is explicitly present in the literal.

---

## 6. Certainty

### 6.1 Purpose

Certainty records the epistemic confidence in a bound value. It enables reasoning
about fuzzy or approximate temporal data without contaminating the deterministic
comparison layer.

Certainty is **metadata only**. It does not affect comparison, ordering, or any
operator defined in this specification.

### 6.2 Encoding

Certainty is a 2-bit unsigned integer, defined per bound:

| Code | Level        | Semantics                                                |
|------|-------------|----------------------------------------------------------|
| 0b00 | definite    | Bound is known precisely to stated granularity.          |
| 0b01 | approximate | Bound is estimated; true value may differ by +/-1 unit   |
|      |             | of the bound's granularity.                              |
| 0b10 | uncertain   | Bound is weakly estimated; true value may differ          |
|      |             | significantly from stated value.                         |
| 0b11 | unknown     | Bound is not meaningfully known. The stored `lo`/`hi`    |
|      |             | value represents a best guess or arbitrary default.      |

### 6.3 Interpretation

Certainty informs downstream consumers but has no normative effect on BTIC operations.

A consumer performing probabilistic temporal reasoning MAY interpret certainty as
follows:

- **approximate (0b01):** The true bound lies within `[bound - delta, bound + delta]`
  where `delta` is one unit of the bound's granularity.
- **uncertain (0b10):** The true bound could be significantly different. The consumer
  should define its own tolerance.
- **unknown (0b11):** The bound is essentially unconstrained. Consumers may choose
  to treat this as equivalent to +/-inf for their purposes.

These interpretations are advisory. BTIC itself treats all bounds as their stored
values for all operations.

---

## 7. Binary Layout

### 7.1 Packed Canonical Format (24 bytes)

The canonical binary representation of a BTIC value is exactly 24 bytes (192 bits):

```
Offset  Size    Field           Encoding
------  ------  --------------- ----------------------------------------
0       8       lo_encoded      big-endian(lo XOR 0x8000_0000_0000_0000)
8       8       hi_encoded      big-endian(hi XOR 0x8000_0000_0000_0000)
16      8       meta            big-endian(meta_word)
```

The XOR with `0x8000_0000_0000_0000` (sign-bit flip) converts signed two's complement
integers to an unsigned representation that preserves order under `memcmp`.

All three fields are stored in big-endian (network) byte order so that `memcmp` on
the 24-byte buffer produces the same result as the defined comparison semantics.

### 7.2 Meta Word Layout (64 bits)

```
Bit(s)   Width  Field            Type
-------  -----  ---------------  -----
63..60   4      lo_granularity   uint
59..56   4      hi_granularity   uint
55..54   2      lo_certainty     uint
53..52   2      hi_certainty     uint
51..48   4      version          uint
47..32   16     flags            uint
31..0    32     reserved         uint
```

**version:** Must be `0x0` for this specification.

**flags:** Reserved for future use. Must be `0x0000` in this version. Defined flag bits will
be specified in future versions.

**reserved:** Must be `0x0000_0000` in this version.

### 7.3 Columnar Decomposition

For columnar/vectorized execution, a BTIC value decomposes into three Arrow arrays:

```
lo[]   : int64[]   -- raw signed millisecond values (NOT sign-flipped)
hi[]   : int64[]   -- raw signed millisecond values (NOT sign-flipped)
meta[] : uint64[]  -- meta word as unsigned 64-bit integer
```

The columnar representation uses native-endian encoding and does NOT apply the
sign-flip XOR. The sign-flip and big-endian encoding exist solely for the packed
canonical format to enable `memcmp` ordering.

### 7.4 Arrow Storage Type

BTIC values are stored as `FixedSizeBinary(24)` in Arrow and Lance. This preserves
the packed canonical format directly in columnar storage, enabling `memcmp`-based
comparison without decoding.

For vectorized predicate evaluation, the packed format is decomposed into the three
columnar arrays described in 7.3. This decomposition is performed at query execution
time, not at storage time.

---

## 8. Invariants

Every valid BTIC value must satisfy all of the following invariants. An implementation
must reject or refuse to construct any value that violates these rules.

### INV-1: Bound Ordering

```
lo < hi
```

This follows from the half-open convention: `[lo, hi)` must contain at least one tick.
The minimum valid interval is `[t, t+1)` (one millisecond, i.e., an instant).

### INV-2: Sentinel Exclusivity

```
lo = INT64_MIN  if and only if  the lower bound is -inf
hi = INT64_MAX  if and only if  the upper bound is +inf
```

`INT64_MIN` and `INT64_MAX` must not appear as finite bound values.

Corollary: for any finite lower bound, `INT64_MIN < lo`. For any finite upper bound,
`hi < INT64_MAX`.

### INV-3: Finite Bound Range

For finite bounds:

```
INT64_MIN < lo <= INT64_MAX - 2
INT64_MIN + 2 <= hi < INT64_MAX
```

These follow from INV-1 and INV-2 combined.

### INV-4: Reserved Bits

```
version = 0x0
flags   = 0x0000
reserved = 0x0000_0000
```

All reserved and version bits must be zero for this version. Implementations must reject
values with non-zero reserved bits unless they recognize the version.

### INV-5: Granularity Range

```
lo_granularity <= 0xA
hi_granularity <= 0xA
```

Values 0xB through 0xF are reserved and must not be used.

### INV-6: Sentinel Granularity

When a bound is a sentinel (+/-inf), its granularity and certainty are meaningless.
They must be set to zero:

```
if lo = INT64_MIN:  lo_granularity = 0x0, lo_certainty = 0b00
if hi = INT64_MAX:  hi_granularity = 0x0, hi_certainty = 0b00
```

This prevents multiple encodings of semantically identical values and ensures
canonical form.

### INV-7: Uniqueness of Representation

For any given combination of `(lo, hi, lo_granularity, hi_granularity,
lo_certainty, hi_certainty)`, there is exactly one valid 24-byte encoding.

This follows from the fixed layout, the zeroed reserved bits, and INV-6.

---

## 9. Comparison Semantics

### 9.1 Total Order

BTIC defines a strict total order over all valid values. The ordering is:

```
Primary:    lo   ascending
Secondary:  hi   ascending
Tertiary:   meta ascending (unsigned)
```

Two BTIC values are equal if and only if all 24 bytes are identical.

### 9.2 memcmp Equivalence

**GUARANTEE:** For any two valid BTIC values `a` and `b` in packed canonical form:

```
sign(memcmp(a, b)) == sign(btic_compare(a, b))
```

This is the foundational property that enables direct use in B-trees, LSM key
comparators, and binary search without decoding.

**Proof sketch:**

1. `lo_encoded` occupies bytes 0..7. The sign-flip XOR maps `INT64_MIN -> 0` and
   `INT64_MAX -> UINT64_MAX`, preserving signed order as unsigned order. Big-endian
   ensures `memcmp` compares most-significant bytes first. Therefore `memcmp` on
   bytes 0..7 agrees with signed comparison of `lo`.

2. If bytes 0..7 are equal (i.e., `lo` values are equal), `memcmp` proceeds to
   bytes 8..15. By the same argument, this compares `hi` correctly.

3. If bytes 0..15 are equal, `memcmp` proceeds to bytes 16..23. `meta` is an
   unsigned integer stored big-endian, so `memcmp` compares it correctly as an
   unsigned value.

### 9.3 Ordering Semantics

The total order sorts intervals by their start point first, then by their end point.
For intervals sharing the same `[lo, hi)`, the ordering by meta provides determinism
but has no temporal meaning.

This ordering has a useful property: for a sorted sequence of BTIC values, all
intervals that could contain a given point `t` form a contiguous subsequence
starting at the first value where `lo <= t`. A scan forward from that point,
terminating when `lo > t`, visits all candidates.

### 9.4 Equality

Two BTIC values are **bytewise equal** if all 24 bytes are identical.

Two BTIC values are **temporally equivalent** if `lo1 == lo2` and `hi1 == hi2`
(ignoring meta). Temporal equivalence is a weaker relation that may be useful for
deduplication or interval algebra, but is distinct from equality.

### 9.5 Standard Comparison Operators

The following operators are available in Cypher queries on Btic-typed properties:

```cypher
a.valid_at = b.valid_at       -- bytewise equal (all 24 bytes)
a.valid_at <> b.valid_at      -- not bytewise equal
a.valid_at < b.valid_at       -- total order: lo asc, hi asc, meta asc
a.valid_at > b.valid_at
a.valid_at <= b.valid_at
a.valid_at >= b.valid_at
```

These operators use the total order defined in 9.1. They compare intervals as
sorted values (by start point, then end point), NOT as temporal containment.
For temporal relationship queries, use the operators defined in 14.

---

## 10. Encoding Procedures

### 10.1 Encode (Logical -> Packed Canonical)

Given logical values `(lo: int64, hi: int64, meta: uint64)`:

```
Step 1: Validate invariants INV-1 through INV-7.
Step 2: Compute lo_encoded = lo XOR 0x8000_0000_0000_0000.
Step 3: Compute hi_encoded = hi XOR 0x8000_0000_0000_0000.
Step 4: Write lo_encoded as 8 bytes, big-endian, at offset 0.
Step 5: Write hi_encoded as 8 bytes, big-endian, at offset 8.
Step 6: Write meta as 8 bytes, big-endian, at offset 16.
```

### 10.2 Decode (Packed Canonical -> Logical)

Given a 24-byte buffer:

```
Step 1: Read 8 bytes at offset 0 as big-endian uint64 -> lo_encoded.
Step 2: Read 8 bytes at offset 8 as big-endian uint64 -> hi_encoded.
Step 3: Read 8 bytes at offset 16 as big-endian uint64 -> meta.
Step 4: Compute lo = lo_encoded XOR 0x8000_0000_0000_0000 (reinterpret as int64).
Step 5: Compute hi = hi_encoded XOR 0x8000_0000_0000_0000 (reinterpret as int64).
Step 6: Validate invariants INV-1 through INV-7.
```

### 10.3 Granularity Expansion (Human Input -> Logical)

Given a human temporal expression with identified granularity:

```
Step 1: Identify the calendrical unit U and granularity G.
Step 2: Compute lo = first tick of U (in ms since epoch, UTC).
Step 3: Compute hi = first tick of the next unit at granularity G.
Step 4: Set lo_granularity = G, hi_granularity = G.
Step 5: Set certainty as appropriate (default: definite).
Step 6: Construct meta word per 7.2 layout.
Step 7: Proceed to 10.1.
```

### 10.4 Packed <-> Columnar Conversion

**Pack -> Columnar** (for N intervals):

```
for i in 0..N:
    decode packed[i] per 10.2 -> (lo, hi, meta)
    lo_col[i] = lo
    hi_col[i] = hi
    meta_col[i] = meta
```

**Columnar -> Pack** (for N intervals):

```
for i in 0..N:
    encode (lo_col[i], hi_col[i], meta_col[i]) per 10.1 -> packed[i]
```

These conversions are purely mechanical and lossless.

---

## 11. Canonicalization Rules

These rules define how inputs are normalized to canonical form. They are applied
once, at encoding time. All subsequent operations assume canonical values.

### CAN-1: Granularity Expansion

A temporal expression specified at a non-millisecond granularity must be expanded
to its `[lo, hi)` millisecond bounds before encoding. After expansion, the interval
is a first-class millisecond interval; granularity is retained as metadata only.

### CAN-2: Bound Normalization

If an input is specified with inclusive/exclusive bounds in non-canonical form, it
must be converted to half-open `[lo, hi)`:

```
(lo, hi]  ->  [lo + 1, hi + 1)
(lo, hi)  ->  [lo + 1, hi)
[lo, hi]  ->  [lo, hi + 1)
[lo, hi)  ->  [lo, hi)          (no change)
```

All adjustments are in milliseconds (1 tick).

### CAN-3: Sentinel Normalization

If a bound is specified as "open" or "unbounded":

```
lower bound unbounded  ->  lo = INT64_MIN
upper bound unbounded  ->  hi = INT64_MAX
```

The corresponding granularity and certainty must be set to zero (INV-6).

### CAN-4: Instant Normalization

A single point in time at millisecond granularity `t` is encoded as:

```
lo = t
hi = t + 1
lo_granularity = millisecond (0x0)
hi_granularity = millisecond (0x0)
```

A single point at a coarser granularity is expanded per CAN-1, not encoded as
an instant. "The year 1985" is a year-long interval, not an instant.

### CAN-5: Reserved Bits

All reserved, flags, and version bits must be set to zero per INV-4.

### CAN-6: Validation

After canonicalization, all invariants (8) must hold. If they do not, the input
is invalid and must be rejected. There is no "best effort" encoding.

---

## 12. Well-Formedness Summary

A BTIC value is well-formed if and only if:

1. It is exactly 24 bytes.
2. Decoding produces `(lo, hi, meta)` satisfying all invariants.
3. Reserved bits are zero.
4. Granularity codes are in the defined range.
5. Sentinel bounds have zeroed granularity and certainty.

An implementation must provide a `validate(bytes[24]) -> bool` function that
checks all conditions.

---

## 13. Uni-DB Type System Integration

### 13.1 Data Type

BTIC is registered as `DataType::Btic` in the schema type system.

Schema declaration in Cypher:

```cypher
CREATE LABEL Event
  PROPERTY name String
  PROPERTY valid_at Btic
```

### 13.2 Value Representation

In the runtime value system:

```rust
enum TemporalValue {
    // ... existing variants ...
    Btic { lo: i64, hi: i64, meta: u64 },
}

enum TemporalType {
    // ... existing variants ...
    Btic,
}
```

The `Value::Temporal(TemporalValue::Btic { .. })` variant carries the logical
(decoded) representation at runtime. Conversion to/from packed canonical format
happens at the storage boundary.

### 13.3 Binary Codec

Tagged binary encoding for the CypherValue codec:

```
TAG_BTIC = 19   (next available tag)
Format:  [0x13][24 bytes packed canonical]
Total:   25 bytes on wire
```

### 13.4 Arrow Storage

BTIC maps to `ArrowDataType::FixedSizeBinary(24)` in Arrow and Lance. This
preserves the packed canonical format directly, enabling `memcmp` comparison
in storage indexes without decoding.

### 13.5 Literal Syntax

BTIC literals are written as strings assigned to `Btic`-typed properties. The
system infers the interval bounds and granularity from the string format.

BTIC uses ISO 8601 interval notation where applicable, with extensions for
certainty and BCE dates.

**Single granular expression** (both bounds derived from one expression):

```cypher
// Year
CREATE (:Event {valid_at: '1985'})
// -> [1985-01-01, 1986-01-01) granularity=year

// Month
CREATE (:Event {valid_at: '1985-03'})
// -> [1985-03-01, 1985-04-01) granularity=month

// Day
CREATE (:Event {valid_at: '1985-03-15'})
// -> [1985-03-15, 1985-03-16) granularity=day

// Full timestamp (instant at millisecond granularity)
CREATE (:Event {valid_at: '1985-03-15T14:30:00.000Z'})
// -> [that ms, that ms + 1) granularity=millisecond
```

**Two-bound interval** (ISO 8601 solidus notation):

```cypher
// Two dates
CREATE (:Event {valid_at: '1985-03/2024-06'})
// -> [1985-03-01, 2024-07-01) lo_gran=month, hi_gran=month

// Mixed granularity
CREATE (:Event {valid_at: '1985/2024-06-15'})
// -> [1985-01-01, 2024-06-16) lo_gran=year, hi_gran=day
```

**Unbounded intervals:**

```cypher
// Right-unbounded (ongoing)
CREATE (:Event {valid_at: '2020-03/'})
// -> [2020-03-01, +inf)

// Left-unbounded
CREATE (:Event {valid_at: '/2024-06'})
// -> [-inf, 2024-07-01)
```

**Certainty annotation** (prefix `~` for approximate, `?` for uncertain,
`??` for unknown). In two-bound intervals, the prefix applies to the component
on its side of the solidus:

```cypher
// Approximate year (both bounds)
CREATE (:Event {valid_at: '~1985'})
// -> [1985-01-01, 1986-01-01) lo_certainty=approximate, hi_certainty=approximate

// Approximate start, definite end
CREATE (:Event {valid_at: '~1985/2024-06'})
// -> lo_certainty=approximate, hi_certainty=definite

// Definite start, approximate end
CREATE (:Event {valid_at: '1985/~2024-06'})
// -> lo_certainty=definite, hi_certainty=approximate

// Both bounds approximate
CREATE (:Event {valid_at: '~1985/~2024-06'})
// -> lo_certainty=approximate, hi_certainty=approximate

// Uncertain
CREATE (:Event {valid_at: '?500 BCE'})
// -> certainty=uncertain

// Unknown start, approximate end
CREATE (:Event {valid_at: '??1985/~2024'})
// -> [1985-01-01, 2025-01-01) lo_certainty=unknown, hi_certainty=approximate
```

**BCE dates:**

```cypher
CREATE (:Event {valid_at: '500 BCE'})
// -> astronomical year -499: [-0499-01-01, -0498-01-01)
```

### 13.6 Type Coercion

When a string literal is written to a `Btic`-typed property, the system:

1. Checks if the string matches BTIC literal patterns (defined in 13.5)
2. Parses the bounds, granularity, and certainty
3. Applies canonicalization rules (11)
4. Encodes to packed canonical format for storage

When a `Btic` value is read and displayed, the system produces a human-readable
string representation preserving granularity:

```
[1985-01-01, 1986-01-01) ~year
[1985-03-01, 2024-07-01) month/month
[-inf, 2024-07-01) /month
```

---

## 14. Operations

### 14.1 Accessors

Functions that decompose a BTIC value into its parts.

| Function | Return type | Semantics |
|----------|-------------|-----------|
| `btic_lo(v)` | Timestamp | Lower bound as a DateTime value. NULL if -inf. |
| `btic_hi(v)` | Timestamp | Upper bound as a DateTime value. NULL if +inf. |
| `btic_duration(v)` | Integer | `hi - lo` in milliseconds. NULL if either bound is infinite. |
| `btic_granularity(v)` | String | Granularity name of the lower bound (`'year'`, `'month'`, etc.). If bounds differ, returns lower bound's granularity. |
| `btic_lo_granularity(v)` | String | Granularity name of the lower bound. |
| `btic_hi_granularity(v)` | String | Granularity name of the upper bound. |
| `btic_certainty(v)` | String | Certainty of the lower bound (`'definite'`, `'approximate'`, `'uncertain'`, `'unknown'`). If bounds differ, returns the least certain. |
| `btic_lo_certainty(v)` | String | Certainty of the lower bound. |
| `btic_hi_certainty(v)` | String | Certainty of the upper bound. |
| `btic_is_instant(v)` | Boolean | True if `hi == lo + 1`. |
| `btic_is_unbounded(v)` | Boolean | True if either bound is +/-inf. |
| `btic_is_finite(v)` | Boolean | True if both bounds are finite. |

### 14.2 Temporal Predicates

Functions that test temporal relationships between intervals and/or points. These
are the primary query operators for BTIC values.

**Point containment:**

| Function | Semantics |
|----------|-----------|
| `btic_contains_point(interval, point)` | `interval.lo <= point < interval.hi` |

**Interval relationships (practical set):**

| Function | Semantics | Covers Allen relations |
|----------|-----------|----------------------|
| `btic_contains(a, b)` | `a.lo <= b.lo AND b.hi <= a.hi` | during-inverse, starts-inverse, finishes-inverse, equals |
| `btic_overlaps(a, b)` | `a.lo < b.hi AND b.lo < a.hi` | All 11 non-disjoint Allen relations |
| `btic_before(a, b)` | `a.hi <= b.lo` | before, meets |
| `btic_after(a, b)` | `b.hi <= a.lo` | after, met-by |
| `btic_meets(a, b)` | `a.hi == b.lo` | meets |
| `btic_adjacent(a, b)` | `a.hi == b.lo OR b.hi == a.lo` | meets, met-by |
| `btic_disjoint(a, b)` | `a.hi <= b.lo OR b.hi <= a.lo` | before, after |
| `btic_equals(a, b)` | `a.lo == b.lo AND a.hi == b.hi` | Temporal equivalence (ignores meta) |
| `btic_starts(a, b)` | `a.lo == b.lo AND a.hi < b.hi` | starts |
| `btic_during(a, b)` | `b.lo < a.lo AND a.hi < b.hi` | during (strict containment) |
| `btic_finishes(a, b)` | `a.hi == b.hi AND b.lo < a.lo` | finishes |

All predicates return Boolean. All operate on the decoded `lo`/`hi` values
(metadata is not considered). NULL input produces NULL output.

**Note on `btic_before`:** This is `before ∪ meets` in Allen's terminology (it
returns true when `a` ends exactly where `b` starts). For strict Allen "before"
(with a gap between intervals), use `btic_before(a, b) AND NOT btic_meets(a, b)`.

**Note on `btic_equals` vs `=`:** `btic_equals(a, b)` tests temporal equivalence
(same `lo` and `hi`, ignoring metadata). The standard `=` operator tests bytewise
equality of all 24 bytes, including granularity and certainty. Two intervals can be
`btic_equals` but not `=` if they represent the same time span with different
metadata (e.g., `'1985'` at year granularity vs `'1985-01-01/1985-12-31'` at day
granularity).

**Vectorized implementation:** All predicates decompose to 1-2 comparisons on
`int64` arrays. For example, `btic_overlaps(a, b)` becomes `a.lo < b.hi AND b.lo < a.hi`
on the columnar decomposition. DataFusion can vectorize these with no decoding
overhead.

### 14.3 Set Operations

Functions that combine two intervals into a new interval.

| Function | Return type | Semantics |
|----------|-------------|-----------|
| `btic_intersection(a, b)` | Btic or NULL | `[max(a.lo, b.lo), min(a.hi, b.hi))`. Returns NULL if the intervals are disjoint (the result would be empty). |
| `btic_span(a, b)` | Btic | `[min(a.lo, b.lo), max(a.hi, b.hi))`. The smallest interval containing both inputs. Always valid. |
| `btic_gap(a, b)` | Btic or NULL | If disjoint and non-adjacent: the interval between them. `[min(a.hi, b.hi), max(a.lo, b.lo))`. Returns NULL if the intervals overlap or are adjacent (i.e., if `max(a.lo, b.lo) <= min(a.hi, b.hi)`). |

**Granularity of results:** Set operation results inherit the granularity of the
bound that was selected. For `btic_intersection`, the result's `lo` comes from
`max(a.lo, b.lo)` — if `a.lo > b.lo`, the result inherits `a`'s lo_granularity.
When both bounds are equal, the finer granularity (lower code value = higher
precision) is inherited. The same principle applies to hi bounds.

**Certainty of results:** Set operation results inherit the least certain value of
the two input bounds at each position (max of the certainty codes, since higher
codes indicate less certainty).

### 14.4 Aggregation

Functions that operate on collections of BTIC values (for GROUP BY, FOLD, or
analytical queries).

| Function | Return type | Semantics |
|----------|-------------|-----------|
| `btic_min(collection)` | Btic | Earliest-starting interval (first in total order). |
| `btic_max(collection)` | Btic | Latest-starting interval (last in total order). |
| `btic_span_agg(collection)` | Btic | Bounding interval: `[min(lo), max(hi))` across all values. |
| `btic_count_at(collection, point)` | Integer | Number of intervals in the collection that contain the point. |

### 14.5 Cypher Query Examples

```cypher
-- Find all events that were happening on a specific date
MATCH (e:Event)
WHERE btic_contains_point(e.valid_at, datetime('2022-06-15'))
RETURN e.name

-- Find overlapping events
MATCH (a:Event), (b:Event)
WHERE btic_overlaps(a.valid_at, b.valid_at)
  AND a <> b
RETURN a.name, b.name

-- Find events that started before 2020
MATCH (e:Event)
WHERE btic_lo(e.valid_at) < datetime('2020-01-01')
RETURN e.name, btic_granularity(e.valid_at)

-- Find ongoing events (right-unbounded)
MATCH (e:Event)
WHERE NOT btic_is_finite(e.valid_at)
  AND btic_lo(e.valid_at) IS NOT NULL
RETURN e.name

-- Compute the time span covering all events
MATCH (e:Event)
RETURN btic_span_agg(e.valid_at) AS total_span

-- Find events with approximate temporal data
MATCH (e:Event)
WHERE btic_certainty(e.valid_at) <> 'definite'
RETURN e.name, btic_certainty(e.valid_at) AS confidence
```

---

## 15. Locy Integration

BTIC predicates are available in Locy rules as standard WHERE conditions. This
enables temporal reasoning with recursive inference, probability, and explanation.

### 15.1 Temporal Predicates in Rules

```
CREATE RULE concurrent_events AS
    MATCH (a:Event), (b:Event)
    WHERE btic_overlaps(a.valid_at, b.valid_at)
      AND a <> b
    YIELD KEY a, KEY b

QUERY concurrent_events RETURN *
```

### 15.2 Recursive Temporal Chains

```
-- Base case: direct causal link with temporal ordering
CREATE RULE causal_chain AS
    MATCH (a:Event)-[:CAUSES]->(b:Event)
    WHERE btic_before(a.valid_at, b.valid_at)
    YIELD KEY a, KEY b

-- Recursive case: transitive causal chain
CREATE RULE causal_chain AS
    MATCH (a:Event)-[:CAUSES]->(mid:Event)
    WHERE mid IS causal_chain TO b
      AND btic_before(a.valid_at, mid.valid_at)
    YIELD KEY a, KEY b
```

### 15.3 Certainty-Aware Reasoning

```
-- High-confidence temporal facts only
CREATE RULE confirmed_timeline AS
    MATCH (e:Event)
    WHERE btic_certainty(e.valid_at) = 'definite'
    YIELD KEY e

-- Lower probability for uncertain temporal data
CREATE RULE possible_timeline AS
    MATCH (e:Event)
    WHERE btic_certainty(e.valid_at) = 'uncertain'
    PROB 0.3
    YIELD KEY e
```

### 15.4 Temporal Contradiction Detection

```
-- Find entities with overlapping facts that assert different values
CREATE RULE temporal_contradiction AS
    MATCH (a:Fact)-[:ABOUT]->(entity:Entity)<-[:ABOUT]-(b:Fact)
    WHERE btic_overlaps(a.valid_at, b.valid_at)
      AND a.attribute = b.attribute
      AND a.value <> b.value
      AND a <> b
    YIELD KEY a, KEY b, entity

-- Explain the contradiction
EXPLAIN RULE temporal_contradiction RETURN *
```

### 15.5 FOLD with Temporal Aggregation

```
-- Count concurrent events per entity
CREATE RULE concurrency_count AS
    MATCH (e:Event)-[:INVOLVES]->(entity:Entity)
    FOLD n = COUNT(*)
    WHERE n >= 3
    YIELD KEY entity, KEY e.valid_at, n AS concurrent_count
```

---

## 16. Edge Cases and Decisions

### 16.1 Maximum Finite Interval

The largest representable finite interval is:

```
lo = INT64_MIN + 1
hi = INT64_MAX - 1
```

Duration: approximately 584.5 million years.

Note: this is distinct from `[-inf, +inf)`, which is a different interval class
(fully-unbounded) with different semantics.

### 16.2 Adjacency

Two intervals `[a, b)` and `[c, d)` are **adjacent** if and only if `b == c` or
`d == a`. Under half-open semantics, adjacent intervals share no ticks and can be
merged into `[min(a,c), max(b,d))` without gaps.

### 16.3 Duration Computation

For a finite interval `[lo, hi)`:

```
duration_ms = hi - lo
```

No adjustment needed. This is a direct consequence of the half-open convention.

For intervals with sentinel bounds, duration is undefined (NULL).

### 16.4 Ambiguous Literals

When a string like `'1985-03-15'` is assigned to a `Btic`-typed property, it is
interpreted as a day interval `[1985-03-15, 1985-03-16)`. The same string assigned
to a `Date`-typed property produces a date value. Context (the property's declared
type) disambiguates.

When a string is used in a context without type information (e.g., a standalone
expression), it is NOT automatically interpreted as a BTIC value. The property
type must be `Btic` for BTIC parsing to apply.

### 16.5 NULL Handling

All BTIC functions follow SQL NULL semantics: if any input is NULL, the output is
NULL. This applies to both predicates (return NULL, not false) and constructors.

---

## 17. Versioning and Evolution

The 4-bit version field in meta supports up to 16 layout versions. Version 0 is
this specification. Future versions may:

- Define flag bits (provenance tags, source annotation).
- Extend the granularity enum into the reserved range (0xB-0xF).
- Add sub-millisecond precision modes.
- Support alternative calendar systems via flags.

Implementations encountering an unrecognized version must reject the value rather
than attempting partial interpretation. This prevents silent data corruption across
version boundaries.

---

## Appendix A: Reference Constants

```
EPOCH            = 1970-01-01T00:00:00.000Z
NEG_INF          = -9_223_372_036_854_775_808  (INT64_MIN,  0x8000_0000_0000_0000)
POS_INF          =  9_223_372_036_854_775_807  (INT64_MAX,  0xFFFF_FFFF_FFFF_FFFF)
MIN_FINITE       = -9_223_372_036_854_775_807  (INT64_MIN + 1)
MAX_FINITE       =  9_223_372_036_854_775_806  (INT64_MAX - 1)
SIGN_FLIP_MASK   = 0x8000_0000_0000_0000
META_VERSION_V1  = 0x0
TAG_BTIC         = 0x13  (19 decimal)
```

---

## Appendix B: Test Vectors

### B.1 Unix Epoch Instant

Input: `'1970-01-01T00:00:00.000Z'` assigned to a Btic property.

```
lo   = 0
hi   = 1
meta = 0x0000_0000_0000_0000
       (lo_gran=0x0, hi_gran=0x0, lo_cert=00, hi_cert=00, ver=0, flags=0, rsv=0)

Packed:
  bytes[0..7]   = BE(0 XOR 0x8000000000000000)  = 0x80 00 00 00 00 00 00 00
  bytes[8..15]  = BE(1 XOR 0x8000000000000000)  = 0x80 00 00 00 00 00 00 01
  bytes[16..23] = 0x00 00 00 00 00 00 00 00
```

### B.2 The Year 1985

Input: `'1985'` assigned to a Btic property.

```
lo   = 1985-01-01T00:00:00.000Z = 473_385_600_000 ms
hi   = 1986-01-01T00:00:00.000Z = 504_921_600_000 ms
meta:
  lo_granularity = 0x7 (year)
  hi_granularity = 0x7 (year)
  lo_certainty   = 0b00 (definite)
  hi_certainty   = 0b00 (definite)
  meta_word      = 0x7700_0000_0000_0000

Packed:
  bytes[0..7]   = BE(473385600000 XOR 0x8000000000000000)
                = 0x80 00 00 6E 37 FB 04 00
  bytes[8..15]  = BE(504921600000 XOR 0x8000000000000000)
                = 0x80 00 00 75 8F AC 30 00
  bytes[16..23] = 0x77 00 00 00 00 00 00 00
```

### B.3 Negative Infinity to March 1985

Input: `'/1985-03'` assigned to a Btic property.

```
lo   = INT64_MIN  (sentinel)
hi   = 1985-04-01T00:00:00.000Z = 481_161_600_000 ms
meta:
  lo_granularity = 0x0 (zeroed per INV-6)
  hi_granularity = 0x5 (month)
  lo_certainty   = 0b00 (zeroed per INV-6)
  hi_certainty   = 0b00 (definite)
  meta_word      = 0x0500_0000_0000_0000

Packed:
  bytes[0..7]   = 0x00 00 00 00 00 00 00 00
  bytes[8..15]  = BE(481161600000 XOR 0x8000000000000000)
                = 0x80 00 00 70 07 77 5C 00
  bytes[16..23] = 0x05 00 00 00 00 00 00 00
```

### B.4 500 BCE (Approximate)

Input: `'~500 BCE'` assigned to a Btic property.

```
Astronomical year -499. Year -499 is NOT a leap year.

lo   = -0499-01-01T00:00:00.000Z  (proleptic Gregorian)
     = -77_914_137_600_000 ms
hi   = -0498-01-01T00:00:00.000Z
     = -77_882_601_600_000 ms
meta:
  lo_granularity = 0x7 (year)
  hi_granularity = 0x7 (year)
  lo_certainty   = 0b01 (approximate)
  hi_certainty   = 0b01 (approximate)
  meta_word      = 0x7750_0000_0000_0000

  Breakdown: 0111 0111 01 01 0000 ...
             gran  gran cr cr ver

Duration: 365 days (31_536_000_000 ms)

Packed:
  bytes[0..7]   = 0x7F FF B9 23 33 81 60 00
  bytes[8..15]  = 0x7F FF B9 2A 8B 32 8C 00
  bytes[16..23] = 0x77 50 00 00 00 00 00 00
```

### B.5 Fully Unbounded

Input: `'/'` assigned to a Btic property.

```
lo   = INT64_MIN
hi   = INT64_MAX
meta = 0x0000_0000_0000_0000  (all zeroed per INV-6)

Packed:
  bytes[0..7]   = 0x00 00 00 00 00 00 00 00
  bytes[8..15]  = 0xFF FF FF FF FF FF FF FF
  bytes[16..23] = 0x00 00 00 00 00 00 00 00
```

### B.6 Mixed-Granularity Interval

Input: `'1985-03/2024-06-15'` assigned to a Btic property.

```
lo   = 1985-03-01T00:00:00.000Z = 478_483_200_000 ms
hi   = 2024-06-16T00:00:00.000Z = 1_718_496_000_000 ms
meta:
  lo_granularity = 0x5 (month)
  hi_granularity = 0x4 (day)
  lo_certainty   = 0b00 (definite)
  hi_certainty   = 0b00 (definite)
  meta_word      = 0x5400_0000_0000_0000

Packed:
  bytes[0..7]   = 0x80 00 00 6F 67 D2 38 00
  bytes[8..15]  = 0x80 00 01 90 1E 57 F8 00
  bytes[16..23] = 0x54 00 00 00 00 00 00 00
```

### B.7 Ongoing Event (Right-Unbounded)

Input: `'2020-03/'` assigned to a Btic property.

```
lo   = 2020-03-01T00:00:00.000Z = 1_583_020_800_000 ms
hi   = INT64_MAX (sentinel)
meta:
  lo_granularity = 0x5 (month)
  hi_granularity = 0x0 (zeroed per INV-6)
  lo_certainty   = 0b00 (definite)
  hi_certainty   = 0b00 (zeroed per INV-6)
  meta_word      = 0x5000_0000_0000_0000

Packed:
  bytes[0..7]   = 0x80 00 01 70 93 64 78 00
  bytes[8..15]  = 0xFF FF FF FF FF FF FF FF
  bytes[16..23] = 0x50 00 00 00 00 00 00 00
```

---

## Appendix C: Allen's 13 Interval Relations Reference

For completeness, the 13 mutually exclusive relations between intervals
`A = [a1, a2)` and `B = [b1, b2)`:

```
Relation          Condition                     Inverse
-----------       --------------------------    ----------------
before            a2 <= b1                      after
meets             a2 == b1                      met-by
overlaps          a1 < b1 < a2 < b2            overlapped-by
starts            a1 == b1 AND a2 < b2          started-by
during            b1 < a1 AND a2 < b2           contains
finishes          a2 == b2 AND b1 < a1          finished-by
equals            a1 == b1 AND a2 == b2         (self-inverse)
```

Every pair of non-empty intervals satisfies exactly one of these 13 relations.
The practical predicates in 14.2 are defined as unions of these relations for
usability.

---

## Appendix D: Design Rationale Summary

| Decision               | Choice           | Primary reason                              |
|------------------------|------------------|---------------------------------------------|
| Interval convention    | Half-open [lo,hi)| Clean duration math; standard in systems    |
| Width                  | 24 bytes         | Self-describing; 3 per cache line           |
| Sign encoding          | XOR sign bit     | memcmp ordering; proven in KV stores        |
| Byte order             | Big-endian       | memcmp compares MSB first                   |
| Infinity               | Sentinels        | No extra bits; sorts correctly              |
| Empty intervals        | Not representable | Preserves total order; simplifies operators |
| Granularity scope      | Per-bound         | Supports mixed-precision intervals          |
| Certainty scope        | Per-bound         | Matches granularity symmetry                |
| Certainty in ordering  | Metadata only    | Preserves determinism and SIMD              |
| Calendar               | Proleptic Gregor.| Continuous; no Julian transition complexity  |
| Year numbering         | Astronomical     | No year-zero gap; clean arithmetic          |
| Leap seconds           | Excluded         | Monotonic timeline; POSIX-compatible        |
| Arrow type             | FixedSizeBinary(24) | memcmp in storage; no custom extension   |
| Type name              | Btic             | Distinct identity; not a generic description|
| Role                   | Valid-time only   | Transaction-time handled by MVCC            |
| Literal syntax         | ISO 8601 + extensions | Familiar; minimal new syntax to learn   |

---

## Appendix E: Implementation Phases

### Phase 1: Core Type (Minimum Viable)

- BTIC Rust crate: encode, decode, validate, display
- Uni-DB integration: `DataType::Btic`, `TemporalValue::Btic`, Arrow mapping, codec
- Literal parsing: single-expression and two-bound intervals
- Accessors: `btic_lo`, `btic_hi`, `btic_duration`
- Predicates: `btic_contains_point`, `btic_overlaps`
- Standard comparison operators (free from memcmp)

### Phase 2: Practical Query Power

- Predicates: `btic_before`, `btic_after`, `btic_meets`, `btic_disjoint`,
  `btic_adjacent`, `btic_contains`
- Set operations: `btic_intersection`, `btic_span`
- Remaining accessors: granularity, certainty, is_instant, is_unbounded
- Certainty annotations in literals (`~`, `?`, `??`)
- BCE date support

### Phase 3: Analytics and Reasoning

- Aggregates: `btic_span_agg`, `btic_count_at`, `btic_min`, `btic_max`
- Set operation: `btic_gap`
- Full Allen relation set: `btic_starts`, `btic_during`, `btic_finishes`, `btic_equals`
- Locy integration: temporal predicates in recursive rules
- DataFusion UDF registration for vectorized execution
