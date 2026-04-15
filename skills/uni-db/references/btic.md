# BTIC (Binary Temporal Interval Codec) Reference

## 1. Overview

BTIC encodes temporal intervals as 24-byte binary values with half-open semantics `[lo, hi)`. Each bound carries independent **granularity** (millisecond to millennium) and **certainty** (definite to unknown) metadata. BTIC values support comparison operators, Allen interval algebra predicates, set operations, and accessor functions.

**Key properties:**
- Half-open intervals: lo is inclusive, hi is exclusive
- Per-bound granularity and certainty metadata
- `memcmp`-compatible binary encoding for efficient indexing
- BCE date support, timezone normalization to UTC
- 30+ dedicated Cypher functions

---

## 2. Literal Syntax

Construct BTIC values in Cypher with `btic('literal')`:

### Single Expressions (one granularity)

```cypher
btic('1985')                     -- year
btic('1985-03')                  -- month
btic('1985-03-15')               -- day
btic('1985-03-15T14')            -- hour
btic('1985-03-15T14:30')         -- minute
btic('1985-03-15T14:30:00')      -- second
btic('1985-03-15T14:30:00.123')  -- millisecond
```

### Range Intervals (solidus `/`)

```cypher
btic('1985-03/2024-06')    -- bounded range (mixed granularities OK)
btic('2020-03/')            -- right-unbounded (ongoing)
btic('/2024-06')            -- left-unbounded (from beginning of time)
btic('/')                   -- fully unbounded (all time)
```

### Certainty Prefixes

```cypher
btic('~1985')               -- approximate: true value ±1 unit of granularity
btic('?1985-03')             -- uncertain: true value may differ significantly
btic('??1985')               -- unknown: stored value is best guess
btic('~1985/?2024-06')       -- independent certainty per bound
```

### BCE Dates

```cypher
btic('500 BCE')              -- 500 BCE (astronomical year -499)
btic('~500BCE/44BCE')        -- approximate range in antiquity
```

### Timezone Handling

Timezones are normalized to UTC milliseconds:

```cypher
btic('1985-03-15T14:30:00Z')       -- UTC
btic('1985-03-15T14:30:00+05:30')  -- offset, stored as UTC
```

---

## 3. Granularity Levels

Each bound has an independent granularity. Single expressions set both bounds to the same granularity; ranges may differ.

| Level | Name | Example |
|-------|------|---------|
| 0 | millisecond | `btic('1985-03-15T14:30:00.123')` |
| 1 | second | `btic('1985-03-15T14:30:00')` |
| 2 | minute | `btic('1985-03-15T14:30')` |
| 3 | hour | `btic('1985-03-15T14')` |
| 4 | day | `btic('1985-03-15')` |
| 5 | month | `btic('1985-03')` |
| 6 | quarter | (derived, not directly parseable) |
| 7 | year | `btic('1985')` |
| 8 | decade | (derived) |
| 9 | century | (derived) |
| 10 | millennium | (derived) |

---

## 4. Certainty Levels

Each bound has an independent certainty level:

| Level | Name | Prefix | Meaning |
|-------|------|--------|---------|
| 0 | definite | (none) | Known precisely to stated granularity |
| 1 | approximate | `~` | May differ by ±1 unit of granularity |
| 2 | uncertain | `?` | May differ significantly |
| 3 | unknown | `??` | Best guess only |

---

## 5. Functions Reference

### Constructor

| Function | Signature | Description |
|----------|-----------|-------------|
| `btic(s)` | `String -> Btic \| Null` | Parse ISO 8601-inspired literal. Returns Null for Null input. |

### Accessor Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `btic_lo(b)` | `Btic -> DateTime \| Null` | Lower bound as UTC datetime. Null if unbounded. |
| `btic_hi(b)` | `Btic -> DateTime \| Null` | Upper bound as UTC datetime. Null if unbounded. |
| `btic_duration(b)` | `Btic -> Int \| Null` | Duration in milliseconds. Null if unbounded. |
| `btic_granularity(b)` | `Btic -> String` | Coarsest granularity of both bounds. |
| `btic_lo_granularity(b)` | `Btic -> String` | Lower bound granularity name. |
| `btic_hi_granularity(b)` | `Btic -> String` | Upper bound granularity name. |
| `btic_certainty(b)` | `Btic -> String` | Least certain level of both bounds. |
| `btic_lo_certainty(b)` | `Btic -> String` | Lower bound certainty name. |
| `btic_hi_certainty(b)` | `Btic -> String` | Upper bound certainty name. |

### Property Functions

| Function | Signature | Description |
|----------|-----------|-------------|
| `btic_is_instant(b)` | `Btic -> Bool` | True if duration is exactly 1 millisecond. |
| `btic_is_unbounded(b)` | `Btic -> Bool` | True if either bound is infinite. |
| `btic_is_finite(b)` | `Btic -> Bool` | True if both bounds are finite. |

### Allen Interval Algebra Predicates

All predicates: `(Btic, Btic) -> Bool | Null`. Returns Null if either argument is Null.

| Function | True when | Diagram |
|----------|-----------|---------|
| `btic_overlaps(a, b)` | Intervals share at least one tick | `a: [----)  b: [----)`  overlap region exists |
| `btic_contains(a, b)` | a fully contains b | `a: [----------)  b:   [----)` |
| `btic_before(a, b)` | a ends before or when b starts | `a: [----)  b:      [----)` |
| `btic_after(a, b)` | a starts after or when b ends | `b: [----)  a:      [----)` |
| `btic_meets(a, b)` | a ends exactly where b starts | `a: [----)b: [----)` (a.hi == b.lo) |
| `btic_adjacent(a, b)` | Meets in either direction | `btic_meets(a,b) OR btic_meets(b,a)` |
| `btic_disjoint(a, b)` | No shared ticks | `a: [----)     b: [----)` |
| `btic_equals(a, b)` | Same lo and hi (ignores metadata) | `a.lo == b.lo AND a.hi == b.hi` |
| `btic_starts(a, b)` | Same start, a ends earlier | `a: [----)  b: [----------)` |
| `btic_during(a, b)` | a strictly inside b | `b: [----------)  a:   [----)` |
| `btic_finishes(a, b)` | Same end, a starts later | `b: [----------)  a:      [----)` |

### Point Containment

| Function | Signature | Description |
|----------|-----------|-------------|
| `btic_contains_point(b, p)` | `(Btic, Temporal\|Int) -> Bool \| Null` | Half-open: `lo <= p < hi`. Accepts datetime or epoch millis. |

### Set Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `btic_intersection(a, b)` | `(Btic, Btic) -> Btic \| Null` | `[max(a.lo, b.lo), min(a.hi, b.hi))`. Null if disjoint. |
| `btic_span(a, b)` | `(Btic, Btic) -> Btic` | Bounding interval: `[min(a.lo, b.lo), max(a.hi, b.hi))`. Always valid. |
| `btic_gap(a, b)` | `(Btic, Btic) -> Btic \| Null` | Gap between disjoint intervals. Null if overlapping or adjacent. |

### Comparison Operators

BTIC values support standard comparison operators using packed byte ordering:

```cypher
WHERE n.period < btic('2000')     -- periods before 2000
WHERE n.period > btic('1985')     -- periods after 1985
WHERE n.period = btic('1990')     -- exact match
WHERE n.period <> btic('1990')    -- not equal
WHERE n.period <= btic('2000')    -- less than or equal
WHERE n.period >= btic('1985')    -- greater than or equal
```

---

## 6. Python API

### Btic Class

```python
from uni_db import Btic, Value, DataType

# Construction
b = Btic("1985")                    # from literal
b = Btic("1985-03/2024-06")         # range
b = Btic("~1985")                   # approximate
b = Btic.from_raw(lo_ms, hi_ms, meta)  # from raw fields

# Properties
b.lo                # int: lower bound in epoch milliseconds
b.hi                # int: upper bound in epoch milliseconds
b.meta              # int: packed metadata word
b.lo_granularity    # str: "year", "month", "day", etc.
b.hi_granularity    # str
b.lo_certainty      # str: "definite", "approximate", "uncertain", "unknown"
b.hi_certainty      # str
b.duration_ms       # int | None: duration in ms, None if unbounded
b.is_instant        # bool
b.is_unbounded      # bool
b.is_finite         # bool

# Allen predicates (methods)
a.overlaps(b)       # bool
a.contains(b)       # bool
a.before(b)         # bool
a.after(b)          # bool
a.meets(b)          # bool
a.disjoint(b)       # bool

# Set operations (methods)
a.intersection(b)   # Btic | None
a.span(b)           # Btic
a.gap(b)            # Btic | None
```

### Value Integration

```python
# Create a BTIC Value
v = Value.btic("1985")
v.is_btic()         # True
v.type_name          # "btic"
v.to_python()        # returns Btic object

# DataType for schema
dt = DataType.BTIC()
```

### Schema and Queries

```python
# Define BTIC property
db.schema() \
    .label("Event") \
        .property("when", DataType.BTIC()) \
    .apply()

# Create with btic() function
with session.tx() as tx:
    tx.execute("CREATE (:Event {name: 'WW2', when: btic('1939/1945')})")
    tx.commit()

# Pass as parameter
btic_val = Btic("1985")
with session.tx() as tx:
    tx.execute(
        "CREATE (:Event {name: 'Birth', when: $when})",
        params={"when": btic_val}
    )
    tx.commit()

# Query with predicates
result = session.query("""
    MATCH (e:Event)
    WHERE btic_overlaps(e.when, btic('1940/1945'))
    RETURN e.name, btic_duration(e.when) AS duration_ms
""")

# Comparison operators
result = session.query("""
    MATCH (e:Event)
    WHERE e.when < btic('2000')
    RETURN e.name ORDER BY e.when
""")
```

---

## 7. Rust API

```rust
use uni_db::{Uni, Value};
use uni_common::TemporalValue;

// Create BTIC value from raw fields
let ww2 = Value::Temporal(TemporalValue::Btic {
    lo: -978_307_200_000,  // 1939-01-01T00:00:00Z
    hi: -757_382_400_000,  // 1945-12-31T00:00:00Z (approx)
    meta: 0x7700_0000_0000_0000, // year/year, definite/definite
});

// Pass as parameter
tx.execute_with("CREATE (:Event {name: 'WW2', when: $when})")
    .param("when", ww2)
    .run()
    .await?;

// Query with BTIC functions
let result = session.query(
    "MATCH (e:Event) WHERE btic_contains(e.when, btic('1943')) RETURN e.name"
).await?;
```

---

## 8. Common Patterns

### Temporal Filtering

```cypher
-- Events during a specific period
MATCH (e:Event)
WHERE btic_during(e.when, btic('1980/1990'))
RETURN e.name

-- Events overlapping a query window
MATCH (e:Event)
WHERE btic_overlaps(e.when, btic('2024-Q1'))
RETURN e.name, btic_lo(e.when) AS start

-- Events before a cutoff
MATCH (e:Event)
WHERE btic_before(e.when, btic('2000'))
RETURN e.name ORDER BY e.when
```

### Timeline Analysis

```cypher
-- Duration of each event
MATCH (e:Event)
RETURN e.name, btic_duration(e.when) AS ms,
       btic_granularity(e.when) AS precision

-- Find the span covering all events
MATCH (e1:Event), (e2:Event)
WHERE e1 <> e2
RETURN btic_span(e1.when, e2.when) AS combined
```

### Certainty-Aware Queries

```cypher
-- Find approximate or uncertain dates
MATCH (e:Event)
WHERE btic_certainty(e.when) <> 'definite'
RETURN e.name, btic_certainty(e.when) AS certainty

-- Per-bound certainty inspection
MATCH (e:Event)
RETURN e.name,
       btic_lo_certainty(e.when) AS start_certainty,
       btic_hi_certainty(e.when) AS end_certainty
```

### Allen Interval Relations

```cypher
-- Find events that start together
MATCH (a:Event), (b:Event)
WHERE a <> b AND btic_starts(a.when, b.when)
RETURN a.name AS shorter, b.name AS longer

-- Find sequential (meeting) events
MATCH (a:Event), (b:Event)
WHERE btic_meets(a.when, b.when)
RETURN a.name AS first, b.name AS next

-- Find gaps between events
MATCH (a:Event), (b:Event)
WHERE btic_disjoint(a.when, b.when)
RETURN a.name, b.name, btic_gap(a.when, b.when) AS gap_interval
```

---

## 9. Internal Encoding

24 bytes, big-endian, `memcmp`-comparable:

```
Bytes  0-7:  lo XOR 0x8000000000000000 (sign-flipped for unsigned ordering)
Bytes  8-15: hi XOR 0x8000000000000000
Bytes 16-23: meta (packed metadata)
```

**Meta word layout:**

```
Bits [63:60]  lo_granularity (4 bits, 0x0-0xA)
Bits [59:56]  hi_granularity (4 bits, 0x0-0xA)
Bits [55:54]  lo_certainty   (2 bits, 0-3)
Bits [53:52]  hi_certainty   (2 bits, 0-3)
Bits [51:48]  version        (4 bits, must be 0)
Bits [47:0]   reserved       (must be 0)
```

**Special bounds:** `i64::MIN` = negative infinity (unbounded left), `i64::MAX` = positive infinity (unbounded right). Sentinel bounds must have zeroed granularity and certainty.

---

## 10. Gotchas

1. **Intervals are half-open `[lo, hi)`** -- The upper bound is exclusive. `btic('1985')` covers Jan 1 1985 through Dec 31 1985 (hi is Jan 1 1986).

2. **Solidus end-bound is inclusive at its granularity** -- `btic('1985-06/1986')` extends through the end of 1986 (hi is Jan 1 1987), not to the start of 1986. Use `btic('1985-06/1985-12')` if you mean "through December 1985".

3. **Comparison uses packed byte ordering** -- `<`, `>`, `<=`, `>=` compare the binary encoding (lo first, then hi, then meta). This is storage-efficient but reflects lexicographic interval ordering, not pure temporal ordering.

4. **Granularity affects bound expansion** -- `btic('1985')` expands to `[1985-01-01, 1986-01-01)`. `btic('1985-03')` expands to `[1985-03-01, 1985-04-01)`. The granularity determines how much time one "unit" covers.

5. **Set operation metadata inheritance** -- Intersection, span, and gap results inherit granularity and certainty from the contributing bound. When bounds are equal, the finer granularity and least certain level wins.

6. **Null propagation** -- All BTIC functions return Null if any argument is Null (except `btic()` constructor which explicitly handles Null input).
