# Fuzz seed corpus

Regression seeds for inputs that previously crashed. `cargo fuzz run`
merges these into the working corpus:

```bash
cargo +nightly fuzz run btic_decode corpus/btic_decode seeds/btic_decode
```

**Name `corpus/` first — the order matters.** libFuzzer writes every
newly-discovered input into the *first* corpus directory on the command line and
treats the remaining ones as read-only. The shorter form
`cargo fuzz run btic_decode seeds/btic_decode` therefore makes *this*
directory the output corpus, and one 30-second run buries its handful of
curated regression inputs under several hundred generated files. (Measured:
1 file became 384.) These are git-tracked, so that lands in `git status` as
hundreds of untracked files rather than anything louder.

Seeds also cover **timeouts**, not just crashes. Exponential backtracking in the
grammar is the failure this corpus has caught twice, and a 30-second blind run
rediscovers it only by luck — the nightly found the second instance three times
in eight nights.

- `btic_decode/utf8-boundary-bce-suffix` — multi-byte UTF-8 straddling the
  `len - 3` byte index panicked `strip_bce_suffix` (fixed 2026-06-10).
- `locy_parse/paren-brace-mapkey-timeout` — the 942-byte nightly artifact that
  exceeded the 10 s per-input budget: `pattern_expression` and
  `"(" ~ expression ~ ")"` both descended into the same `(`-led interior, so the
  work doubled per nesting level (fixed 2026-09-18).
- `locy_parse/paren-brace-mapkey-minimal`,
  `cypher_parse/paren-brace-mapkey-minimal` — the same shape reduced to its
  repeating core, `G=({G:`. Cheap to replay and it reproduces in both
  front-ends, which share `primary_expression`.
- `cypher_parse/nested-valid-map-pattern` — the same defect reached through a
  **valid** query, `RETURN (a {k: (a {k: ... }) })`. Worth keeping separate: a
  mitigation aimed at malformed or unclosed input would leave this one hanging.
