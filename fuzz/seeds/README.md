# Fuzz seed corpus

Regression seeds for inputs that previously crashed. `cargo fuzz run`
merges these into the working corpus:

```bash
cargo +nightly fuzz run btic_decode seeds/btic_decode
```

- `btic_decode/utf8-boundary-bce-suffix` — multi-byte UTF-8 straddling the
  `len - 3` byte index panicked `strip_bce_suffix` (fixed 2026-06-10).
