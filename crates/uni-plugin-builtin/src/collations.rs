//! Built-in collation registrations.
//!
//! M5c scaffolding: ships ASCII case-insensitive and Unicode-codepoint
//! collations. ICU locale-aware collations land in M5c cutover commits
//! gated by an `icu` feature.

use std::cmp::Ordering;
use std::sync::Arc;

use uni_plugin::traits::collation::CollationProvider;
use uni_plugin::{PluginError, PluginRegistrar};

/// Register the built-in collation providers.
///
/// # Errors
///
/// Returns [`PluginError`] on duplicate registration.
pub fn register_into(r: &mut PluginRegistrar<'_>) -> Result<(), PluginError> {
    r.collation(Arc::new(AsciiCaseSensitive))?;
    r.collation(Arc::new(AsciiCaseInsensitive))?;
    r.collation(Arc::new(UnicodeCodepoint))?;
    r.collation(Arc::new(UnicodeCaseInsensitive))?;
    r.collation(Arc::new(NaturalNumeric))?;
    Ok(())
}

/// ASCII case-sensitive collation (default for `ORDER BY string-col`).
#[derive(Debug)]
pub struct AsciiCaseSensitive;

impl CollationProvider for AsciiCaseSensitive {
    fn name(&self) -> &str {
        "ascii.case_sensitive"
    }
    fn compare(&self, a: &str, b: &str) -> Ordering {
        a.cmp(b)
    }
    fn normalize(&self, s: &str) -> String {
        s.to_owned()
    }
}

/// ASCII case-insensitive collation.
#[derive(Debug)]
pub struct AsciiCaseInsensitive;

impl CollationProvider for AsciiCaseInsensitive {
    fn name(&self) -> &str {
        "ascii.case_insensitive"
    }
    fn compare(&self, a: &str, b: &str) -> Ordering {
        a.bytes()
            .map(|b| b.to_ascii_lowercase())
            .cmp(b.bytes().map(|b| b.to_ascii_lowercase()))
    }
    fn normalize(&self, s: &str) -> String {
        s.to_ascii_lowercase()
    }
}

/// Unicode codepoint collation (lexicographic by `char` value).
#[derive(Debug)]
pub struct UnicodeCodepoint;

impl CollationProvider for UnicodeCodepoint {
    fn name(&self) -> &str {
        "unicode.codepoint"
    }
    fn compare(&self, a: &str, b: &str) -> Ordering {
        a.chars().cmp(b.chars())
    }
}

/// Unicode case-insensitive collation.
///
/// Compares by lowercased Unicode codepoints. Note: full case-folding
/// per Unicode Default Caseless Matching (which handles e.g. German
/// `ß` ↔ `SS`) requires the `unicode-case` data; this implementation
/// uses Rust's `to_lowercase()` which covers most practical cases but
/// not every Unicode-defined caseless pair.
#[derive(Debug)]
pub struct UnicodeCaseInsensitive;

impl CollationProvider for UnicodeCaseInsensitive {
    fn name(&self) -> &str {
        "unicode.case_insensitive"
    }
    fn compare(&self, a: &str, b: &str) -> Ordering {
        a.to_lowercase().chars().cmp(b.to_lowercase().chars())
    }
    fn normalize(&self, s: &str) -> String {
        s.to_lowercase()
    }
}

/// Natural-numeric collation — sorts embedded integer sequences
/// numerically instead of lexicographically.
///
/// Behavior: split each string into alternating non-digit / digit
/// chunks; compare chunk-wise, with digit chunks compared numerically
/// (longer-digit-runs are bigger when the prefix is equal). Useful for
/// `file2.txt < file10.txt` ordering instead of the default
/// lexicographic `file10.txt < file2.txt`.
///
/// Negative numbers, decimals, and exponents are not parsed — leading
/// `-` is treated as a non-digit. Users who need full numeric parsing
/// should ship their own collation plugin.
#[derive(Debug)]
pub struct NaturalNumeric;

impl CollationProvider for NaturalNumeric {
    fn name(&self) -> &str {
        "natural.numeric"
    }
    fn compare(&self, a: &str, b: &str) -> Ordering {
        natural_compare(a, b)
    }
}

/// Compare two strings using natural-numeric collation rules.
fn natural_compare(a: &str, b: &str) -> Ordering {
    let mut ai = a.char_indices().peekable();
    let mut bi = b.char_indices().peekable();

    loop {
        match (ai.peek().copied(), bi.peek().copied()) {
            (None, None) => return Ordering::Equal,
            (None, _) => return Ordering::Less,
            (_, None) => return Ordering::Greater,
            (Some((ai_idx, ac)), Some((bi_idx, bc))) => {
                let a_is_digit = ac.is_ascii_digit();
                let b_is_digit = bc.is_ascii_digit();

                if a_is_digit && b_is_digit {
                    // Both digits → consume a run and compare numerically.
                    let a_end = consume_digits(a, ai_idx);
                    let b_end = consume_digits(b, bi_idx);
                    let a_num = strip_leading_zeros(&a[ai_idx..a_end]);
                    let b_num = strip_leading_zeros(&b[bi_idx..b_end]);
                    let ord = match a_num.len().cmp(&b_num.len()) {
                        Ordering::Equal => a_num.cmp(b_num),
                        other => other,
                    };
                    if ord != Ordering::Equal {
                        return ord;
                    }
                    // Advance past the digit runs.
                    while ai.peek().map(|(i, _)| *i < a_end).unwrap_or(false) {
                        ai.next();
                    }
                    while bi.peek().map(|(i, _)| *i < b_end).unwrap_or(false) {
                        bi.next();
                    }
                } else {
                    // Compare as non-digits.
                    let ord = ac.cmp(&bc);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                    ai.next();
                    bi.next();
                }
            }
        }
    }
}

fn consume_digits(s: &str, from: usize) -> usize {
    let mut end = from;
    for (i, c) in s[from..].char_indices() {
        if c.is_ascii_digit() {
            end = from + i + c.len_utf8();
        } else {
            return end;
        }
    }
    end
}

fn strip_leading_zeros(s: &str) -> &str {
    let trimmed = s.trim_start_matches('0');
    if trimmed.is_empty() {
        // All-zeros — keep one zero so "0" compares equal to "00".
        &s[s.len().saturating_sub(1)..]
    } else {
        trimmed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ascii_case_sensitive_distinguishes_case() {
        let c = AsciiCaseSensitive;
        assert_eq!(c.compare("A", "a"), Ordering::Less);
        assert_eq!(c.compare("apple", "apple"), Ordering::Equal);
    }

    #[test]
    fn ascii_case_insensitive_collapses_case() {
        let c = AsciiCaseInsensitive;
        assert_eq!(c.compare("A", "a"), Ordering::Equal);
        assert_eq!(c.compare("Apple", "BANANA"), Ordering::Less);
    }

    #[test]
    fn case_insensitive_normalize_lowercases() {
        assert_eq!(AsciiCaseInsensitive.normalize("HeLLo"), "hello");
    }

    #[test]
    fn unicode_codepoint_handles_non_ascii() {
        let c = UnicodeCodepoint;
        assert_eq!(c.compare("á", "b"), Ordering::Greater);
    }

    #[test]
    fn unicode_case_insensitive_collapses_case() {
        let c = UnicodeCaseInsensitive;
        assert_eq!(c.compare("CAFÉ", "café"), Ordering::Equal);
        assert_eq!(c.normalize("CAFÉ"), "café");
    }

    #[test]
    fn natural_numeric_orders_file_numbers() {
        let c = NaturalNumeric;
        // file2.txt should be less than file10.txt under natural sort.
        assert_eq!(c.compare("file2.txt", "file10.txt"), Ordering::Less);
        assert_eq!(c.compare("file10.txt", "file2.txt"), Ordering::Greater);
    }

    #[test]
    fn natural_numeric_handles_leading_zeros() {
        let c = NaturalNumeric;
        // "007" and "7" compare equal numerically.
        assert_eq!(c.compare("file007.txt", "file7.txt"), Ordering::Equal);
    }

    #[test]
    fn natural_numeric_handles_no_digits() {
        let c = NaturalNumeric;
        assert_eq!(c.compare("apple", "banana"), Ordering::Less);
        assert_eq!(c.compare("xyz", "xyz"), Ordering::Equal);
    }

    #[test]
    fn natural_numeric_handles_multiple_digit_runs() {
        let c = NaturalNumeric;
        // ch1-s2 < ch1-s10 < ch2-s1.
        assert_eq!(c.compare("ch1-s2", "ch1-s10"), Ordering::Less);
        assert_eq!(c.compare("ch1-s10", "ch2-s1"), Ordering::Less);
    }
}
