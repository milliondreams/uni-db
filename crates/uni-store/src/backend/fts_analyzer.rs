// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Mapping from the backend-agnostic [`TokenizerConfig`] onto Lance's
//! `InvertedIndexParams`.
//!
//! `uni-common` deliberately stays Lance-free, so all translation from the
//! serializable analyzer description into Lance types lives here, gated behind
//! the `lance-backend` feature.

use anyhow::{Result, anyhow};
use lance_index::scalar::InvertedIndexParams;
use uni_common::core::schema::{AnalyzerConfig, BaseTokenizer, FtsLanguage, TokenizerConfig};

/// Serde-name of a [`FtsLanguage`] as understood by Lance's
/// `InvertedIndexParams::language`.
///
/// Lance parses the language from its `serde` representation, which is the
/// capitalized variant name (no `rename_all`), e.g. `"English"`.
fn lance_language_name(language: FtsLanguage) -> &'static str {
    match language {
        FtsLanguage::Arabic => "Arabic",
        FtsLanguage::Danish => "Danish",
        FtsLanguage::Dutch => "Dutch",
        FtsLanguage::English => "English",
        FtsLanguage::Finnish => "Finnish",
        FtsLanguage::French => "French",
        FtsLanguage::German => "German",
        FtsLanguage::Greek => "Greek",
        FtsLanguage::Hungarian => "Hungarian",
        FtsLanguage::Italian => "Italian",
        FtsLanguage::Norwegian => "Norwegian",
        FtsLanguage::Portuguese => "Portuguese",
        FtsLanguage::Romanian => "Romanian",
        FtsLanguage::Russian => "Russian",
        FtsLanguage::Spanish => "Spanish",
        FtsLanguage::Swedish => "Swedish",
        FtsLanguage::Tamil => "Tamil",
        FtsLanguage::Turkish => "Turkish",
        // `FtsLanguage` is `#[non_exhaustive]`; a future variant this build does
        // not know about falls back to English rather than failing.
        _ => "English",
    }
}

/// Whether Lance ships a built-in stop-word list for `language`.
///
/// Lance's `StopWordFilter::new` returns `None` (a hard build error) for the
/// languages below. Callers must supply `custom_stop_words` or disable
/// stop-word removal for these.
fn language_has_builtin_stop_words(language: FtsLanguage) -> bool {
    !matches!(
        language,
        // StopWordFilter::new(..) returns None for these five languages.
        FtsLanguage::Arabic
            | FtsLanguage::Greek
            | FtsLanguage::Romanian
            | FtsLanguage::Tamil
            | FtsLanguage::Turkish
    )
}

/// Map a [`TokenizerConfig`] onto Lance's `InvertedIndexParams`.
///
/// `with_positions` requests position postings (needed for phrase queries);
/// it is forced off for the N-gram base tokenizer, which Lance does not allow
/// to store positions.
///
/// # Errors
///
/// Returns an error when the configuration is invalid for Lance:
/// - N-gram `min == 0` or `min > max`.
/// - Stop-word removal requested for a language without a built-in list and
///   without an explicit `custom_stop_words` override.
pub fn to_inverted_params(
    cfg: &TokenizerConfig,
    with_positions: bool,
) -> Result<InvertedIndexParams> {
    match cfg {
        TokenizerConfig::Standard => Ok(InvertedIndexParams::default()
            .base_tokenizer("simple".to_string())
            .with_position(with_positions)),
        TokenizerConfig::Whitespace => Ok(InvertedIndexParams::default()
            .base_tokenizer("whitespace".to_string())
            .with_position(with_positions)),
        TokenizerConfig::Ngram { min, max } => {
            build_ngram(u32::from(*min), u32::from(*max))
        }
        TokenizerConfig::Custom { name } => Ok(InvertedIndexParams::default()
            .base_tokenizer(name.clone())
            .with_position(with_positions)),
        TokenizerConfig::Analyzer(a) => analyzer_to_params(a, with_positions),
        // `TokenizerConfig` is `#[non_exhaustive]`; treat any future variant as
        // the standard tokenizer rather than failing index creation.
        _ => Ok(InvertedIndexParams::default()
            .base_tokenizer("simple".to_string())
            .with_position(with_positions)),
    }
}

/// Build N-gram params, validating gram bounds and forcing positions off.
///
/// # Errors
///
/// Returns an error when `min == 0` or `min > max`.
fn build_ngram(min: u32, max: u32) -> Result<InvertedIndexParams> {
    if min == 0 {
        return Err(anyhow!(
            "invalid ngram tokenizer: min gram length must be >= 1 (got min={min})"
        ));
    }
    if min > max {
        return Err(anyhow!(
            "invalid ngram tokenizer: min ({min}) must not exceed max ({max})"
        ));
    }
    // Ngram is incompatible with position postings in Lance.
    Ok(InvertedIndexParams::default()
        .base_tokenizer("ngram".to_string())
        .ngram_min_length(min)
        .ngram_max_length(max)
        .with_position(false))
}

/// Translate a fully-specified [`AnalyzerConfig`] into `InvertedIndexParams`.
///
/// # Errors
///
/// See [`to_inverted_params`].
fn analyzer_to_params(a: &AnalyzerConfig, with_positions: bool) -> Result<InvertedIndexParams> {
    // Resolve the base tokenizer name and whether it forces positions off.
    let (base_name, ngram): (String, Option<(u32, u32)>) = match &a.base {
        BaseTokenizer::Simple => ("simple".to_string(), None),
        BaseTokenizer::Whitespace => ("whitespace".to_string(), None),
        BaseTokenizer::Raw => ("raw".to_string(), None),
        BaseTokenizer::Ngram { min, max } => ("ngram".to_string(), Some((*min, *max))),
        BaseTokenizer::Custom(name) => (name.clone(), None),
        // `BaseTokenizer` is `#[non_exhaustive]`; treat an unknown future
        // variant as the simple tokenizer.
        _ => ("simple".to_string(), None),
    };

    let mut params = InvertedIndexParams::default().base_tokenizer(base_name);

    // Language (used for stemming and built-in stop words).
    params = params
        .language(lance_language_name(a.language))
        .map_err(|e| anyhow!("invalid FTS language {:?}: {e}", a.language))?;

    // N-gram bounds + validation. Positions are incompatible with ngram.
    let is_ngram = if let Some((min, max)) = ngram {
        if min == 0 {
            return Err(anyhow!(
                "invalid ngram tokenizer: min gram length must be >= 1 (got min={min})"
            ));
        }
        if min > max {
            return Err(anyhow!(
                "invalid ngram tokenizer: min ({min}) must not exceed max ({max})"
            ));
        }
        params = params.ngram_min_length(min).ngram_max_length(max);
        true
    } else {
        false
    };

    // Stop-word handling: reject an unsupported language unless the caller
    // supplied an explicit list.
    let remove_stop_words = a.remove_stop_words;
    if remove_stop_words
        && a.custom_stop_words.is_none()
        && !language_has_builtin_stop_words(a.language)
    {
        return Err(anyhow!(
            "stop-word removal is not supported for language {:?}: Lance ships no built-in \
             stop-word list. Provide `custom_stop_words` or disable `remove_stop_words`.",
            a.language
        ));
    }

    params = params
        .lower_case(a.lower_case)
        .stem(a.stem)
        .remove_stop_words(remove_stop_words)
        .custom_stop_words(a.custom_stop_words.clone())
        .ascii_folding(a.ascii_folding)
        .max_token_length(a.max_token_length.map(|n| n as usize));

    // Positions: forced off for ngram, otherwise honor the request.
    params = params.with_position(if is_ngram { false } else { with_positions });

    Ok(params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_maps_to_simple() {
        let p = to_inverted_params(&TokenizerConfig::Standard, true).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["base_tokenizer"], "simple");
        assert_eq!(json["with_position"], true);
    }

    #[test]
    fn whitespace_maps_to_whitespace() {
        let p = to_inverted_params(&TokenizerConfig::Whitespace, false).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["base_tokenizer"], "whitespace");
        assert_eq!(json["with_position"], false);
    }

    #[test]
    fn custom_passthrough_preserves_name() {
        let p = to_inverted_params(
            &TokenizerConfig::Custom {
                name: "jieba/default".to_string(),
            },
            true,
        )
        .unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["base_tokenizer"], "jieba/default");
    }

    #[test]
    fn ngram_forces_position_off() {
        let p = to_inverted_params(&TokenizerConfig::Ngram { min: 2, max: 4 }, true).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["base_tokenizer"], "ngram");
        assert_eq!(json["min_ngram_length"], 2);
        assert_eq!(json["max_ngram_length"], 4);
        // with_position must be false even though we asked for true.
        assert_eq!(json["with_position"], false);
    }

    #[test]
    fn ngram_min_zero_rejected() {
        let err = to_inverted_params(&TokenizerConfig::Ngram { min: 0, max: 3 }, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("min gram length must be >= 1"), "{err}");
    }

    #[test]
    fn ngram_min_greater_than_max_rejected() {
        let err = to_inverted_params(&TokenizerConfig::Ngram { min: 5, max: 3 }, false)
            .unwrap_err()
            .to_string();
        assert!(err.contains("must not exceed max"), "{err}");
    }

    #[test]
    fn analyzer_full_config_maps_fields() {
        let cfg = TokenizerConfig::Analyzer(AnalyzerConfig {
            base: BaseTokenizer::Simple,
            language: FtsLanguage::French,
            lower_case: false,
            stem: true,
            remove_stop_words: true,
            custom_stop_words: None,
            ascii_folding: false,
            max_token_length: Some(64),
        });
        let p = to_inverted_params(&cfg, true).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["base_tokenizer"], "simple");
        assert_eq!(json["language"], "French");
        assert_eq!(json["lower_case"], false);
        assert_eq!(json["stem"], true);
        assert_eq!(json["remove_stop_words"], true);
        assert_eq!(json["ascii_folding"], false);
        assert_eq!(json["max_token_length"], 64);
        assert_eq!(json["with_position"], true);
    }

    #[test]
    fn analyzer_unsupported_stopword_language_rejected() {
        let cfg = TokenizerConfig::Analyzer(AnalyzerConfig {
            language: FtsLanguage::Turkish,
            remove_stop_words: true,
            custom_stop_words: None,
            ..AnalyzerConfig::default()
        });
        let err = to_inverted_params(&cfg, false).unwrap_err().to_string();
        assert!(err.contains("not supported for language"), "{err}");
    }

    #[test]
    fn analyzer_unsupported_stopword_language_ok_with_custom_list() {
        let cfg = TokenizerConfig::Analyzer(AnalyzerConfig {
            language: FtsLanguage::Turkish,
            remove_stop_words: true,
            custom_stop_words: Some(vec!["ve".to_string(), "bir".to_string()]),
            ..AnalyzerConfig::default()
        });
        let p = to_inverted_params(&cfg, false).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["language"], "Turkish");
        assert_eq!(json["remove_stop_words"], true);
    }

    #[test]
    fn analyzer_unsupported_stopword_language_ok_when_disabled() {
        let cfg = TokenizerConfig::Analyzer(AnalyzerConfig {
            language: FtsLanguage::Arabic,
            remove_stop_words: false,
            ..AnalyzerConfig::default()
        });
        let p = to_inverted_params(&cfg, false).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["remove_stop_words"], false);
    }

    #[test]
    fn analyzer_ngram_base_forces_position_off_and_validates() {
        let cfg = TokenizerConfig::Analyzer(AnalyzerConfig {
            base: BaseTokenizer::Ngram { min: 3, max: 5 },
            ..AnalyzerConfig::default()
        });
        let p = to_inverted_params(&cfg, true).unwrap();
        let json = serde_json::to_value(&p).unwrap();
        assert_eq!(json["base_tokenizer"], "ngram");
        assert_eq!(json["with_position"], false);

        let bad = TokenizerConfig::Analyzer(AnalyzerConfig {
            base: BaseTokenizer::Ngram { min: 0, max: 5 },
            ..AnalyzerConfig::default()
        });
        assert!(to_inverted_params(&bad, false).is_err());
    }
}
