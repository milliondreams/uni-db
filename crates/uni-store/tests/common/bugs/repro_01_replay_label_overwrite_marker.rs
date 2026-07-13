// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

//! Repro for l0.rs:1561 (finding [1]).
//!
//! `L0Buffer::replay_mutations` handling of `Mutation::SetVertexLabels`
//! restores `vertex_labels` but never inserts the vid into
//! `vertex_label_overwrites` — unlike the live `set_vertex_labels` path.
//! That marker is the load-bearing signal the M8 flush pass filters on to
//! persist label-only mutations on prior-window vertices. Without it a
//! WAL-durable `SET n:Label` on a flushed vertex is silently lost at the
//! first post-recovery flush.
//!
//! Differential unit proof: the live path sets the marker, the replay path
//! does not, for identical inputs.

use uni_common::core::id::Vid;
use uni_store::runtime::l0::L0Buffer;
use uni_store::runtime::wal::Mutation;

#[test]
fn repro_replay_setvertexlabels_omits_overwrite_marker() {
    let vid = Vid::new(42);
    let labels = vec!["Person".to_string(), "VIP".to_string()];

    // Live mutation path (the reference): marks the vid in overwrites.
    let mut live = L0Buffer::new(0, None);
    live.set_vertex_labels(vid, &labels);
    assert!(
        live.vertex_label_overwrites.contains(&vid),
        "live set_vertex_labels must mark the vid in vertex_label_overwrites"
    );

    // WAL-replay path (recovery): restores labels but omits the marker.
    let mut replayed = L0Buffer::new(0, None);
    replayed
        .replay_mutations(vec![Mutation::SetVertexLabels {
            vid,
            labels: labels.clone(),
        }])
        .expect("replay_mutations should succeed");

    // Labels ARE restored ...
    assert_eq!(
        replayed.vertex_labels.get(&vid),
        Some(&labels),
        "replay restores the label set"
    );

    // ... and the M8-flush marker is now ALSO set (fixed at l0.rs:1561), so the
    // post-recovery flush's M8 pass sees this vid and persists the relabel
    // instead of silently dropping it. The replay path now matches the live
    // `set_vertex_labels` reference above.
    assert!(
        replayed.vertex_label_overwrites.contains(&vid),
        "replay_mutations must mark the vid in vertex_label_overwrites, like the live path"
    );
}
