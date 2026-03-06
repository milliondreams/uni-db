// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team
//
// Test for Issue #4: Constraint Check O(V) → O(1)
//
// Verifies that unique constraint checks use the O(1) constraint_index
// instead of O(V) full scan through vertex_properties.

use anyhow::Result;
use uni_common::Value;
use uni_common::core::id::Vid;
use uni_store::runtime::l0::{L0Buffer, serialize_constraint_key};

#[test]
fn test_serialize_constraint_key() {
    // Test that serialization is deterministic and order-independent
    let key1 = serialize_constraint_key(
        "Person",
        &[
            (
                "email".to_string(),
                Value::String("alice@example.com".to_string()),
            ),
            ("username".to_string(), Value::String("alice".to_string())),
        ],
    );

    let key2 = serialize_constraint_key(
        "Person",
        &[
            ("username".to_string(), Value::String("alice".to_string())),
            (
                "email".to_string(),
                Value::String("alice@example.com".to_string()),
            ),
        ],
    );

    // Keys should be identical (sorted internally)
    assert_eq!(key1, key2);

    // Different values should produce different keys
    let key3 = serialize_constraint_key(
        "Person",
        &[
            (
                "email".to_string(),
                Value::String("bob@example.com".to_string()),
            ),
            ("username".to_string(), Value::String("alice".to_string())),
        ],
    );
    assert_ne!(key1, key3);

    // Different labels should produce different keys
    let key4 = serialize_constraint_key(
        "User",
        &[
            (
                "email".to_string(),
                Value::String("alice@example.com".to_string()),
            ),
            ("username".to_string(), Value::String("alice".to_string())),
        ],
    );
    assert_ne!(key1, key4);
}

#[test]
fn test_l0_constraint_index_operations() -> Result<()> {
    let mut l0 = L0Buffer::new(0, None);

    let vid1 = Vid::new(1);
    let vid2 = Vid::new(2);

    let key1 = serialize_constraint_key(
        "Person",
        &[(
            "email".to_string(),
            Value::String("alice@example.com".to_string()),
        )],
    );

    let key2 = serialize_constraint_key(
        "Person",
        &[(
            "email".to_string(),
            Value::String("bob@example.com".to_string()),
        )],
    );

    // Insert constraint keys
    l0.insert_constraint_key(key1.clone(), vid1);
    l0.insert_constraint_key(key2.clone(), vid2);

    // has_constraint_key should find existing keys owned by other vertices
    assert!(l0.has_constraint_key(&key1, vid2)); // key1 is owned by vid1, so vid2 sees conflict
    assert!(l0.has_constraint_key(&key2, vid1)); // key2 is owned by vid2, so vid1 sees conflict

    // has_constraint_key should return false when checking own key
    assert!(!l0.has_constraint_key(&key1, vid1)); // vid1 checking its own key
    assert!(!l0.has_constraint_key(&key2, vid2)); // vid2 checking its own key

    // Non-existent key should return false
    let key3 = serialize_constraint_key(
        "Person",
        &[(
            "email".to_string(),
            Value::String("charlie@example.com".to_string()),
        )],
    );
    assert!(!l0.has_constraint_key(&key3, vid1));

    // Test deletion: constraint_index entries should be removed when vertex is deleted
    l0.insert_vertex_with_labels(
        vid1,
        [(
            "email".to_string(),
            Value::String("alice@example.com".to_string()),
        )]
        .into_iter()
        .collect(),
        &["Person".to_string()],
    );
    l0.delete_vertex(vid1)?;

    // After deletion, key1 should no longer conflict
    assert!(!l0.has_constraint_key(&key1, vid2));

    Ok(())
}

#[test]
fn test_constraint_index_populated_on_merge() -> Result<()> {
    let mut l0_main = L0Buffer::new(0, None);
    let mut l0_tx = L0Buffer::new(0, None);

    let vid1 = Vid::new(1);
    let vid2 = Vid::new(2);

    // Populate transaction L0 with constraint keys
    let key1 = serialize_constraint_key(
        "Person",
        &[(
            "email".to_string(),
            Value::String("alice@example.com".to_string()),
        )],
    );
    l0_tx.insert_constraint_key(key1.clone(), vid1);
    l0_tx.insert_vertex_with_labels(
        vid1,
        [(
            "email".to_string(),
            Value::String("alice@example.com".to_string()),
        )]
        .into_iter()
        .collect(),
        &["Person".to_string()],
    );

    // Merge transaction into main
    l0_main.merge(&l0_tx)?;

    // Verify constraint index was merged
    assert_eq!(l0_main.constraint_index.get(&key1), Some(&vid1));
    assert!(l0_main.has_constraint_key(&key1, vid2));

    Ok(())
}

#[test]
fn test_constraint_index_with_multiple_vertices() -> Result<()> {
    let mut l0 = L0Buffer::new(0, None);

    // Insert 100 vertices, each with a unique email
    for i in 0..100 {
        let vid = Vid::new(i);
        let email = format!("user{}@example.com", i);
        let key = serialize_constraint_key(
            "Person",
            &[("email".to_string(), Value::String(email.clone()))],
        );

        l0.insert_constraint_key(key.clone(), vid);
        l0.insert_vertex_with_labels(
            vid,
            [("email".to_string(), Value::String(email))]
                .into_iter()
                .collect(),
            &["Person".to_string()],
        );
    }

    // Verify all 100 constraint keys are in the index
    assert_eq!(l0.constraint_index.len(), 100);

    // Verify each key can be looked up
    for i in 0..100 {
        let vid = Vid::new(i);
        let email = format!("user{}@example.com", i);
        let key =
            serialize_constraint_key("Person", &[("email".to_string(), Value::String(email))]);
        assert_eq!(l0.constraint_index.get(&key), Some(&vid));
    }

    // Delete half the vertices
    for i in 0..50 {
        let vid = Vid::new(i);
        l0.delete_vertex(vid)?;
    }

    // Verify constraint index was cleaned up (only 50 entries remain)
    assert_eq!(l0.constraint_index.len(), 50);

    // Verify deleted vertices' keys are gone
    for i in 0..50 {
        let email = format!("user{}@example.com", i);
        let key =
            serialize_constraint_key("Person", &[("email".to_string(), Value::String(email))]);
        assert!(!l0.constraint_index.contains_key(&key));
    }

    Ok(())
}
