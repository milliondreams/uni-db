// SPDX-License-Identifier: Apache-2.0
// Copyright 2024-2026 Dragonscale Team

use uni_cypher::ast::*;

fn parse_schema(input: &str) -> SchemaCommand {
    let query = uni_cypher::parse(input).unwrap();

    let Query::Schema(cmd) = query else {
        panic!("Expected schema command");
    };
    *cmd
}

#[test]
fn test_create_label() {
    let input = "CREATE LABEL Person (name STRING NOT NULL, age INT)";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };

    assert_eq!(c.name, "Person");
    assert_eq!(c.properties.len(), 2);

    assert_eq!(c.properties[0].name, "name");
    assert_eq!(c.properties[0].data_type, "STRING");
    assert!(!c.properties[0].nullable);

    assert_eq!(c.properties[1].name, "age");
    assert_eq!(c.properties[1].data_type, "INT");
    assert!(c.properties[1].nullable); // Default is nullable
}

#[test]
fn test_property_vector_dim_type() {
    let cmd = parse_schema("CREATE LABEL Doc (embedding VECTOR(768))");
    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };
    assert_eq!(c.properties[0].data_type, "VECTOR(768)");
}

#[test]
fn test_property_list_and_nested_types() {
    let cmd = parse_schema("CREATE LABEL Doc (tags LIST<STRING>, tokens LIST<VECTOR(128)>)");
    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };
    assert_eq!(c.properties[0].data_type, "LIST<STRING>");
    assert_eq!(c.properties[1].data_type, "LIST<VECTOR(128)>");
}

#[test]
fn test_property_map_types() {
    // Scalar + nested map value types; whitespace after the comma is optional and the
    // captured span is forwarded verbatim to the backend parse_data_type.
    let cmd = parse_schema(
        "CREATE LABEL Doc (a MAP<STRING, FLOAT>, b MAP<STRING,INT>, \
         c MAP<STRING, LIST<INT>>, d MAP<STRING, MAP<STRING, INT>>)",
    );
    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };
    assert_eq!(c.properties[0].data_type, "MAP<STRING, FLOAT>");
    assert_eq!(c.properties[1].data_type, "MAP<STRING,INT>");
    assert_eq!(c.properties[2].data_type, "MAP<STRING, LIST<INT>>");
    assert_eq!(c.properties[3].data_type, "MAP<STRING, MAP<STRING, INT>>");
}

#[test]
fn test_map_type_does_not_swallow_constraints() {
    let cmd = parse_schema(
        "CREATE LABEL Doc (m MAP<STRING, INT> NOT NULL, e MAP<STRING, FLOAT> DESCRIPTION 'scores', k INT)",
    );
    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };
    assert_eq!(c.properties[0].data_type, "MAP<STRING, INT>");
    assert!(!c.properties[0].nullable, "NOT NULL must not be swallowed");
    assert_eq!(c.properties[1].data_type, "MAP<STRING, FLOAT>");
    // The map type didn't consume the following property.
    assert_eq!(c.properties[2].name, "k");
    assert_eq!(c.properties[2].data_type, "INT");
}

#[test]
fn test_map_keyword_usable_as_identifier() {
    // `map`/`MAP` must remain usable as a label name and a property name — the `!ident_char`
    // guard keeps the `type_map` rule from shadowing ordinary identifiers.
    let cmd = parse_schema("CREATE LABEL Map (map STRING, mapping INT)");
    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };
    assert_eq!(c.name, "Map");
    assert_eq!(c.properties[0].name, "map");
    assert_eq!(c.properties[0].data_type, "STRING");
    assert_eq!(c.properties[1].name, "mapping");
    assert_eq!(c.properties[1].data_type, "INT");
}

#[test]
fn test_parameterized_type_does_not_swallow_constraints() {
    // The parameterized type must not greedily consume a following constraint.
    let cmd = parse_schema(
        "CREATE LABEL Doc (v VECTOR(768) NOT NULL, tags LIST<STRING> UNIQUE, e VECTOR(4) DESCRIPTION 'emb')",
    );
    let SchemaCommand::CreateLabel(c) = cmd else {
        panic!("Expected CreateLabel");
    };
    assert_eq!(c.properties[0].data_type, "VECTOR(768)");
    assert!(!c.properties[0].nullable);
    assert_eq!(c.properties[1].data_type, "LIST<STRING>");
    assert!(c.properties[1].unique);
    assert_eq!(c.properties[2].data_type, "VECTOR(4)");
    assert_eq!(c.properties[2].description.as_deref(), Some("emb"));
}

#[test]
fn test_edge_type_parameterized_property() {
    let cmd = parse_schema("CREATE EDGE TYPE SIM (embedding VECTOR(16)) FROM A TO B");
    let SchemaCommand::CreateEdgeType(c) = cmd else {
        panic!("Expected CreateEdgeType");
    };
    assert_eq!(c.properties[0].data_type, "VECTOR(16)");
}

#[test]
fn test_alter_add_parameterized_property() {
    let cmd = parse_schema("ALTER LABEL Doc ADD PROPERTY tokens LIST<VECTOR(2)>");
    let SchemaCommand::AlterLabel(c) = cmd else {
        panic!("Expected AlterLabel");
    };
    let AlterAction::AddProperty(prop) = &c.action else {
        panic!("Expected AddProperty action");
    };
    assert_eq!(prop.name, "tokens");
    assert_eq!(prop.data_type, "LIST<VECTOR(2)>");
}

#[test]
fn test_create_edge_type() {
    let input = "CREATE EDGE TYPE FRIENDS (since INT) FROM Person TO Person";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateEdgeType(c) = cmd else {
        panic!("Expected CreateEdgeType");
    };

    assert_eq!(c.name, "FRIENDS");
    assert_eq!(c.src_labels, vec!["Person"]);
    assert_eq!(c.dst_labels, vec!["Person"]);
    assert_eq!(c.properties.len(), 1);
    assert_eq!(c.properties[0].name, "since");
    assert_eq!(c.properties[0].data_type, "INT");
}

#[test]
fn test_create_constraint_unique() {
    let input = "CREATE CONSTRAINT ON (n:Person) ASSERT n.email IS UNIQUE";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateConstraint(c) = cmd else {
        panic!("Expected CreateConstraint");
    };

    assert_eq!(c.label, "Person");
    assert!(matches!(c.constraint_type, ConstraintType::Unique));
    assert_eq!(c.properties, vec!["email"]);
}

#[test]
fn test_create_constraint_exists() {
    let input = "CREATE CONSTRAINT ON (n:Person) ASSERT EXISTS(n.name)";
    let cmd = parse_schema(input);

    let SchemaCommand::CreateConstraint(c) = cmd else {
        panic!("Expected CreateConstraint");
    };

    assert_eq!(c.label, "Person");
    assert!(matches!(c.constraint_type, ConstraintType::Exists));
    assert_eq!(c.properties, vec!["name"]);
}

#[test]
fn test_alter_label() {
    let input = "ALTER LABEL Person ADD PROPERTY email STRING";
    let cmd = parse_schema(input);

    let SchemaCommand::AlterLabel(c) = cmd else {
        panic!("Expected AlterLabel");
    };

    assert_eq!(c.name, "Person");
    let AlterAction::AddProperty(prop) = &c.action else {
        panic!("Expected AddProperty action");
    };
    assert_eq!(prop.name, "email");
    assert_eq!(prop.data_type, "STRING");
}
