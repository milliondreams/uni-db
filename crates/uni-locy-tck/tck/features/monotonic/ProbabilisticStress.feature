Feature: Probabilistic Stress Corpus

  Adversarial edge-case scenarios for MNOR/MPROD composition.
  Covers clamping, multi-layer stacking, recursive MNOR,
  overloaded clauses, ASSUME interactions, and degenerate values.

  Corpus reference: Probabilistic Reasoning Spec v1.1 §Semantic Stress Corpus

  Background:
    Given an empty graph

  # ── H3a: MNOR clamps out-of-range inputs ──────────────────────────────

  Scenario: MNOR clamps inputs exceeding 1.0
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:CAUSE {strength: 1.5}]->(b:Node {name: 'B'}),
             (a)-[:CAUSE {strength: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Node)-[e:CAUSE]->(b:Node)
        FOLD p = MNOR(e.strength)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 1.0

  # ── H3b: MPROD clamps out-of-range inputs ─────────────────────────────

  Scenario: MPROD clamps inputs exceeding 1.0
    Given having executed:
      """
      CREATE (a:Node {name: 'A'})-[:REQ {avail: 2.0}]->(b:Node {name: 'B'}),
             (a)-[:REQ {avail: 0.5}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE joint AS
        MATCH (a:Node)-[e:REQ]->(b:Node)
        FOLD p = MPROD(e.avail)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'joint' should have 1 facts
    And the derived relation 'joint' should contain a fact where p = 0.5

  # ── I1: BEST BY ASC selects minimum probability ───────────────────────

  Scenario: BEST BY ASC selects weakest link
    Given having executed:
      """
      CREATE (s:System {name: 'Sys'})-[:ROUTE {reliability: 0.95}]->(r1:Route {name: 'R1'}),
             (s)-[:ROUTE {reliability: 0.70}]->(r2:Route {name: 'R2'}),
             (s)-[:ROUTE {reliability: 0.85}]->(r3:Route {name: 'R3'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE weakest AS
        MATCH (s:System)-[e:ROUTE]->(r:Route)
        BEST BY e.reliability ASC
        YIELD KEY s, e.reliability AS reliability
      """
    Then evaluation should succeed
    And the derived relation 'weakest' should have 1 facts
    And the derived relation 'weakest' should contain a fact where reliability = 0.70

  # ── F1: Overloaded clauses combine via MNOR ────────────────────────────

  Scenario: Overloaded rule clauses combine via MNOR
    Given having executed:
      """
      CREATE (a:Zone {name: 'A'})-[:SENSOR {prob: 0.3}]->(b:Zone {name: 'B'}),
             (a)-[:SENSOR {prob: 0.5}]->(b),
             (a)-[:ALARM {prob: 0.2}]->(b),
             (a)-[:ALARM {prob: 0.4}]->(b)
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Zone)-[e:SENSOR]->(b:Zone)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      CREATE RULE risk AS
        MATCH (a:Zone)-[e:ALARM]->(b:Zone)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should have 1 facts
    And the derived relation 'risk' should contain a fact where p = 0.832

  # ── H2: MPROD with 20 components ──────────────────────────────────────

  Scenario: MPROD with 20 near-one factors
    Given having executed:
      """
      CREATE (s:System {name: 'Sys'})-[:DEP {avail: 0.99}]->(c1:Comp {name: 'C1'}),
             (s)-[:DEP {avail: 0.99}]->(c2:Comp {name: 'C2'}),
             (s)-[:DEP {avail: 0.99}]->(c3:Comp {name: 'C3'}),
             (s)-[:DEP {avail: 0.99}]->(c4:Comp {name: 'C4'}),
             (s)-[:DEP {avail: 0.99}]->(c5:Comp {name: 'C5'}),
             (s)-[:DEP {avail: 0.99}]->(c6:Comp {name: 'C6'}),
             (s)-[:DEP {avail: 0.99}]->(c7:Comp {name: 'C7'}),
             (s)-[:DEP {avail: 0.99}]->(c8:Comp {name: 'C8'}),
             (s)-[:DEP {avail: 0.99}]->(c9:Comp {name: 'C9'}),
             (s)-[:DEP {avail: 0.99}]->(c10:Comp {name: 'C10'}),
             (s)-[:DEP {avail: 0.99}]->(c11:Comp {name: 'C11'}),
             (s)-[:DEP {avail: 0.99}]->(c12:Comp {name: 'C12'}),
             (s)-[:DEP {avail: 0.99}]->(c13:Comp {name: 'C13'}),
             (s)-[:DEP {avail: 0.99}]->(c14:Comp {name: 'C14'}),
             (s)-[:DEP {avail: 0.99}]->(c15:Comp {name: 'C15'}),
             (s)-[:DEP {avail: 0.99}]->(c16:Comp {name: 'C16'}),
             (s)-[:DEP {avail: 0.99}]->(c17:Comp {name: 'C17'}),
             (s)-[:DEP {avail: 0.99}]->(c18:Comp {name: 'C18'}),
             (s)-[:DEP {avail: 0.99}]->(c19:Comp {name: 'C19'}),
             (s)-[:DEP {avail: 0.99}]->(c20:Comp {name: 'C20'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE sys_avail AS
        MATCH (s:System)-[r:DEP]->(c:Comp)
        FOLD p = MPROD(r.avail)
        YIELD KEY s, p
      """
    Then evaluation should succeed
    And the derived relation 'sys_avail' should have 1 facts
    And the derived relation 'sys_avail' should contain a fact where p = 0.8179069376

  # ── B1: Three-layer MNOR→MPROD→MNOR stack ─────────────────────────────

  Scenario: Three-layer nested aggregation
    Given having executed:
      """
      CREATE (p:Plant {name: 'P1'})-[:HAS_ASM {rel: 1.0}]->(a1:Assembly {name: 'A1'}),
             (a1)-[:USES {rel: 1.0}]->(pt1:Part {name: 'Part1'}),
             (a1)-[:USES {rel: 1.0}]->(pt2:Part {name: 'Part2'}),
             (pt1)-[:SUPPLIED_BY {rel: 0.9}]->(s1:Supplier {name: 'S1'}),
             (pt1)-[:SUPPLIED_BY {rel: 0.8}]->(s2:Supplier {name: 'S2'}),
             (pt2)-[:SUPPLIED_BY {rel: 0.7}]->(s3:Supplier {name: 'S3'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE supply_avail AS
        MATCH (pt:Part)-[e:SUPPLIED_BY]->(s:Supplier)
        FOLD avail = MNOR(e.rel)
        YIELD KEY pt, avail

      CREATE RULE asm_avail AS
        MATCH (a:Assembly)-[:USES]->(pt:Part)
        WHERE pt IS supply_avail
        FOLD joint = MPROD(avail)
        YIELD KEY a, joint

      CREATE RULE plant_cap AS
        MATCH (p:Plant)-[:HAS_ASM]->(a:Assembly)
        WHERE a IS asm_avail
        FOLD cap = MNOR(joint)
        YIELD KEY p, cap
      """
    Then evaluation should succeed
    And the derived relation 'supply_avail' should have 2 facts
    And the derived relation 'supply_avail' should contain a fact where pt.name = 'Part1' and avail = 0.98
    And the derived relation 'supply_avail' should contain a fact where pt.name = 'Part2' and avail = 0.7
    And the derived relation 'asm_avail' should have 1 facts
    And the derived relation 'asm_avail' should contain a fact where joint = 0.686
    And the derived relation 'plant_cap' should have 1 facts
    And the derived relation 'plant_cap' should contain a fact where cap = 0.686

  # ── G2: MNOR of empty input set produces no facts ──────────────────────

  Scenario: MNOR with no matching edges yields zero facts
    Given having executed:
      """
      CREATE (pt:Part {name: 'Widget'})-[:SUPPLIED_BY {rel: 0.9}]->(s:Supplier {name: 'OnlySrc'}),
             (orphan:Part {name: 'Orphan'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE avail AS
        MATCH (pt:Part)-[e:SUPPLIED_BY]->(s:Supplier)
        FOLD prob = MNOR(e.rel)
        YIELD KEY pt, prob
      """
    Then evaluation should succeed
    And the derived relation 'avail' should have 1 facts
    And the derived relation 'avail' should contain a fact where prob = 0.9

  Scenario: ASSUME DELETE removes edges visible to Cypher
    Given having executed:
      """
      CREATE (pt:Part {name: 'Widget'})-[:SUPPLIED_BY {rel: 0.9}]->(s:Supplier {name: 'OnlySrc'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE avail AS
        MATCH (pt:Part)-[e:SUPPLIED_BY]->(s:Supplier)
        FOLD prob = MNOR(e.rel)
        YIELD KEY pt, prob

      ASSUME { MATCH (:Part)-[r:SUPPLIED_BY]->(:Supplier) DELETE r }
      THEN { MATCH (:Part)-[r:SUPPLIED_BY]->(:Supplier) RETURN r }
      """
    Then evaluation should succeed
    And the derived relation 'avail' should have 1 facts
    And the command result 0 should be an Assume with 0 rows

  # ── B2: Recursive MNOR over diamond (transitive risk combination) ──────

  Scenario: Recursive MNOR combines transitive paths over diamond
    Given having executed:
      """
      CREATE (a:Host {name: 'A'})-[:INFECTS {prob: 0.5}]->(b:Host {name: 'B'}),
             (a)-[:INFECTS {prob: 0.3}]->(c:Host {name: 'C'}),
             (b)-[:INFECTS {prob: 0.4}]->(c),
             (c)-[:INFECTS {prob: 0.6}]->(d:Host {name: 'D'})
      """
    When evaluating the following Locy program:
      """
      CREATE RULE risk AS
        MATCH (a:Host)-[e:INFECTS]->(b:Host)
        FOLD p = MNOR(e.prob)
        YIELD KEY a, KEY b, p

      CREATE RULE risk AS
        MATCH (a:Host)-[:INFECTS]->(mid:Host)
        WHERE mid IS risk TO b
        FOLD p = MNOR(p)
        YIELD KEY a, KEY b, p
      """
    Then evaluation should succeed
    And the derived relation 'risk' should contain a fact where a.name = 'A' and p = 0.58
    And the derived relation 'risk' should contain a fact where a.name = 'A' and p = 0.6
