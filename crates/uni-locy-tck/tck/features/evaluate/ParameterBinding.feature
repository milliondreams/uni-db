Feature: Parameter Binding in Locy Programs

  Locy programs support $parameter references in MATCH WHERE, QUERY WHERE,
  and QUERY RETURN clauses, mirroring Cypher's query parameter semantics.
  Parameters are supplied via LocyConfig.params and keyed without the leading
  '$' (e.g., key "agent_id" binds "$agent_id").

  Background:
    Given an empty graph

  # ── QUERY WHERE with $param ───────────────────────────────────────────────

  Scenario: $param in QUERY WHERE filters to matching agent
    Given having executed:
      """
      CREATE (:Episode {agent_id: 'agent-1', action: 'login'}),
             (:Episode {agent_id: 'agent-2', action: 'logout'})
      """
    And the parameter agent_id = 'agent-1'
    When evaluating the following Locy program with params:
      """
      CREATE RULE episodes AS MATCH (e:Episode) YIELD KEY e, e.agent_id AS aid, e.action AS act
      QUERY episodes WHERE aid = $agent_id RETURN act
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where act = 'login'

  Scenario: $param in QUERY WHERE excludes non-matching rows
    Given having executed:
      """
      CREATE (:Episode {agent_id: 'agent-A', action: 'start'}),
             (:Episode {agent_id: 'agent-B', action: 'stop'})
      """
    And the parameter agent_id = 'agent-A'
    When evaluating the following Locy program with params:
      """
      CREATE RULE episodes AS MATCH (e:Episode) YIELD KEY e, e.agent_id AS aid
      QUERY episodes WHERE aid = $agent_id RETURN e.action AS act
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where act = 'start'

  # ── Rule MATCH WHERE with $param (DataFusion path) ────────────────────────

  Scenario: $param in rule MATCH WHERE scopes derived relation
    Given having executed:
      """
      CREATE (:Episode {agent_id: 'agent-X', label: 'alpha'}),
             (:Episode {agent_id: 'agent-Y', label: 'beta'})
      """
    And the parameter agent_id = 'agent-X'
    When evaluating the following Locy program with params:
      """
      CREATE RULE scoped AS
        MATCH (e:Episode)
        WHERE e.agent_id = $agent_id
        YIELD KEY e, e.label AS lbl
      QUERY scoped RETURN lbl
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where lbl = 'alpha'

  # ── Integer and numeric params ─────────────────────────────────────────────

  Scenario: Integer $param used in WHERE comparison
    Given having executed:
      """
      CREATE (:Score {name: 'low',  val: 30}),
             (:Score {name: 'high', val: 80})
      """
    And the parameter threshold = 50
    When evaluating the following Locy program with params:
      """
      CREATE RULE scores AS MATCH (s:Score) YIELD KEY s, s.val AS v, s.name AS nm
      QUERY scores WHERE v > $threshold RETURN nm
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where nm = 'high'

  # ── Multiple params ────────────────────────────────────────────────────────

  Scenario: Multiple $params filter on two dimensions
    Given having executed:
      """
      CREATE (:Event {agent_id: 'a1', kind: 'login',  score: 0.9}),
             (:Event {agent_id: 'a1', kind: 'logout', score: 0.2}),
             (:Event {agent_id: 'a2', kind: 'login',  score: 0.7})
      """
    And the parameter agent_id = 'a1'
    And the parameter min_score = 0.5
    When evaluating the following Locy program with params:
      """
      CREATE RULE events AS
        MATCH (e:Event)
        YIELD KEY e, e.agent_id AS aid, e.kind AS kind, e.score AS score
      QUERY events WHERE aid = $agent_id AND score > $min_score RETURN kind
      """
    Then evaluation should succeed
    And the command result 0 should be a Query with 1 rows
    And the command result 0 should be a Query containing row where kind = 'login'
