use std::collections::{HashMap, HashSet, VecDeque};

use super::dependency::DependencyGraph;
use super::errors::LocyCompileError;

/// Result of stratification: SCCs, topological order, and recursion flags.
pub struct StratificationResult {
    /// Each SCC is a set of mutually-recursive rule names.
    pub sccs: Vec<HashSet<String>>,
    /// Topologically sorted SCC indices (evaluation order).
    pub scc_order: Vec<usize>,
    /// Maps rule name → SCC index.
    pub scc_map: HashMap<String, usize>,
    /// Per-SCC recursion flag.
    pub is_recursive: Vec<bool>,
    /// Per-SCC: which other SCCs it depends on.
    pub scc_depends_on: Vec<HashSet<usize>>,
}

/// Run Tarjan's SCC algorithm, check for cyclic negation, and produce
/// a topological evaluation order.
pub fn stratify(graph: &DependencyGraph) -> Result<StratificationResult, LocyCompileError> {
    // Build combined adjacency (positive + negative) for SCC detection
    let mut adj: HashMap<&str, HashSet<&str>> = HashMap::new();
    for rule in &graph.all_rules {
        adj.entry(rule.as_str()).or_default();
    }
    for (from, tos) in &graph.positive_edges {
        for to in tos {
            adj.entry(from.as_str()).or_default().insert(to.as_str());
        }
    }
    for (from, tos) in &graph.negative_edges {
        for to in tos {
            adj.entry(from.as_str()).or_default().insert(to.as_str());
        }
    }

    // Sort nodes for deterministic traversal
    let mut rules: Vec<&str> = graph.all_rules.iter().map(|s| s.as_str()).collect();
    rules.sort();

    let sccs = tarjan(&rules, &adj);

    // Build scc_map
    let mut scc_map: HashMap<String, usize> = HashMap::new();
    for (i, scc) in sccs.iter().enumerate() {
        for rule in scc {
            scc_map.insert(rule.clone(), i);
        }
    }

    // Check for cyclic negation: negative edge within the same SCC
    for (from, tos) in &graph.negative_edges {
        for to in tos {
            let from_scc = scc_map[from.as_str()];
            let to_scc = scc_map[to.as_str()];
            if from_scc == to_scc {
                let mut rules: Vec<String> = sccs[from_scc].iter().cloned().collect();
                rules.sort();
                return Err(LocyCompileError::CyclicNegation { rules });
            }
        }
    }

    // Determine recursiveness per SCC
    let mut is_recursive = vec![false; sccs.len()];
    for (i, scc) in sccs.iter().enumerate() {
        if scc.len() > 1 {
            is_recursive[i] = true;
        } else {
            // Single-node SCC: recursive only if it has a self-edge
            let rule = scc.iter().next().unwrap();
            let has_self_edge = graph
                .positive_edges
                .get(rule.as_str())
                .is_some_and(|deps| deps.contains(rule));
            is_recursive[i] = has_self_edge;
        }
    }

    // Build SCC-level condensation DAG
    let mut scc_depends_on: Vec<HashSet<usize>> = vec![HashSet::new(); sccs.len()];
    for (from, tos) in graph
        .positive_edges
        .iter()
        .chain(graph.negative_edges.iter())
    {
        let from_scc = scc_map[from.as_str()];
        for to in tos {
            let to_scc = scc_map[to.as_str()];
            if from_scc != to_scc {
                scc_depends_on[from_scc].insert(to_scc);
            }
        }
    }

    // Topological sort via Kahn's algorithm
    let n = sccs.len();
    let mut in_degree = vec![0usize; n];
    let mut reverse_deps: Vec<Vec<usize>> = vec![vec![]; n];
    for (i, deps) in scc_depends_on.iter().enumerate() {
        for &dep in deps {
            reverse_deps[dep].push(i);
        }
        in_degree[i] = deps.len();
    }

    let mut queue: VecDeque<usize> = VecDeque::new();
    for (i, &deg) in in_degree.iter().enumerate() {
        if deg == 0 {
            queue.push_back(i);
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(node) = queue.pop_front() {
        order.push(node);
        for &dependent in &reverse_deps[node] {
            in_degree[dependent] -= 1;
            if in_degree[dependent] == 0 {
                queue.push_back(dependent);
            }
        }
    }

    Ok(StratificationResult {
        sccs,
        scc_order: order,
        scc_map,
        is_recursive,
        scc_depends_on,
    })
}

// ─── Tarjan's SCC ────────────────────────────────────────────────────────────

fn tarjan(nodes: &[&str], adj: &HashMap<&str, HashSet<&str>>) -> Vec<HashSet<String>> {
    struct State<'a> {
        index_counter: usize,
        stack: Vec<&'a str>,
        on_stack: HashSet<&'a str>,
        index: HashMap<&'a str, usize>,
        lowlink: HashMap<&'a str, usize>,
        sccs: Vec<HashSet<String>>,
    }

    fn strongconnect<'a>(v: &'a str, adj: &HashMap<&str, HashSet<&'a str>>, state: &mut State<'a>) {
        state.index.insert(v, state.index_counter);
        state.lowlink.insert(v, state.index_counter);
        state.index_counter += 1;
        state.stack.push(v);
        state.on_stack.insert(v);

        if let Some(neighbors) = adj.get(v) {
            for &w in neighbors {
                if !state.index.contains_key(w) {
                    strongconnect(w, adj, state);
                    let w_low = state.lowlink[w];
                    let v_low = state.lowlink[v];
                    if w_low < v_low {
                        state.lowlink.insert(v, w_low);
                    }
                } else if state.on_stack.contains(w) {
                    let w_idx = state.index[w];
                    let v_low = state.lowlink[v];
                    if w_idx < v_low {
                        state.lowlink.insert(v, w_idx);
                    }
                }
            }
        }

        if state.lowlink[v] == state.index[v] {
            let mut scc = HashSet::new();
            loop {
                let w = state.stack.pop().unwrap();
                state.on_stack.remove(w);
                scc.insert(w.to_string());
                if w == v {
                    break;
                }
            }
            state.sccs.push(scc);
        }
    }

    let mut state = State {
        index_counter: 0,
        stack: Vec::new(),
        on_stack: HashSet::new(),
        index: HashMap::new(),
        lowlink: HashMap::new(),
        sccs: Vec::new(),
    };

    for &node in nodes {
        if !state.index.contains_key(node) {
            strongconnect(node, adj, &mut state);
        }
    }

    state.sccs
}
