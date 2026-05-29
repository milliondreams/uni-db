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

/// Iterative Tarjan SCC.
///
/// Uses an explicit work stack so the recursion depth is no longer bounded
/// by the program's thread stack. Locy programs with thousands of mutually
/// dependent rules (e.g. long linear dependency chains) would previously
/// risk stack overflow in the recursive form; the iterative version is
/// bounded only by heap.
fn tarjan(nodes: &[&str], adj: &HashMap<&str, HashSet<&str>>) -> Vec<HashSet<String>> {
    // Per-node bookkeeping.
    let mut index_counter: usize = 0;
    let mut scc_stack: Vec<&str> = Vec::new();
    let mut on_stack: HashSet<&str> = HashSet::new();
    let mut index: HashMap<&str, usize> = HashMap::new();
    let mut lowlink: HashMap<&str, usize> = HashMap::new();
    let mut sccs: Vec<HashSet<String>> = Vec::new();

    /// One iterative call frame, encoding the state of an in-flight
    /// `strongconnect(v)`: we are currently iterating `v`'s neighbours,
    /// with `cursor` neighbours already processed.
    struct Frame<'a> {
        v: &'a str,
        neighbours: Vec<&'a str>,
        cursor: usize,
    }

    let snapshot_neighbours = |v: &str| -> Vec<&str> {
        adj.get(v)
            .map(|set| set.iter().copied().collect())
            .unwrap_or_default()
    };

    // Iterate the input slice in caller-provided order so behaviour matches
    // the previous recursive implementation, which used direct recursion in
    // the same order.
    for &root in nodes {
        if index.contains_key(root) {
            continue;
        }

        let mut work: Vec<Frame<'_>> = Vec::new();

        // Visit `root` and push its frame.
        index.insert(root, index_counter);
        lowlink.insert(root, index_counter);
        index_counter += 1;
        scc_stack.push(root);
        on_stack.insert(root);
        work.push(Frame {
            v: root,
            neighbours: snapshot_neighbours(root),
            cursor: 0,
        });

        while let Some(frame) = work.last_mut() {
            // Process the next outgoing edge of `frame.v`.
            if frame.cursor < frame.neighbours.len() {
                let w = frame.neighbours[frame.cursor];
                frame.cursor += 1;
                if !index.contains_key(w) {
                    // Recurse into `w`: visit, then push its frame and
                    // resume `frame` (with cursor already advanced) on
                    // unwind.
                    index.insert(w, index_counter);
                    lowlink.insert(w, index_counter);
                    index_counter += 1;
                    scc_stack.push(w);
                    on_stack.insert(w);
                    work.push(Frame {
                        v: w,
                        neighbours: snapshot_neighbours(w),
                        cursor: 0,
                    });
                } else if on_stack.contains(w) {
                    let v = frame.v;
                    let w_idx = index[w];
                    let v_low = lowlink[v];
                    if w_idx < v_low {
                        lowlink.insert(v, w_idx);
                    }
                }
                continue;
            }

            // All neighbours processed: finalize `v`.
            let v = frame.v;
            let v_low = lowlink[v];
            let v_idx = index[v];

            if v_low == v_idx {
                let mut scc = HashSet::new();
                loop {
                    // The SCC stack is nonempty by construction: `v` was
                    // pushed when its frame was created and has not yet
                    // been popped.
                    let w = scc_stack
                        .pop()
                        .expect("Tarjan SCC stack underflow — invariant violated");
                    on_stack.remove(w);
                    scc.insert(w.to_string());
                    if w == v {
                        break;
                    }
                }
                sccs.push(scc);
            }

            // Pop `frame` and propagate `v`'s lowlink up to the parent
            // (mirrors the post-recursive update in the original code).
            work.pop();
            if let Some(parent) = work.last_mut() {
                let p = parent.v;
                let p_low = lowlink[p];
                if v_low < p_low {
                    lowlink.insert(p, v_low);
                }
            }
        }
    }

    sccs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deeply linear dependency chain `r_0 → r_1 → ... → r_{N-1}` would
    /// previously blow the thread stack via recursive `strongconnect`.
    /// The iterative implementation is bounded only by heap.
    #[test]
    fn deep_linear_chain_does_not_overflow_stack() {
        const N: usize = 5_000;

        let mut graph = DependencyGraph {
            positive_edges: HashMap::new(),
            negative_edges: HashMap::new(),
            all_rules: HashSet::new(),
        };
        for i in 0..N {
            let name = format!("r_{i}");
            graph.all_rules.insert(name.clone());
            if i + 1 < N {
                let next = format!("r_{}", i + 1);
                graph.positive_edges.entry(name).or_default().insert(next);
            }
        }

        let result = stratify(&graph).expect("stratify must succeed for an acyclic chain");
        assert_eq!(result.sccs.len(), N, "each rule should be its own SCC");
        assert!(
            result.is_recursive.iter().all(|&r| !r),
            "no rule should be flagged recursive in a pure chain"
        );
    }

    /// Mutual recursion `a ⇄ b` should collapse to a single SCC and be
    /// flagged as recursive (same behaviour as the previous recursive
    /// implementation).
    #[test]
    fn two_cycle_collapses_to_one_recursive_scc() {
        let mut graph = DependencyGraph {
            positive_edges: HashMap::new(),
            negative_edges: HashMap::new(),
            all_rules: HashSet::new(),
        };
        graph.all_rules.insert("a".to_owned());
        graph.all_rules.insert("b".to_owned());
        graph
            .positive_edges
            .entry("a".to_owned())
            .or_default()
            .insert("b".to_owned());
        graph
            .positive_edges
            .entry("b".to_owned())
            .or_default()
            .insert("a".to_owned());

        let result = stratify(&graph).expect("stratify must succeed");
        assert_eq!(result.sccs.len(), 1);
        assert_eq!(result.sccs[0].len(), 2);
        assert!(result.is_recursive[0]);
    }
}
