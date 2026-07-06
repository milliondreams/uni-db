pub mod test_issue_112_transaction_edge_versions;
pub mod test_issue_143_oom_guards;
pub mod test_issue_18_150_poisoned_mutex;
pub mod test_issue_19_tx_memory_limit;
pub mod test_issue_25_cascade_deletion;
pub mod test_issue_27_chunked_index;
pub mod test_issue_29_vid_labels_index;
pub mod test_issue_43_uid_index;
pub mod test_issue_4_constraint_check;
pub mod test_issue_53_edge_properties_after_compaction;
pub mod test_issue_54_adjacency_compaction_visibility;
pub mod test_issue_62_recursive_decoder;
pub mod test_issue_75_batch_frontier;
pub mod test_issue_77_ghost_vertex_endpoints;

// Correctness-scan Wave 0 repros (R3 tombstone resurrection).
pub mod repro_11_compact_adjacency_empty_resurrect;
pub mod test_repro_compaction_empty_resurrect;
