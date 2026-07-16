// Consolidated integration-test binary: every test file links into one
// binary to minimize compile/link time. See docs/test_layout.md.
// Add new integration tests as a `mod` here, NOT as a new tests/*.rs file.

mod repro_df_expr_engine;
mod repro_df_udfs_sync;
mod repro_function_rename;
mod repro_value_functions;
