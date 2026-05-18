// Profile target: same workload as timing_breakdown at N=24 only, repeated
// long enough for samply / perf to get useful samples.
//
// Run with:
//   cargo build --release --example timing_cypher_profile
//   samply record --rate 4000 target/release/examples/timing_cypher_profile

use std::sync::Arc;
use std::time::Instant;
use uni_db::Uni;

const STATEMENTS_PER_TASK: usize = 100;
const N_SESSIONS: usize = 24;
const REPS: usize = 30; // 30 reps × 24 tasks × 100 stmts = 72k CREATEs

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let db = Arc::new(Uni::in_memory().build().await?);
    let total = Instant::now();
    for rep in 0..REPS {
        let mut handles = Vec::with_capacity(N_SESSIONS);
        for s in 0..N_SESSIONS {
            let db = db.clone();
            handles.push(tokio::spawn(async move {
                let session = db.session();
                let tx = session.tx().await.unwrap();
                for i in 0..STATEMENTS_PER_TASK {
                    tx.execute(&format!(
                        "CREATE (n:Person {{idx: {i}, sess: {s}, rep: {rep}}})"
                    ))
                    .await
                    .unwrap();
                }
                tx.commit().await.unwrap();
            }));
        }
        for h in handles {
            h.await?;
        }
    }
    eprintln!(
        "Done: {} reps × {} sessions × {} stmts in {:?}",
        REPS,
        N_SESSIONS,
        STATEMENTS_PER_TASK,
        total.elapsed()
    );
    Ok(())
}
