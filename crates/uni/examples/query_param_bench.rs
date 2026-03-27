// Micro-benchmark: String interpolation vs Parameterized queries
// Run with: cargo run --release --example query_param_bench

use serde_json::json;
use std::time::Instant;
use uni_db::{Uni, Value};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔬 Query Parameter Benchmark: String Interpolation vs Parameterized\n");
    println!("Testing: 50 CREATE queries with 128-dim embeddings\n");

    const ITERATIONS: usize = 50;

    // Setup
    let embedding: Vec<f32> = (0..128).map(|x| x as f32).collect();

    // Create two separate databases for clean comparison
    let db1 = Uni::open("/tmp/uni_bench1").build().await?;
    let db2 = Uni::open("/tmp/uni_bench2").build().await?;

    // Schema is created dynamically on first insert

    println!("📊 Approach 1: String Interpolation (current)\n");
    println!("   Query example:");
    let embedding_str = json!(embedding).to_string();
    let example = format!(
        "CREATE (n:Person {{name: 'Bench_0', age: 30, embedding: {}}})",
        &embedding_str[..60] // Show first 60 chars
    );
    println!("   {}", example);
    println!(
        "   Query length: ~{} bytes\n",
        format!(
            "CREATE (n:Person {{name: 'Bench_0', age: 30, embedding: {}}})",
            embedding_str
        )
        .len()
    );

    // Benchmark 1: String interpolation (current approach)
    let start = Instant::now();
    for i in 0..ITERATIONS {
        let embedding_str = json!(embedding).to_string();
        let cypher = format!(
            "CREATE (n:Person {{name: 'Bench_{}', age: 30, embedding: {}}})",
            i, embedding_str
        );
        db1.session().execute(&cypher).await?;
    }
    let duration1 = start.elapsed();
    let per_query1 = duration1.as_micros() as f64 / ITERATIONS as f64;

    println!("✅ Completed: {} queries in {:?}", ITERATIONS, duration1);
    println!("   Average: {:.1}µs per query\n", per_query1);

    println!("---\n");

    println!("📊 Approach 2: Parameterized Queries\n");
    println!("   Query example:");
    println!("   CREATE (n:Person {{name: $name, age: $age, embedding: $embedding}})");
    println!("   Query length: ~70 bytes");
    println!("   + Parameters passed as structured data\n");

    // Benchmark 2: Parameterized queries
    let embedding_value = Value::List(embedding.iter().map(|&f| Value::Float(f as f64)).collect());

    let start = Instant::now();
    let cypher = "CREATE (n:Person {name: $name, age: $age, embedding: $embedding})";

    for i in 0..ITERATIONS {
        db2.session()
            .query_with(cypher)
            .param("name", Value::String(format!("Bench_{}", i)))
            .param("age", Value::Int(30))
            .param("embedding", embedding_value.clone())
            .execute()
            .await?;
    }
    let duration2 = start.elapsed();
    let per_query2 = duration2.as_micros() as f64 / ITERATIONS as f64;

    println!("✅ Completed: {} queries in {:?}", ITERATIONS, duration2);
    println!("   Average: {:.1}µs per query\n", per_query2);

    println!("═══════════════════════════════════════════════════════════════");
    println!("\n📈 RESULTS\n");

    println!("┌─────────────────────────┬──────────────┬──────────────┐");
    println!("│ Approach                │ Total Time   │ Per Query    │");
    println!("├─────────────────────────┼──────────────┼──────────────┤");
    println!(
        "│ String Interpolation    │ {:>9.2}ms │ {:>9.1}µs │",
        duration1.as_micros() as f64 / 1000.0,
        per_query1
    );
    println!(
        "│ Parameterized Queries   │ {:>9.2}ms │ {:>9.1}µs │",
        duration2.as_micros() as f64 / 1000.0,
        per_query2
    );
    println!("└─────────────────────────┴──────────────┴──────────────┘");

    let speedup = per_query1 / per_query2;
    let savings_us = per_query1 - per_query2;
    let savings_pct = (savings_us / per_query1) * 100.0;

    println!("\n🎯 Performance Improvement:");
    println!("   Speedup: {:.2}x faster", speedup);
    println!(
        "   Savings: {:.1}µs per query ({:.1}% reduction)",
        savings_us, savings_pct
    );

    if savings_pct > 5.0 {
        println!("\n✅ Parameterized queries are significantly faster!");
    } else {
        println!("\n⚠️  Difference is small - other factors may dominate.");
    }

    println!("\n═══════════════════════════════════════════════════════════════");

    // Detailed breakdown
    println!("\n🔍 Overhead Breakdown (String Interpolation):\n");

    // Measure just JSON serialization
    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let _embedding_str = json!(embedding).to_string();
    }
    let json_overhead = start.elapsed().as_micros() as f64 / ITERATIONS as f64;
    println!("   JSON serialization:     {:.1}µs", json_overhead);

    // Measure string formatting
    let embedding_str = json!(embedding).to_string();
    let start = Instant::now();
    for i in 0..ITERATIONS {
        let _cypher = format!(
            "CREATE (n:Person {{name: 'Bench_{}', age: 30, embedding: {}}})",
            i, embedding_str
        );
    }
    let format_overhead = start.elapsed().as_micros() as f64 / ITERATIONS as f64;
    println!("   String concatenation:   {:.1}µs", format_overhead);

    let parse_execute_overhead = per_query1 - json_overhead - format_overhead;
    println!("   Parse + Execute:        {:.1}µs", parse_execute_overhead);
    println!("   ─────────────────────────────────");
    println!("   Total:                  {:.1}µs\n", per_query1);

    println!("🔍 Overhead Breakdown (Parameterized):\n");

    // Measure parameter preparation
    let start = Instant::now();
    for i in 0..ITERATIONS {
        let _name = Value::String(format!("Bench_{}", i));
        let _age = Value::Int(30);
        let _embedding = embedding_value.clone();
    }
    let param_overhead = start.elapsed().as_micros() as f64 / ITERATIONS as f64;
    println!("   Parameter preparation:  {:.1}µs", param_overhead);

    let parse_execute_overhead2 = per_query2 - param_overhead;
    println!(
        "   Parse + Execute:        {:.1}µs",
        parse_execute_overhead2
    );
    println!("   ─────────────────────────────────");
    println!("   Total:                  {:.1}µs\n", per_query2);

    println!("💡 Key Insight:");
    let parsing_savings = parse_execute_overhead - parse_execute_overhead2;
    if parsing_savings > 0.0 {
        println!(
            "   Parsing is {:.1}µs faster with parameters ({:.1}% reduction)",
            parsing_savings,
            (parsing_savings / parse_execute_overhead) * 100.0
        );
        println!("   This confirms that parsing large array literals is expensive!");
    }

    Ok(())
}
