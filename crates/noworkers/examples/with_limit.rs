//! Concurrency limiting with noworkers.
//!
//! This example shows how to limit the number of concurrent tasks
//! to prevent resource exhaustion and control parallelism.

use noworkers::Workers;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Concurrency Limiting Example");
    println!("============================\n");

    // Track concurrent task count
    let concurrent_count = Arc::new(AtomicUsize::new(0));
    let max_seen = Arc::new(AtomicUsize::new(0));

    let mut workers = Workers::new();
    
    // Limit to 3 concurrent tasks
    let limit = 3;
    workers.with_limit(limit);
    println!("Concurrency limit set to: {}\n", limit);

    let start = Instant::now();

    // Spawn 10 tasks, but only 3 will run at a time
    for i in 0..10 {
        let count = concurrent_count.clone();
        let max = max_seen.clone();
        
        workers
            .add(move |_cancel| async move {
                // Increment concurrent count
                let current = count.fetch_add(1, Ordering::SeqCst) + 1;
                
                // Track maximum concurrency
                let mut max_val = max.load(Ordering::SeqCst);
                while current > max_val {
                    match max.compare_exchange_weak(
                        max_val,
                        current,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => break,
                        Err(x) => max_val = x,
                    }
                }
                
                println!(
                    "[{:>3}ms] Task {:>2} started (concurrent: {})",
                    start.elapsed().as_millis(),
                    i,
                    current
                );
                
                // Simulate work
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                
                // Decrement concurrent count
                let remaining = count.fetch_sub(1, Ordering::SeqCst) - 1;
                println!(
                    "[{:>3}ms] Task {:>2} finished (remaining: {})",
                    start.elapsed().as_millis(),
                    i,
                    remaining
                );
                
                Ok(())
            })
            .await?;
    }

    workers.wait().await?;

    println!("\n========== Summary ==========");
    println!("Total time: {}ms", start.elapsed().as_millis());
    println!("Max concurrent tasks: {}", max_seen.load(Ordering::SeqCst));
    println!("Expected batches: {} (10 tasks / 3 limit)", (10 + limit - 1) / limit);
    println!("Expected time: ~{}ms (4 batches * 200ms)", ((10 + limit - 1) / limit) * 200);

    Ok(())
}