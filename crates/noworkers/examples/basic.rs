//! Basic usage of the noworkers library.
//!
//! This example demonstrates the simplest way to use noworkers to spawn
//! and manage concurrent async tasks.

use noworkers::Workers;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Basic noworkers example");
    println!("=======================\n");

    // Create a new worker group
    let workers = Workers::new();

    // Spawn several async tasks
    for i in 0..5 {
        workers
            .add(move |_cancel| async move {
                println!("Task {} starting...", i);

                // Simulate some async work
                tokio::time::sleep(tokio::time::Duration::from_millis(100 * (i as u64 + 1))).await;

                println!("Task {} completed!", i);
                Ok(())
            })
            .await?;
    }

    println!("\nAll tasks spawned, waiting for completion...\n");

    // Wait for all tasks to complete
    workers.wait().await?;

    println!("\nAll tasks completed successfully!");

    Ok(())
}
