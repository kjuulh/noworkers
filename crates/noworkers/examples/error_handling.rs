//! Error handling and propagation with noworkers.
//!
//! This example demonstrates how noworkers handles errors with
//! first-error-wins semantics, similar to Go's errgroup.

use noworkers::Workers;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Error Handling Example");
    println!("=====================\n");

    // Example 1: First error wins
    let _ = example_first_error_wins().await;
    
    // Example 2: Mixed success and failure
    let _ = example_mixed_results().await;
    
    // Example 3: All tasks succeed
    example_all_succeed().await?;

    Ok(())
}

async fn example_first_error_wins() -> anyhow::Result<()> {
    println!("1. First Error Wins");
    println!("-------------------");
    
    let workers = Workers::new();
    
    // Spawn multiple tasks that will fail at different times
    workers
        .add(|_| async {
            tokio::time::sleep(Duration::from_millis(300)).await;
            println!("  Task A: Failing after 300ms");
            Err(anyhow::anyhow!("Error from task A"))
        })
        .await?;
    
    workers
        .add(|_| async {
            tokio::time::sleep(Duration::from_millis(100)).await;
            println!("  Task B: Failing after 100ms (this should win)");
            Err(anyhow::anyhow!("Error from task B"))
        })
        .await?;
    
    workers
        .add(|_| async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            println!("  Task C: Failing after 200ms");
            Err(anyhow::anyhow!("Error from task C"))
        })
        .await?;
    
    let result = workers.wait().await;
    
    match result {
        Ok(_) => println!("  Unexpected success"),
        Err(e) => println!("  First error received: {}", e),
    }
    
    println!();
    Ok(())
}

async fn example_mixed_results() -> anyhow::Result<()> {
    println!("2. Mixed Success and Failure");
    println!("----------------------------");
    
    let workers = Workers::new();
    
    // Some tasks succeed
    for i in 0..3 {
        workers
            .add(move |cancel| async move {
                println!("  Success task {} started", i);
                
                // Check for cancellation
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(50)) => {
                        println!("  Success task {} completed", i);
                        Ok(())
                    }
                    _ = cancel.cancelled() => {
                        println!("  Success task {} cancelled", i);
                        Ok(())
                    }
                }
            })
            .await?;
    }
    
    // One task fails
    workers
        .add(|_| async {
            tokio::time::sleep(Duration::from_millis(150)).await;
            println!("  >>> Failure task triggering error");
            Err(anyhow::anyhow!("Intentional failure in mixed group"))
        })
        .await?;
    
    // More successful tasks (that might get cancelled)
    for i in 3..6 {
        workers
            .add(move |cancel| async move {
                println!("  Late task {} started", i);
                
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        println!("  Late task {} completed (unlikely)", i);
                        Ok(())
                    }
                    _ = cancel.cancelled() => {
                        println!("  Late task {} cancelled", i);
                        Ok(())
                    }
                }
            })
            .await?;
    }
    
    let result = workers.wait().await;
    
    match result {
        Ok(_) => println!("  Unexpected success"),
        Err(e) => println!("  Error propagated: {}", e),
    }
    
    println!();
    Ok(())
}

async fn example_all_succeed() -> anyhow::Result<()> {
    println!("3. All Tasks Succeed");
    println!("--------------------");
    
    let workers = Workers::new();
    
    // All tasks complete successfully
    for i in 0..5 {
        workers
            .add(move |_| async move {
                println!("  Task {} processing...", i);
                tokio::time::sleep(Duration::from_millis(100 * (i as u64 + 1))).await;
                println!("  Task {} done", i);
                Ok(())
            })
            .await?;
    }
    
    // Should complete without error
    workers.wait().await?;
    println!("  All tasks completed successfully!");
    
    println!();
    Ok(())
}