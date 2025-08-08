//! Cancellation patterns with noworkers.
//!
//! This example demonstrates different ways to cancel running tasks:
//! - External cancellation token
//! - Task-driven cancellation (timeout)
//! - Error-triggered cancellation

use noworkers::Workers;
use tokio_util::sync::CancellationToken;
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Cancellation Patterns Example");
    println!("=============================\n");

    // Example 1: External cancellation token
    example_external_cancellation().await?;
    
    // Example 2: Task-driven cancellation (timeout)
    example_timeout_cancellation().await?;
    
    // Example 3: Error-triggered cancellation
    let _ = example_error_cancellation().await;

    Ok(())
}

async fn example_external_cancellation() -> anyhow::Result<()> {
    println!("1. External Cancellation Token");
    println!("------------------------------");
    
    let mut workers = Workers::new();
    let cancel_token = CancellationToken::new();
    
    // Link workers to external cancellation token
    workers.with_cancel(&cancel_token);
    
    // Spawn tasks that respect cancellation
    for i in 0..5 {
        workers
            .add(move |cancel| async move {
                println!("  Task {} started", i);
                
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        println!("  Task {} completed normally", i);
                        Ok(())
                    }
                    _ = cancel.cancelled() => {
                        println!("  Task {} cancelled!", i);
                        Ok(())
                    }
                }
            })
            .await?;
    }
    
    // Cancel after 500ms
    let cancel_clone = cancel_token.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(500)).await;
        println!("  >>> Triggering external cancellation");
        cancel_clone.cancel();
    });
    
    workers.wait().await?;
    println!();
    
    Ok(())
}

async fn example_timeout_cancellation() -> anyhow::Result<()> {
    println!("2. Task-Driven Cancellation (Timeout)");
    println!("-------------------------------------");
    
    let mut workers = Workers::new();
    
    // Set up a timeout task that cancels after 1 second
    workers.with_cancel_task(async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        println!("  >>> Timeout reached, cancelling all tasks");
    });
    
    // Spawn long-running tasks
    for i in 0..3 {
        workers
            .add(move |cancel| async move {
                println!("  Long task {} started", i);
                
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {
                        println!("  Long task {} completed (shouldn't happen)", i);
                        Ok(())
                    }
                    _ = cancel.cancelled() => {
                        println!("  Long task {} cancelled by timeout", i);
                        Ok(())
                    }
                }
            })
            .await?;
    }
    
    workers.wait().await?;
    println!();
    
    Ok(())
}

async fn example_error_cancellation() -> anyhow::Result<()> {
    println!("3. Error-Triggered Cancellation");
    println!("-------------------------------");
    
    let workers = Workers::new();
    
    // Task that will fail and trigger cancellation
    workers
        .add(|_| async {
            println!("  Failing task started");
            tokio::time::sleep(Duration::from_millis(200)).await;
            println!("  >>> Task failing with error!");
            Err(anyhow::anyhow!("Intentional failure"))
        })
        .await?;
    
    // Tasks that will be cancelled due to the error
    for i in 0..3 {
        workers
            .add(move |cancel| async move {
                println!("  Normal task {} started", i);
                
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        println!("  Normal task {} completed (shouldn't happen)", i);
                        Ok(())
                    }
                    _ = cancel.cancelled() => {
                        println!("  Normal task {} cancelled due to error", i);
                        Ok(())
                    }
                }
            })
            .await?;
    }
    
    // This will return the error from the failing task
    let result = workers.wait().await;
    
    match result {
        Ok(_) => println!("  Unexpected success"),
        Err(e) => println!("  Expected error caught: {}", e),
    }
    
    println!();
    Ok(())
}