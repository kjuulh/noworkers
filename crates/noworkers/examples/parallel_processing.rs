//! Parallel data processing pipeline using noworkers.
//!
//! This example shows how to process large datasets in parallel
//! with controlled concurrency and progress tracking.

use noworkers::Workers;
use noworkers::extensions::WithSysLimitCpus;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Parallel Processing Pipeline Example");
    println!("====================================\n");

    // Simulate a large dataset
    let dataset: Vec<i32> = (1..=100).collect();
    let total_items = dataset.len();

    println!("Processing {} items in parallel...\n", total_items);

    // Example 1: Simple parallel map
    example_parallel_map(dataset.clone()).await?;

    // Example 2: Pipeline with multiple stages
    example_pipeline(dataset.clone()).await?;

    // Example 3: Batch processing
    example_batch_processing(dataset).await?;

    Ok(())
}

async fn example_parallel_map(dataset: Vec<i32>) -> anyhow::Result<()> {
    println!("1. Simple Parallel Map");
    println!("----------------------");

    let mut workers = Workers::new();

    // Use system CPU count for optimal parallelism
    workers.with_limit_to_system_cpus();

    let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let processed = Arc::new(AtomicUsize::new(0));
    let total = dataset.len();

    let start = Instant::now();

    for item in dataset {
        let results = results.clone();
        let processed = processed.clone();

        workers
            .add(move |_cancel| async move {
                // Simulate CPU-intensive processing
                let result = expensive_computation(item).await?;

                // Store result
                let mut res = results.lock().await;
                res.push(result);

                // Update progress
                let count = processed.fetch_add(1, Ordering::SeqCst) + 1;
                if count.is_multiple_of(10) {
                    println!("  Processed {}/{} items", count, total);
                }

                Ok(())
            })
            .await?;
    }

    workers.wait().await?;

    let results = results.lock().await;
    let sum: i32 = results.iter().sum();

    println!("  Completed in {}ms", start.elapsed().as_millis());
    println!("  Sum of results: {}\n", sum);

    Ok(())
}

async fn example_pipeline(dataset: Vec<i32>) -> anyhow::Result<()> {
    println!("2. Multi-Stage Pipeline");
    println!("-----------------------");

    // Stage 1: Transform
    let stage1_output = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut stage1 = Workers::new();
    stage1.with_limit(4);

    println!("  Stage 1: Transforming data...");
    for item in dataset {
        let output = stage1_output.clone();

        stage1
            .add(move |_| async move {
                let transformed = item * 2;
                tokio::time::sleep(Duration::from_millis(10)).await;

                let mut out = output.lock().await;
                out.push(transformed);

                Ok(())
            })
            .await?;
    }

    stage1.wait().await?;

    // Stage 2: Filter and aggregate
    let stage2_output = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    let mut stage2 = Workers::new();
    stage2.with_limit(2);

    println!("  Stage 2: Filtering and aggregating...");
    let stage1_data = stage1_output.lock().await.clone();

    for chunk in stage1_data.chunks(10) {
        let chunk = chunk.to_vec();
        let output = stage2_output.clone();

        stage2
            .add(move |_| async move {
                // Filter even numbers and sum
                let filtered_sum: i32 = chunk.iter().filter(|&&x| x % 2 == 0).sum();

                tokio::time::sleep(Duration::from_millis(20)).await;

                let mut out = output.lock().await;
                out.push(filtered_sum);

                Ok(())
            })
            .await?;
    }

    stage2.wait().await?;

    let final_results = stage2_output.lock().await;
    let total: i32 = final_results.iter().sum();

    println!("  Pipeline result: {}\n", total);

    Ok(())
}

async fn example_batch_processing(dataset: Vec<i32>) -> anyhow::Result<()> {
    println!("3. Batch Processing");
    println!("-------------------");

    let batch_size = 20;
    let batches: Vec<Vec<i32>> = dataset
        .chunks(batch_size)
        .map(|chunk| chunk.to_vec())
        .collect();

    println!(
        "  Processing {} batches of {} items each",
        batches.len(),
        batch_size
    );

    let mut workers = Workers::new();
    workers.with_limit(3); // Process 3 batches concurrently

    let results = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let start = Instant::now();

    for (batch_idx, batch) in batches.into_iter().enumerate() {
        let results = results.clone();

        workers
            .add(move |cancel| async move {
                println!("  Batch {} started", batch_idx);

                // Process batch with cancellation support
                tokio::select! {
                    batch_result = process_batch(batch, batch_idx) => {
                        match batch_result {
                            Ok(result) => {
                                let mut res = results.lock().await;
                                res.push(result);
                                println!("  Batch {} completed", batch_idx);
                                Ok(())
                            }
                            Err(e) => {
                                println!("  Batch {} failed: {}", batch_idx, e);
                                Err(e)
                            }
                        }
                    }
                    _ = cancel.cancelled() => {
                        println!("  Batch {} cancelled", batch_idx);
                        Ok(())
                    }
                }
            })
            .await?;
    }

    workers.wait().await?;

    let results = results.lock().await;
    let total_processed: usize = results.iter().sum();

    println!(
        "  Processed {} items in {}ms\n",
        total_processed,
        start.elapsed().as_millis()
    );

    Ok(())
}

async fn expensive_computation(n: i32) -> anyhow::Result<i32> {
    // Simulate CPU-intensive work
    tokio::time::sleep(Duration::from_millis(5)).await;

    // Some complex calculation
    let result = (n * n) + (n / 2) - 1;

    Ok(result)
}

async fn process_batch(batch: Vec<i32>, _batch_idx: usize) -> anyhow::Result<usize> {
    // Simulate batch processing
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Process each item in the batch
    let processed_count = batch.len();

    // In a real scenario, you might:
    // - Write to a database
    // - Send to an API
    // - Transform and save to files

    Ok(processed_count)
}
