//! File processing example using noworkers.
//!
//! This example demonstrates processing multiple files concurrently,
//! such as reading, transforming, and writing files in parallel.

use noworkers::Workers;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("File Processing Example");
    println!("======================\n");

    // Simulate a list of files to process
    let files = vec![
        "data/input1.txt",
        "data/input2.txt",
        "data/input3.txt",
        "data/input4.txt",
        "data/input5.txt",
        "data/input6.txt",
        "data/input7.txt",
        "data/input8.txt",
    ];

    println!("Processing {} files concurrently...\n", files.len());

    let mut workers = Workers::new();
    
    // Limit concurrent file operations to avoid file descriptor exhaustion
    workers.with_limit(4);
    
    // Track processing statistics
    let stats = Arc::new(tokio::sync::Mutex::new(ProcessingStats::default()));
    let start = Instant::now();

    for file_path in files {
        let stats = stats.clone();
        let path = file_path.to_string();
        
        workers
            .add(move |cancel| async move {
                tokio::select! {
                    result = process_file(&path) => {
                        let mut s = stats.lock().await;
                        
                        match result {
                            Ok(bytes_processed) => {
                                println!("✓ Processed {} ({} bytes)", path, bytes_processed);
                                s.files_processed += 1;
                                s.bytes_processed += bytes_processed;
                            }
                            Err(e) => {
                                println!("✗ Failed to process {}: {}", path, e);
                                s.files_failed += 1;
                            }
                        }
                        
                        Ok(())
                    }
                    _ = cancel.cancelled() => {
                        println!("⚠ Cancelled processing of {}", path);
                        Ok(())
                    }
                }
            })
            .await?;
    }

    // Wait for all file processing to complete
    workers.wait().await?;
    
    let stats = stats.lock().await;
    let elapsed = start.elapsed();
    
    println!("\n========== Summary ==========");
    println!("Total time: {}ms", elapsed.as_millis());
    println!("Files processed: {}", stats.files_processed);
    println!("Files failed: {}", stats.files_failed);
    println!("Total bytes: {}", stats.bytes_processed);
    println!(
        "Throughput: {:.2} MB/s",
        (stats.bytes_processed as f64 / 1_000_000.0) / elapsed.as_secs_f64()
    );

    Ok(())
}

#[derive(Default)]
struct ProcessingStats {
    files_processed: usize,
    files_failed: usize,
    bytes_processed: usize,
}

/// Simulates processing a file
async fn process_file(file_path: &str) -> anyhow::Result<usize> {
    // Simulate reading file
    let content = simulate_read_file(file_path).await?;
    
    // Simulate processing (e.g., compression, encryption, transformation)
    let processed = simulate_transform(content).await?;
    
    // Simulate writing output
    let output_path = PathBuf::from(file_path)
        .with_extension("processed");
    
    simulate_write_file(&output_path, processed.clone()).await?;
    
    Ok(processed.len())
}

async fn simulate_read_file(path: &str) -> anyhow::Result<Vec<u8>> {
    // Simulate file I/O delay
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Generate simulated file content
    let size = (path.len() * 1000) % 10000 + 1000;
    let content = vec![0u8; size];
    
    Ok(content)
}

async fn simulate_transform(data: Vec<u8>) -> anyhow::Result<Vec<u8>> {
    // Simulate CPU-intensive transformation
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Simulate compression (returns smaller data)
    let compressed_size = data.len() / 2;
    Ok(vec![1u8; compressed_size])
}

async fn simulate_write_file(_path: &PathBuf, data: Vec<u8>) -> anyhow::Result<()> {
    // Simulate file I/O delay
    tokio::time::sleep(Duration::from_millis(30)).await;
    
    // In a real implementation, you would write to disk here
    let _ = data; // Use the data to avoid warning
    
    Ok(())
}