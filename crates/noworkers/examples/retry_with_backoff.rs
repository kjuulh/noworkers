//! Retry with exponential backoff example using noworkers.
//!
//! This example shows how to implement retry logic with exponential
//! backoff for handling transient failures in distributed systems.

use noworkers::Workers;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Retry with Exponential Backoff Example");
    println!("======================================\n");

    // Simulate multiple API endpoints to call
    let endpoints = vec![
        "https://api.example.com/endpoint1",
        "https://api.example.com/endpoint2",
        "https://api.example.com/endpoint3",
        "https://api.example.com/endpoint4",
        "https://api.example.com/endpoint5",
    ];

    let mut workers = Workers::new();
    
    // Limit concurrent API calls
    workers.with_limit(3);
    
    // Global request counter for simulation
    let request_counter = Arc::new(AtomicUsize::new(0));

    println!("Making API calls with retry logic...\n");

    for endpoint in endpoints {
        let counter = request_counter.clone();
        let url = endpoint.to_string();
        
        workers
            .add(move |cancel| async move {
                // Retry configuration
                let max_retries = 3;
                let initial_backoff = Duration::from_millis(100);
                let max_backoff = Duration::from_secs(5);
                
                let mut attempt = 0;
                let mut backoff = initial_backoff;
                
                loop {
                    attempt += 1;
                    
                    // Check for cancellation before each attempt
                    if cancel.is_cancelled() {
                        println!("⚠ {} - Cancelled before attempt {}", url, attempt);
                        return Ok(());
                    }
                    
                    println!("→ {} - Attempt {}/{}", url, attempt, max_retries + 1);
                    
                    match make_api_call(&url, &counter).await {
                        Ok(response) => {
                            println!("✓ {} - Success: {}", url, response);
                            return Ok(());
                        }
                        Err(e) if attempt > max_retries => {
                            println!("✗ {} - Failed after {} attempts: {}", url, attempt, e);
                            return Err(anyhow::anyhow!("Max retries exceeded for {}: {}", url, e));
                        }
                        Err(e) => {
                            println!("↻ {} - Attempt {} failed: {}", url, attempt, e);
                            
                            // Check if error is retryable
                            if !is_retryable_error(&e) {
                                println!("✗ {} - Non-retryable error: {}", url, e);
                                return Err(e);
                            }
                            
                            // Wait with exponential backoff
                            println!("  Waiting {}ms before retry...", backoff.as_millis());
                            
                            tokio::select! {
                                _ = tokio::time::sleep(backoff) => {
                                    // Calculate next backoff with jitter
                                    backoff = calculate_next_backoff(backoff, max_backoff);
                                }
                                _ = cancel.cancelled() => {
                                    println!("⚠ {} - Cancelled during backoff", url);
                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            })
            .await?;
    }

    // Wait for all API calls to complete
    let result = workers.wait().await;
    
    println!("\n========== Summary ==========");
    match result {
        Ok(_) => println!("All API calls completed successfully!"),
        Err(e) => println!("Some API calls failed: {}", e),
    }
    
    let total_requests = request_counter.load(Ordering::SeqCst);
    println!("Total requests made: {}", total_requests);

    Ok(())
}

/// Simulates making an API call
async fn make_api_call(url: &str, counter: &Arc<AtomicUsize>) -> anyhow::Result<String> {
    let request_num = counter.fetch_add(1, Ordering::SeqCst);
    
    // Simulate network delay
    tokio::time::sleep(Duration::from_millis(50)).await;
    
    // Simulate different failure scenarios
    match request_num % 7 {
        0 => {
            // Simulate timeout
            Err(anyhow::anyhow!("Request timeout"))
        }
        1 | 2 => {
            // Simulate temporary server error
            Err(anyhow::anyhow!("503 Service Unavailable"))
        }
        3 if url.contains("endpoint3") => {
            // Simulate rate limiting for specific endpoint
            Err(anyhow::anyhow!("429 Too Many Requests"))
        }
        _ => {
            // Success
            Ok(format!("Response from {} (request #{})", url, request_num))
        }
    }
}

/// Determines if an error is retryable
fn is_retryable_error(error: &anyhow::Error) -> bool {
    let error_str = error.to_string();
    
    // List of retryable error patterns
    error_str.contains("timeout") ||
    error_str.contains("503") ||
    error_str.contains("429") ||
    error_str.contains("Service Unavailable") ||
    error_str.contains("Too Many Requests")
}

/// Calculates the next backoff duration with jitter
fn calculate_next_backoff(current: Duration, max: Duration) -> Duration {
    // Double the backoff
    let mut next = current * 2;
    
    // Cap at maximum
    if next > max {
        next = max;
    }
    
    // Add jitter (±10%)
    let jitter_range = next.as_millis() / 10;
    let jitter = (rand::random::<u64>() % (jitter_range as u64 * 2)) as i64 - jitter_range as i64;
    
    let final_millis = (next.as_millis() as i64 + jitter).max(1) as u64;
    Duration::from_millis(final_millis)
}

// Simple random number generator for jitter
mod rand {
    use std::sync::atomic::{AtomicU64, Ordering};
    
    static SEED: AtomicU64 = AtomicU64::new(12345);
    
    pub fn random<T>() -> T
    where
        T: From<u64>,
    {
        // Simple LCG for demonstration
        let old = SEED.fetch_add(1, Ordering::Relaxed);
        let new = (old.wrapping_mul(1103515245).wrapping_add(12345)) >> 16;
        T::from(new)
    }
}