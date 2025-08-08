//! Web scraper example using noworkers.
//!
//! This example demonstrates a real-world use case: scraping multiple
//! URLs concurrently with rate limiting and error handling.

use noworkers::Workers;
use std::time::{Duration, Instant};

/// Simulated web page data
struct WebPage {
    url: String,
    content: String,
    fetch_time: Duration,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    println!("Web Scraper Example");
    println!("==================\n");

    let urls = vec![
        "https://example.com/page1",
        "https://example.com/page2",
        "https://example.com/page3",
        "https://example.com/page4",
        "https://example.com/page5",
        "https://example.com/page6",
        "https://example.com/page7",
        "https://example.com/page8",
        "https://example.com/page9",
        "https://example.com/page10",
    ];

    println!("Scraping {} URLs with rate limiting...\n", urls.len());

    let mut workers = Workers::new();
    
    // Limit concurrent requests to avoid overwhelming the server
    workers.with_limit(3);
    
    // Add timeout for the entire scraping operation
    workers.with_cancel_task(async {
        tokio::time::sleep(Duration::from_secs(10)).await;
        println!("⏰ Timeout reached - cancelling remaining requests");
    });

    let start = Instant::now();
    let results = std::sync::Arc::new(tokio::sync::Mutex::new(Vec::new()));

    for (idx, url) in urls.into_iter().enumerate() {
        let url = url.to_string();
        let results = results.clone();
        
        workers
            .add(move |cancel| async move {
                // Simulate rate limiting with a small delay
                tokio::time::sleep(Duration::from_millis(100)).await;
                
                tokio::select! {
                    result = fetch_page(&url, idx) => {
                        match result {
                            Ok(page) => {
                                println!(
                                    "✓ [{:>3}ms] Fetched {} ({}ms)",
                                    start.elapsed().as_millis(),
                                    page.url,
                                    page.fetch_time.as_millis()
                                );
                                
                                let mut res = results.lock().await;
                                res.push(page);
                                
                                Ok(())
                            }
                            Err(e) => {
                                println!(
                                    "✗ [{:>3}ms] Failed to fetch {}: {}",
                                    start.elapsed().as_millis(),
                                    url,
                                    e
                                );
                                // In a real scraper, you might want to continue
                                // despite individual failures
                                Ok(()) // Continue scraping other pages
                                
                                // Or propagate the error to stop all scraping:
                                // Err(e)
                            }
                        }
                    }
                    _ = cancel.cancelled() => {
                        println!("⚠ Cancelled fetch for {}", url);
                        Ok(())
                    }
                }
            })
            .await?;
    }

    // Wait for all scraping to complete
    let scrape_result = workers.wait().await;
    
    println!("\n========== Results ==========");
    
    match scrape_result {
        Ok(_) => {
            let results = results.lock().await;
            println!("Successfully scraped {} pages", results.len());
            println!("Total time: {}ms", start.elapsed().as_millis());
            
            // Process results
            let total_content_size: usize = results.iter()
                .map(|p| p.content.len())
                .sum();
            
            let avg_fetch_time: u128 = if !results.is_empty() {
                results.iter()
                    .map(|p| p.fetch_time.as_millis())
                    .sum::<u128>() / results.len() as u128
            } else {
                0
            };
            
            println!("Total content size: {} bytes", total_content_size);
            println!("Average fetch time: {}ms", avg_fetch_time);
        }
        Err(e) => {
            println!("Scraping failed with error: {}", e);
            let results = results.lock().await;
            println!("Managed to scrape {} pages before failure", results.len());
        }
    }

    Ok(())
}

/// Simulates fetching a web page
async fn fetch_page(url: &str, idx: usize) -> anyhow::Result<WebPage> {
    let fetch_start = Instant::now();
    
    // Simulate network delay (varies by page)
    let delay = 200 + (idx * 50) % 300;
    tokio::time::sleep(Duration::from_millis(delay as u64)).await;
    
    // Simulate occasional failures
    if idx == 7 {
        return Err(anyhow::anyhow!("Connection timeout"));
    }
    
    // Simulate fetching content
    let content = format!(
        "<!DOCTYPE html><html><body><h1>Page {}</h1><p>Content for {}</p></body></html>",
        idx, url
    );
    
    Ok(WebPage {
        url: url.to_string(),
        content,
        fetch_time: fetch_start.elapsed(),
    })
}