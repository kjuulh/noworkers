#![feature(random)]
use std::{random, sync::atomic::AtomicUsize};

use tokio_util::sync::CancellationToken;

static SLOW_IN_PROGRESS: AtomicUsize = AtomicUsize::new(0);
static FAST_IN_PROGRESS: AtomicUsize = AtomicUsize::new(0);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tokio::spawn(async move {
        loop {
            println!(
                "slow: {}, fast: {}",
                SLOW_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed),
                FAST_IN_PROGRESS.load(std::sync::atomic::Ordering::Relaxed)
            );
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    });
    let mut workers = noworkers::Workers::new();

    workers.with_limit(30);

    for _ in 0..1000 {
        let range: u16 = random::random(..);
        if range < (u16::MAX / 4) {
            workers.add(slow).await?;
            continue;
        }

        workers.add(fast).await?;
    }

    workers.wait().await?;

    Ok(())
}

async fn fast(_cancel: CancellationToken) -> anyhow::Result<()> {
    FAST_IN_PROGRESS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    // println!("{}: running fast", now());
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    FAST_IN_PROGRESS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}
async fn slow(_cancel: CancellationToken) -> anyhow::Result<()> {
    SLOW_IN_PROGRESS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    // println!("{}: running slow", now());
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    // println!("{}: completed slow", now());
    SLOW_IN_PROGRESS.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    Ok(())
}

fn now() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis()
}
