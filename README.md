# noworkers

A small, ergonomic Rust crate for spawning and supervising groups of asynchronous “workers” on Tokio.  
Manage concurrent tasks with optional limits, cancellation, and first-error propagation.

Inpired by golang (errgroups)

## Features

- **Unlimited or bounded concurrency** via `with_limit(usize)`.  
- **Cancellation support** with [`tokio_util::sync::CancellationToken`]—either external (`with_cancel`) or task-driven (`with_cancel_task`).
- **First-error wins**: the first worker to fail cancels the rest and reports its error.
- **Graceful shutdown**: `.wait()` awaits all workers, cancels any in-flight tasks, and returns the first error (if any).

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
noworkers = "0.1"
````

Then in your code:

```rust
use noworkers::Workers;
```

## Quick Example

```rust
use noworkers::Workers;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create a worker group with up to 5 concurrent tasks
    let mut workers = Workers::new();

    // Limit amount of concurrent workers
    workers.with_limit(5);

    // Adds cancellation signal
    workers.with_cancel_task(async move {
      // send cancellation to tasks after 60 seconds
      tokio::time::sleep(std::time::Duration::from_secs(60)).await
    });

    // Spawn 10 async jobs
    for i in 0..10 {
        // Work is done immediatley, so this will wait in two batches of 1 seconds each (because of limit)
        workers.add(move |cancel_token| async move {
            // optional tokio::select, if you use cancellation for your tasks, if not just do your work
            tokio::select! {
                // Do work, in this case just sleep
                _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                    println!("Job {i} done");
                    Ok(())
                }
                // If we receive cancel we close
                _ = cancel_token.cancelled() => {
                    println!("Job {i} cancelled");
                    Ok(())
                }
            }
        }).await?;
    }

    // Wait for all to finish or for the first error
    workers.wait().await?;
    Ok(())
}
```

## API Overview

* `Workers::new() -> Workers`
  Create a fresh worker group.

* `with_limit(limit: usize) -> &mut Self`
  Bound in-flight tasks to `limit`, back-pressuring `.add()` calls when full.

* `with_cancel(token: &CancellationToken) -> &mut Self`
  Tie this group’s lifetime to an external token.

* `with_cancel_task(fut: impl Future<Output=()>) -> &mut Self`
  Spawn a task that, when it completes, cancels the group.

* `add<F, Fut>(&self, f: F) -> anyhow::Result<()>`
  Spawn a new worker. `f` is a closure taking a child `CancellationToken` and returning `Future<Output=anyhow::Result<()>>`.

* `wait(self) -> anyhow::Result<()>`
  Await all workers. Returns the first error (if any), after cancelling in-flight workers.

## Error Handling

* The **first** worker to return `Err(_)` wins: its error is sent on a oneshot and all others are cancelled.
* Subsequent errors are ignored.

## License

Dual-licensed under **MIT** or **Apache-2.0**.
See [LICENSE-MIT](LICENSE-MIT) and [LICENSE-APACHE](LICENSE-APACHE) for details.

## Contribute

Simply create an issue here or pr https://github.com/kjuulh/noworkers.git, development happens publicly at: https://git.front.kjuulh.io/kjuulh/noworkers.


