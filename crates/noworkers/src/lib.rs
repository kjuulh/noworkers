//! # noworkers
//!
//! A small, ergonomic Rust crate for spawning and supervising groups of asynchronous "workers" on Tokio.
//! Manage concurrent tasks with optional limits, cancellation, and first-error propagation.
//!
//! This crate is inspired by Go's [errgroup](https://pkg.go.dev/golang.org/x/sync/errgroup) package,
//! providing similar functionality in an idiomatic Rust way.
//!
//! ## Overview
//!
//! `noworkers` provides a simple way to manage groups of concurrent async tasks with:
//!
//! * **Bounded or unbounded concurrency** - Control how many tasks run simultaneously
//! * **Automatic cancellation** - First error cancels all remaining tasks
//! * **Flexible cancellation strategies** - External tokens or task-driven cancellation
//! * **Zero-cost abstractions** - Minimal overhead over raw tokio tasks
//!
//! ## Quick Start
//!
//! ```rust
//! use noworkers::Workers;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create a new worker group
//!     let mut workers = Workers::new();
//!     
//!     // Limit concurrent tasks to 5
//!     workers.with_limit(5);
//!     
//!     // Spawn 10 tasks
//!     for i in 0..10 {
//!         workers.add(move |_cancel| async move {
//!             println!("Task {i} running");
//!             tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
//!             Ok(())
//!         }).await?;
//!     }
//!     
//!     // Wait for all tasks to complete
//!     workers.wait().await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Core Concepts
//!
//! ### Worker Groups
//!
//! A [`Workers`] instance represents a group of related async tasks that should be
//! managed together. All tasks in a group share:
//!
//! * A common concurrency limit (if set)
//! * A cancellation token hierarchy
//! * First-error propagation semantics
//!
//! ### Error Handling
//!
//! The first task to return an error "wins" - its error is captured and all other
//! tasks are immediately cancelled. This provides fail-fast semantics similar to
//! Go's errgroup.
//!
//! ```rust
//! use noworkers::Workers;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let workers = Workers::new();
//!
//! // This task will fail first
//! workers.add(|_| async {
//!     tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
//!     Err(anyhow::anyhow!("task failed"))
//! }).await?;
//!
//! // These tasks will be cancelled
//! for i in 0..5 {
//!     workers.add(move |cancel| async move {
//!         // Check for cancellation
//!         tokio::select! {
//!             _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
//!                 Ok(())
//!             }
//!             _ = cancel.cancelled() => {
//!                 println!("Task {i} cancelled");
//!                 Ok(())
//!             }
//!         }
//!     }).await?;
//! }
//!
//! // This will return the error from the first task
//! let result = workers.wait().await;
//! assert!(result.is_err());
//! # Ok(())
//! # }
//! ```
//!
//! ### Concurrency Limits
//!
//! You can limit how many tasks run concurrently using [`Workers::with_limit`].
//! When the limit is reached, new tasks will wait for a slot to become available.
//!
//! ```rust
//! use noworkers::Workers;
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Ordering};
//!
//! # async fn example() -> anyhow::Result<()> {
//! let mut workers = Workers::new();
//! workers.with_limit(2); // Only 2 tasks run at once
//!
//! let concurrent_count = Arc::new(AtomicUsize::new(0));
//!
//! for i in 0..10 {
//!     let count = concurrent_count.clone();
//!     workers.add(move |_| async move {
//!         let current = count.fetch_add(1, Ordering::SeqCst) + 1;
//!         assert!(current <= 2, "Too many concurrent tasks!");
//!         
//!         tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
//!         
//!         count.fetch_sub(1, Ordering::SeqCst);
//!         Ok(())
//!     }).await?;
//! }
//!
//! workers.wait().await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Cancellation
//!
//! There are two ways to trigger cancellation:
//!
//! 1. **External cancellation** - Use an existing `CancellationToken`
//! 2. **Task-driven cancellation** - A dedicated task that triggers cancellation when complete
//!
//! ```rust
//! use noworkers::Workers;
//! use tokio_util::sync::CancellationToken;
//!
//! # async fn example() -> anyhow::Result<()> {
//! // External cancellation
//! let mut workers = Workers::new();
//! let cancel = CancellationToken::new();
//! workers.with_cancel(&cancel);
//!
//! // Cancel after 1 second
//! let cancel_clone = cancel.clone();
//! tokio::spawn(async move {
//!     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
//!     cancel_clone.cancel();
//! });
//!
//! // Task-driven cancellation
//! let mut workers2 = Workers::new();
//! workers2.with_cancel_task(async {
//!     tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
//! });
//! # Ok(())
//! # }
//! ```
//!
//! ## Extensions
//!
//! The crate provides extension traits for common patterns:
//!
//! ```rust
//! use noworkers::Workers;
//! use noworkers::extensions::WithSysLimitCpus;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let mut workers = Workers::new();
//! // Automatically limit to number of CPU cores
//! workers.with_limit_to_system_cpus();
//! # Ok(())
//! # }
//! ```
//!
//! ## Examples
//!
//! See the `examples/` directory for more complete examples:
//!
//! * `basic.rs` - Simple task spawning and waiting
//! * `with_limit.rs` - Concurrency limiting
//! * `cancellation.rs` - Cancellation patterns
//! * `error_handling.rs` - Error propagation
//! * `web_scraper.rs` - Real-world web scraping example
//! * `parallel_processing.rs` - Data processing pipeline

use std::{future::Future, sync::Arc};

use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

/// Extension traits for common patterns.
///
/// This module provides convenient extension methods for [`Workers`] through traits.
pub mod extensions {
    use crate::Workers;

    /// Extension trait for CPU-based concurrency limits.
    ///
    /// This trait provides a convenient way to limit worker concurrency based on
    /// the system's available CPU cores.
    pub trait WithSysLimitCpus {
        /// Limits the worker group to the number of available CPU cores.
        ///
        /// This is a convenience method that automatically detects the number of
        /// logical CPU cores available and sets that as the concurrency limit.
        ///
        /// # Example
        ///
        /// ```rust
        /// use noworkers::Workers;
        /// use noworkers::extensions::WithSysLimitCpus;
        ///
        /// # async fn example() -> anyhow::Result<()> {
        /// let mut workers = Workers::new();
        /// // Automatically limit to system CPU count (e.g., 8 on an 8-core machine)
        /// workers.with_limit_to_system_cpus();
        ///
        /// // Spawn many tasks, but only CPU count will run concurrently
        /// for i in 0..100 {
        ///     workers.add(move |_| async move {
        ///         // CPU-bound work here
        ///         Ok(())
        ///     }).await?;
        /// }
        /// # Ok(())
        /// # }
        /// ```
        ///
        /// # Panics
        ///
        /// Panics if the system's available parallelism cannot be determined.
        fn with_limit_to_system_cpus(&mut self) -> &mut Self;
    }

    impl WithSysLimitCpus for Workers {
        fn with_limit_to_system_cpus(&mut self) -> &mut Self {
            self.with_limit(
                std::thread::available_parallelism()
                    .expect("to be able to get system cpu info")
                    .into(),
            )
        }
    }
}

type ErrChan = Arc<
    Mutex<(
        Option<tokio::sync::oneshot::Sender<anyhow::Error>>,
        tokio::sync::oneshot::Receiver<anyhow::Error>,
    )>,
>;

type JoinHandles = Arc<Mutex<Vec<JoinHandle<()>>>>;

/// A group of supervised async workers with optional concurrency limits.
///
/// `Workers` manages a collection of async tasks that run concurrently on Tokio.
///
/// # Examples
///
/// Basic usage:
/// ```rust
/// use noworkers::Workers;
///
/// # async fn example() -> anyhow::Result<()> {
/// let workers = Workers::new();
///
/// // Spawn some work
/// workers.add(|_| async {
///     println!("Doing work!");
///     Ok(())
/// }).await?;
///
/// // Wait for completion
/// workers.wait().await?;
/// # Ok(())
/// # }
/// ```
///
/// With concurrency limit:
/// ```rust
/// use noworkers::Workers;
///
/// # async fn example() -> anyhow::Result<()> {
/// let mut workers = Workers::new();
/// workers.with_limit(3); // Max 3 concurrent tasks
///
/// for i in 0..10 {
///     workers.add(move |_| async move {
///         println!("Task {i}");
///         Ok(())
///     }).await?;
/// }
///
/// workers.wait().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Workers {
    limit: WorkerLimit,

    once: ErrChan,
    cancellation: CancellationToken,
    handles: JoinHandles,
}

impl Default for Workers {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Default, Clone)]
enum WorkerLimit {
    #[default]
    NoLimit,
    Amount {
        queue: tokio::sync::mpsc::Sender<()>,
        done: Arc<Mutex<tokio::sync::mpsc::Receiver<()>>>,
    },
}

impl WorkerLimit {
    pub async fn queue_worker(&self) -> WorkerGuard {
        match self {
            WorkerLimit::NoLimit => {}
            WorkerLimit::Amount { queue, .. } => {
                // Queue work, if the channel is limited, we will block until there is enough room
                queue
                    .send(())
                    .await
                    .expect("tried to queue work on a closed worker channel");
            }
        }

        WorkerGuard {
            limit: self.clone(),
        }
    }
}

/// Guard that tracks active worker slots for concurrency limiting.
///
/// This guard is automatically created when a worker starts and dropped when it completes.
/// When dropped, it signals that a worker slot is available for the next queued task.
///
/// This type is not directly constructible by users.
pub struct WorkerGuard {
    limit: WorkerLimit,
}

impl Drop for WorkerGuard {
    fn drop(&mut self) {
        match &self.limit {
            WorkerLimit::NoLimit => { /* no limit on dequeue */ }
            WorkerLimit::Amount { done, .. } => {
                let done = done.clone();
                tokio::spawn(async move {
                    let mut done = done.lock().await;

                    // dequeue an item, leave room for the next
                    done.recv().await
                });
            }
        }
    }
}

impl Workers {
    /// Creates a new worker group with no concurrency limit.
    ///
    /// The returned `Workers` instance can spawn unlimited concurrent tasks.
    /// Use [`with_limit`](Self::with_limit) to add a concurrency limit.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use noworkers::Workers;
    ///
    /// let workers = Workers::new();
    /// // Can spawn unlimited concurrent tasks
    /// ```
    pub fn new() -> Self {
        let once = tokio::sync::oneshot::channel();

        Self {
            once: Arc::new(Mutex::new((Some(once.0), once.1))),
            limit: WorkerLimit::default(),
            cancellation: CancellationToken::default(),
            handles: Arc::default(),
        }
    }

    /// Associates an external cancellation token with this worker group.
    ///
    /// When the provided token is cancelled, all workers in this group will receive
    /// cancellation signals. The group creates a child token from the provided token,
    /// so cancelling the group won't affect the parent token.
    ///
    /// # Important
    ///
    /// Do not use both `with_cancel` and [`with_cancel_task`](Self::with_cancel_task) on the same
    /// worker group. This is undefined behavior.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use noworkers::Workers;
    /// use tokio_util::sync::CancellationToken;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let mut workers = Workers::new();
    /// let cancel = CancellationToken::new();
    ///
    /// // Link workers to external cancellation
    /// workers.with_cancel(&cancel);
    ///
    /// // Spawn workers that respect cancellation
    /// workers.add(|cancel| async move {
    ///     tokio::select! {
    ///         _ = tokio::time::sleep(tokio::time::Duration::from_secs(10)) => {
    ///             Ok(())
    ///         }
    ///         _ = cancel.cancelled() => {
    ///             println!("Cancelled!");
    ///             Ok(())
    ///         }
    ///     }
    /// }).await?;
    ///
    /// // Cancel from external source
    /// cancel.cancel();
    /// workers.wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_cancel(&mut self, cancel: &CancellationToken) -> &mut Self {
        self.cancellation = cancel.child_token();
        self
    }

    /// Spawns a task that cancels the worker group when it completes.
    ///
    /// This is useful for implementing timeouts or other cancellation conditions.
    /// When the provided future completes, all workers in the group receive
    /// cancellation signals.
    ///
    /// # Important
    ///
    /// Do not use both [`with_cancel`](Self::with_cancel) and `with_cancel_task` on the same
    /// worker group. This is undefined behavior.
    ///
    /// # Examples
    ///
    /// Timeout after 5 seconds:
    /// ```rust
    /// use noworkers::Workers;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let mut workers = Workers::new();
    ///
    /// // Cancel all workers after 5 seconds
    /// workers.with_cancel_task(async {
    ///     tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
    /// });
    ///
    /// // Spawn long-running workers
    /// for i in 0..10 {
    ///     workers.add(move |cancel| async move {
    ///         tokio::select! {
    ///             _ = tokio::time::sleep(tokio::time::Duration::from_secs(60)) => {
    ///                 println!("Task {i} completed");
    ///                 Ok(())
    ///             }
    ///             _ = cancel.cancelled() => {
    ///                 println!("Task {i} cancelled by timeout");
    ///                 Ok(())
    ///             }
    ///         }
    ///     }).await?;
    /// }
    ///
    /// workers.wait().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_cancel_task<T>(&mut self, f: T) -> &mut Self
    where
        T: Future<Output = ()> + Send + 'static,
    {
        let cancel = self.cancellation.clone();

        tokio::spawn(async move {
            f.await;
            cancel.cancel();
        });
        self
    }

    /// Sets a concurrency limit for this worker group.
    ///
    /// When the limit is reached, calls to [`add`](Self::add) will wait until a slot
    /// becomes available. This provides backpressure and prevents unbounded task spawning.
    ///
    /// # Important
    ///
    /// When using with cancellation, workers should check their cancellation tokens.
    /// Even after cancellation, queued tasks will still be scheduled (though they should
    /// exit quickly if they respect cancellation).
    ///
    /// # Examples
    ///
    /// ```rust
    /// use noworkers::Workers;
    /// use std::time::Instant;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let mut workers = Workers::new();
    /// workers.with_limit(2); // Only 2 tasks run concurrently
    ///
    /// let start = Instant::now();
    ///
    /// // Spawn 4 tasks that each take 100ms
    /// for i in 0..4 {
    ///     workers.add(move |_| async move {
    ///         println!("Task {i} started at {:?}", start.elapsed());
    ///         tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    ///         Ok(())
    ///     }).await?;
    /// }
    ///
    /// workers.wait().await?;
    /// // Total time will be ~200ms (2 batches of 2 tasks)
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        let (tx, rx) = tokio::sync::mpsc::channel(limit);

        self.limit = WorkerLimit::Amount {
            queue: tx,
            done: Arc::new(Mutex::new(rx)),
        };
        self
    }

    /// Spawns a new worker task in this group.
    ///
    /// The provided closure receives a [`CancellationToken`] that will be triggered if:
    /// - Another worker returns an error
    /// - The group's cancellation token is triggered
    /// - [`wait`](Self::wait) completes
    ///
    /// # Immediate Execution
    ///
    /// The task starts executing immediately when `add` is called, not when `wait` is called.
    /// The `wait` method is only for awaiting completion and collecting errors.
    ///
    /// # Backpressure
    ///
    /// If a concurrency limit is set via [`with_limit`](Self::with_limit), this method
    /// will wait until a slot is available before spawning the task.
    ///
    /// # Error Propagation
    ///
    /// If the worker returns an error:
    /// 1. All other workers receive cancellation signals
    /// 2. The error is stored to be returned by [`wait`](Self::wait)
    /// 3. Subsequent errors are ignored (first error wins)
    ///
    /// # Examples
    ///
    /// Basic task:
    /// ```rust
    /// use noworkers::Workers;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let workers = Workers::new();
    ///
    /// workers.add(|_cancel| async {
    ///     println!("Doing work");
    ///     Ok(())
    /// }).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Task with cancellation handling:
    /// ```rust
    /// use noworkers::Workers;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let workers = Workers::new();
    ///
    /// workers.add(|cancel| async move {
    ///     tokio::select! {
    ///         result = do_work() => {
    ///             result
    ///         }
    ///         _ = cancel.cancelled() => {
    ///             println!("Cancelled");
    ///             Ok(())
    ///         }
    ///     }
    /// }).await?;
    ///
    /// # async fn do_work() -> anyhow::Result<()> { Ok(()) }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add<T, TFut>(&self, f: T) -> anyhow::Result<()>
    where
        T: FnOnce(CancellationToken) -> TFut + Send + 'static,
        TFut: Future<Output = anyhow::Result<()>> + Send + 'static,
    {
        let s = self.clone();

        let handle = tokio::spawn(async move {
            let queue_guard = s.limit.queue_worker().await;
            if let Err(err) = f(s.cancellation.child_token()).await {
                if let Ok(mut erronce) = s.once.try_lock() {
                    if let Some(tx) = erronce.0.take() {
                        // oneshot works as a once channel, we don't care about subsequent errors
                        let _ = tx.send(err);
                        s.cancellation.cancel();
                    }
                }
            }

            // ensure it survives the scope, it isn't required that we call it here though
            drop(queue_guard)
        });

        {
            let mut handles = self.handles.lock().await;
            handles.push(handle);
        }

        Ok(())
    }

    /// Waits for all workers to complete and returns the first error (if any).
    ///
    /// This method:
    /// 1. Waits for all spawned tasks to complete
    /// 2. Cancels any remaining tasks if one fails
    /// 3. Returns the first error encountered, or `Ok(())` if all succeed
    ///
    /// # Consumption
    ///
    /// This method consumes the `Workers` instance. After calling `wait`,
    /// you cannot add more tasks to this group.
    ///
    /// # Examples
    ///
    /// Success case:
    /// ```rust
    /// use noworkers::Workers;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let workers = Workers::new();
    ///
    /// for i in 0..5 {
    ///     workers.add(move |_| async move {
    ///         println!("Task {i}");
    ///         Ok(())
    ///     }).await?;
    /// }
    ///
    /// // Blocks until all complete
    /// workers.wait().await?;
    /// println!("All done!");
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Error case:
    /// ```rust
    /// use noworkers::Workers;
    ///
    /// # async fn example() {
    /// let workers = Workers::new();
    ///
    /// workers.add(|_| async {
    ///     Err(anyhow::anyhow!("Task failed"))
    /// }).await.unwrap();
    ///
    /// workers.add(|_| async {
    ///     Ok(()) // This might be cancelled
    /// }).await.unwrap();
    ///
    /// // Returns the error from the first task
    /// let result = workers.wait().await;
    /// assert!(result.is_err());
    /// # }
    /// ```
    pub async fn wait(self) -> anyhow::Result<()> {
        {
            let mut handles = self.handles.lock().await;

            for handle in handles.iter_mut() {
                handle.await?;
            }
        }

        self.cancellation.cancel();

        {
            let mut once = self.once.lock().await;
            if let Ok(e) = once.1.try_recv() {
                anyhow::bail!("{}", e)
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::time::{Duration, SystemTime};

    use crate::*;

    #[tokio::test]
    async fn test_can_start_worker_group() -> anyhow::Result<()> {
        let workers = Workers::new();

        workers
            .add(|_cancel| async move {
                println!("starting worker");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                println!("worker finished");
                Ok(())
            })
            .await?;

        workers.wait().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_worker_can_return_error() -> anyhow::Result<()> {
        let workers = Workers::new();

        workers
            .add(|_cancel| async move {
                println!("starting worker");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                println!("worker finished");
                anyhow::bail!("worker should fail")
            })
            .await?;

        workers.wait().await.expect_err("Error: worker should fail");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_waits() -> anyhow::Result<()> {
        let workers = Workers::new();

        workers
            .add(|_cancel| async move {
                println!("starting worker");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                println!("worker finished");
                Ok(())
            })
            .await?;

        let (called_tx, called_rx) = tokio::sync::oneshot::channel();

        workers
            .add(|_cancel| async move {
                println!("starting worker (immediate)");
                println!("worker finished (immediate)");
                println!("worker send finish (immediate)");
                let _ = called_tx.send(());

                Ok(())
            })
            .await?;

        workers.wait().await?;

        called_rx.await.expect("to receive called");

        Ok(())
    }

    #[tokio::test]
    async fn test_group_waits_are_cancelled_on_error() -> anyhow::Result<()> {
        let workers = Workers::new();

        workers
            .add(|_cancel| async move {
                println!("starting worker");
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;

                println!("worker finished");
                Err(anyhow::anyhow!("expected error"))
            })
            .await?;

        for i in 0..10 {
            workers
                .add(move |cancel| async move {
                    println!("starting worker (waits) id: {i}");

                    cancel.cancelled().await;

                    println!("worker finished (waits) id: {i}");

                    Ok(())
                })
                .await?;
        }

        workers.wait().await.expect_err("expected error");

        Ok(())
    }

    #[tokio::test]
    async fn test_are_concurrent() -> anyhow::Result<()> {
        let workers = Workers::new();

        let (initial_tx, initial_rx) = tokio::sync::oneshot::channel();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        let (ok_tx, ok_rx) = tokio::sync::oneshot::channel();

        // Having two workers swap between tasks should illustrate that we're concurrent, and not just waiting in line for each component

        workers
            .add(move |_cancel| async move {
                println!("starting worker b");

                println!("waiting for initial request");
                initial_rx.await?;
                println!("sending reply");
                reply_tx.send(()).unwrap();

                println!("worker finished");
                Err(anyhow::anyhow!("expected error"))
            })
            .await?;

        workers
            .add(move |_cancel| async move {
                println!("starting worker a");

                println!("sending initial");
                initial_tx.send(()).unwrap();
                println!("received reply");
                reply_rx.await?;
                println!("sending ok");
                ok_tx.send(()).unwrap();

                println!("worker finished");
                Err(anyhow::anyhow!("expected error"))
            })
            .await?;

        workers.wait().await.expect_err("expected error");

        ok_rx.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_multiple_errors() -> anyhow::Result<()> {
        let workers = Workers::new();

        let now = std::time::SystemTime::now();
        for _ in 0..100 {
            workers
                .add(move |_cancel| async move {
                    println!("starting worker a");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                    println!("worker finished");
                    Err(anyhow::anyhow!("unexpected"))
                })
                .await?;
        }
        workers
            .add(move |_cancel| async move {
                println!("starting worker b");

                println!("worker finished");
                Err(anyhow::anyhow!("expected error"))
            })
            .await?;

        let err = workers.wait().await.unwrap_err();
        if !err.to_string().contains("expected error") {
            panic!("'{err}' error it should not have been this one");
        }

        let after = now.elapsed()?;
        println!(
            "it took {} seconds and {} total ms, {} total nanos to add 100 workers",
            after.as_secs(),
            after.as_millis(),
            after.as_nanos()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_wait_is_optional() -> anyhow::Result<()> {
        let workers = Workers::new();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        workers
            .add(move |_cancel| async move {
                println!("starting worker a");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                done_tx.send(()).unwrap();

                println!("worker finished");
                Ok(())
            })
            .await?;

        done_rx.await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_wait_is_optional_err() -> anyhow::Result<()> {
        let workers = Workers::new();

        let (done_tx, done_rx) = tokio::sync::oneshot::channel();

        workers
            .add(move |_cancel| async move {
                println!("starting worker a");
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                done_tx.send(()).unwrap();

                println!("worker finished");
                anyhow::bail!("expected failure");
            })
            .await?;

        done_rx.await?;

        workers
            .wait()
            .await
            .expect_err("there should be an error here");

        Ok(())
    }

    #[tokio::test]
    async fn test_wait_called_twice() -> anyhow::Result<()> {
        let workers = Workers::new();

        workers
            .add(move |_cancel| async move {
                println!("starting worker");

                println!("worker finished");
                Ok(())
            })
            .await?;

        workers.wait().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_limits_work() -> anyhow::Result<()> {
        let mut workers = Workers::new();

        // sets how many tasks we can queue at once
        workers.with_limit(40);

        let start = SystemTime::now();

        for i in 0..1000 {
            workers
                .add(move |_cancel| async move {
                    println!(
                        "starting worker: {i}: {} millis",
                        start.elapsed().unwrap().as_millis()
                    );

                    tokio::time::sleep(Duration::from_millis(10)).await;

                    // println!(
                    //     "worker finished: {i}: {} millis",
                    //     start.elapsed().unwrap().as_millis()
                    // );
                    Ok(())
                })
                .await?;
        }

        workers.wait().await?;

        assert!(start.elapsed().unwrap() > Duration::from_millis(100));

        Ok(())
    }

    #[tokio::test]
    async fn test_blocking() -> anyhow::Result<()> {
        let mut workers = Workers::new();

        // sets how many tasks we can queue at once
        workers.with_limit(40);

        let start = SystemTime::now();

        let lock = Arc::new(Mutex::new(()));

        for i in 0..1000 {
            let lock = lock.clone();
            workers
                .add(move |_cancel| async move {
                    // println!(
                    //     "starting worker: {i}: {} millis",
                    //     start.elapsed().unwrap().as_millis()
                    // );

                    let guard = lock.lock().await;

                    println!(
                        "worker finished: {i}: {} millis",
                        start.elapsed().unwrap().as_millis()
                    );

                    drop(guard);

                    Ok(())
                })
                .await?;
        }

        workers.wait().await?;

        // we compile, and there is no deadlock

        Ok(())
    }

    #[tokio::test]
    async fn test_cancellation() -> anyhow::Result<()> {
        let mut workers = Workers::new();

        let cancel = CancellationToken::new();

        workers.with_cancel(&cancel.child_token());

        // sets how many tasks we can queue at once
        workers.with_limit(5);

        let start = SystemTime::now();

        for i in 0..10 {
            workers
                .add(move |cancel| async move {
                    println!("worker: {i} waiting for cancellation");

                    cancel.cancelled().await;

                    println!("worker: {i} received cancellation");

                    Ok(())
                })
                .await?;
        }

        tokio::spawn(async move {
            println!("queuing cancellation (waiting 300ms)");
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            println!("sending external cancellation");
            cancel.cancel();
            println!("cancellation sent");
        });

        // No deadlocks
        workers.wait().await?;

        assert!(start.elapsed().unwrap() >= Duration::from_millis(300));

        Ok(())
    }

    #[tokio::test]
    async fn test_cancellation_task() -> anyhow::Result<()> {
        let mut workers = Workers::new();

        workers.with_cancel_task(async move {
            println!("queuing cancellation (waiting 300ms)");
            tokio::time::sleep(Duration::from_millis(300)).await;
            println!("cancellation sent");
        });

        let start = SystemTime::now();

        for i in 0..10 {
            workers
                .add(move |cancel| async move {
                    println!("worker: {i} waiting for cancellation");

                    cancel.cancelled().await;

                    println!("worker: {i} received cancellation");

                    Ok(())
                })
                .await?;
        }

        // No deadlocks
        workers.wait().await?;

        assert!(start.elapsed().unwrap() >= Duration::from_millis(300));

        Ok(())
    }
}
