use std::{future::Future, sync::Arc};

use tokio::{sync::Mutex, task::JoinHandle};
use tokio_util::sync::CancellationToken;

pub mod extensions {
    use crate::Workers;

    pub trait WithSysLimitCpus {
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
    pub fn new() -> Self {
        let once = tokio::sync::oneshot::channel();

        Self {
            once: Arc::new(Mutex::new((Some(once.0), once.1))),
            limit: WorkerLimit::default(),
            cancellation: CancellationToken::default(),
            handles: Arc::default(),
        }
    }

    /// respects an external cancellation token, it is undefined behavior to use this with with_cancel_task, we will always only cancel a child token, i.e. never the external token
    pub fn with_cancel(&mut self, cancel: &CancellationToken) -> &mut Self {
        self.cancellation = cancel.child_token();
        self
    }

    /// with_cancel and with_cancel_task used together is considered undefined behavior, as we will cancel the external cancellation token on cancel_task completion
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

    /// with_limit can be dangerous if used with an external cancel, because we still queue work after the cancel, it doesn't guarantee that everyone respects said channel, and closes down in a timely manner. Work will still be queued after the cancel, it is up to the provided worker function to respect when cancel is called
    pub fn with_limit(&mut self, limit: usize) -> &mut Self {
        let (tx, rx) = tokio::sync::mpsc::channel(limit);

        self.limit = WorkerLimit::Amount {
            queue: tx,
            done: Arc::new(Mutex::new(rx)),
        };
        self
    }

    /// Add
    /// Note: Add is immediate, this means your future will be polled immediately, no matter if wait has been called or not. Wait is just for waiting for completion, as well as receiving errors
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
