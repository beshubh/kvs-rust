use crossbeam;
use std::panic::AssertUnwindSafe;

use crate::thread_pool::{ThreadPool, ThreadPoolMessage};
use crate::Result;

pub struct SharedQueueThreadPool {
    work_channel: crossbeam::channel::Sender<ThreadPoolMessage>,
    workers: Vec<std::thread::JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(max_workers: u32) -> Result<Self> {
        let mut threads = vec![];
        let (tx, rx) = crossbeam::channel::unbounded::<ThreadPoolMessage>();

        for _ in 0..max_workers {
            let rx = rx.clone();
            let thread = std::thread::spawn(move || loop {
                let job = {
                    match rx.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    }
                };
                match job {
                    ThreadPoolMessage::RunJob(job) => {
                        let res = std::panic::catch_unwind(AssertUnwindSafe(job));
                        if let Err(e) = res {
                            eprintln!("Thread panicked: {:?}", e);
                        }
                    }
                    ThreadPoolMessage::Shutdown => break,
                }
            });
            threads.push(thread);
        }
        Ok(SharedQueueThreadPool {
            work_channel: tx,
            workers: threads,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);
        self.work_channel
            .send(ThreadPoolMessage::RunJob(job))
            .unwrap();
    }
}

impl Drop for SharedQueueThreadPool {
    fn drop(&mut self) {
        for _ in &self.workers {
            self.work_channel.send(ThreadPoolMessage::Shutdown).unwrap();
        }

        for worker in self.workers.drain(..) {
            worker.join().unwrap();
        }
    }
}
