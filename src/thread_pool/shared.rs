use crate::Result;
use std::sync::Arc;
use std::sync::Mutex;

use crate::thread_pool::{ThreadPool, ThreadPoolMessage};

pub struct SharedQueueThreadPool {
    work_channel: std::sync::mpsc::Sender<ThreadPoolMessage>,
    _workers: Vec<std::thread::JoinHandle<()>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(max_workers: u32) -> Result<Self> {
        let mut threads = vec![];
        let (tx, rx) = std::sync::mpsc::channel::<ThreadPoolMessage>();
        let rx = Arc::new(Mutex::new(rx));

        for _ in 0..max_workers {
            let rx = rx.clone();
            let thread = std::thread::spawn(move || loop {
                let job = {
                    let receiver = rx.lock().unwrap();
                    match receiver.recv() {
                        Ok(job) => job,
                        Err(_) => break,
                    }
                };
                // FIXME: what if the job panics?
                match job {
                    ThreadPoolMessage::RunJob(job) => job(),
                    // FIXME: should we just break? I think something else should be done or I don't know will have to research.
                    ThreadPoolMessage::Shutdown => break,
                }
            });
            threads.push(thread);
        }
        Ok(SharedQueueThreadPool {
            work_channel: tx,
            _workers: threads,
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        let job = Box::new(job);
        println!("I am here, spawned.");
        self.work_channel
            .send(ThreadPoolMessage::RunJob(job))
            .unwrap();
    }

    fn shutdown(&self) {
        for _ in &self._workers {
            self.work_channel.send(ThreadPoolMessage::Shutdown).unwrap();
        }
    }
}
