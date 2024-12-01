use crate::Result;

pub trait ThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized;

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static;
}

pub type Job = Box<dyn FnOnce() + Send + 'static>;
pub enum ThreadPoolMessage {
    RunJob(Box<dyn FnOnce() + Send + 'static>),
    Shutdown,
}

mod naive;
mod rayon;
mod shared;

pub use naive::NaiveThreadPool;
pub use rayon::RayonThreadPool;
pub use shared::SharedQueueThreadPool;
