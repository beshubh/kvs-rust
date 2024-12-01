use crate::thread_pool::ThreadPool;

pub struct RayonThreadPool {
    pool: rayon::ThreadPool,
}

impl ThreadPool for RayonThreadPool {
    fn new(threads: u32) -> super::Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool {
            pool: rayon::ThreadPoolBuilder::new()
                .num_threads(threads as usize)
                .build()
                .unwrap(),
        })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.pool.spawn(job);
    }
}
