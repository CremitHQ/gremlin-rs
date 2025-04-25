use std::sync::{atomic::Ordering, Arc};

use tokio::sync::SemaphorePermit;

use super::{manager::Manager, PoolInner};

pub struct ConnectionPoolGuard<M: Manager> {
    pub pool: Arc<PoolInner<M>>,
    cancelled: bool,
}

impl<M: Manager> ConnectionPoolGuard<M> {
    pub fn new(pool: Arc<PoolInner<M>>) -> Self {
        Self {
            pool,
            cancelled: false,
        }
    }

    pub fn from_permit(pool: Arc<PoolInner<M>>, permit: SemaphorePermit<'_>) -> Self {
        permit.forget();
        Self::new(pool)
    }

    pub fn release_without_resizing(self) {
        self.pool.semaphore.add_permits(1);
        self.cancel();
    }

    pub fn cancel(mut self) {
        self.cancelled = true;
    }
}

impl<M: Manager> Drop for ConnectionPoolGuard<M> {
    fn drop(&mut self) {
        if !self.cancelled {
            self.pool.state.current_size.fetch_sub(1, Ordering::AcqRel);
            self.pool.semaphore.add_permits(1);
        }
    }
}
