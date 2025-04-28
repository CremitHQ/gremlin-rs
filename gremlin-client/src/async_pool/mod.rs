pub mod config;
pub mod connection;
pub mod error;
pub mod guard;
pub mod manager;
pub mod state;

use std::{
    marker::PhantomData,
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use config::PoolConfig;
use connection::{ConnectionGuard, IdleConnection, LiveConnection, PoolConnection};
use crossbeam_queue::ArrayQueue;
use error::ConnectionError;
use guard::ConnectionPoolGuard;
use manager::Manager;
use state::{ConnectionState, PoolState};
use tokio::sync::{Semaphore, SemaphorePermit};

pub struct Pool<M: Manager> {
    inner: Arc<PoolInner<M>>,
}

impl<M: Manager> Clone for Pool<M> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<M: Manager> Pool<M> {
    pub async fn connect(manager: M, pool_config: PoolConfig) -> Result<Self, ConnectionError> {
        let inner = PoolInner::new_arc(manager, pool_config);
        inner.try_min_connections().await?;

        let conn = inner.acquire().await?;
        inner.release(conn);

        Ok(Self { inner })
    }

    pub fn set_connect_option(&self, connect_option: M::ConnectOption) {
        self.inner.manager.set_connect_option(connect_option);
    }

    pub fn get_connect_option(&self) -> M::ConnectOption {
        self.inner.manager.get_connect_option()
    }

    pub async fn get(&self) -> Result<PoolConnection<M>, ConnectionError> {
        let conn = self.inner.acquire().await?;
        Ok(conn.into_pool_connection())
    }

    pub fn builder() -> PoolBuilder<M> {
        PoolBuilder::new()
    }
}

pub struct PoolBuilder<M: Manager> {
    pool_config: PoolConfig,
    _manager: PhantomData<M>,
}

impl<M: Manager> PoolBuilder<M> {
    pub fn new() -> Self {
        Self {
            pool_config: PoolConfig::default(),
            _manager: PhantomData,
        }
    }

    pub fn pool_config(mut self, pool_config: PoolConfig) -> Self {
        self.pool_config = pool_config;
        self
    }

    pub async fn build(self, manager: M) -> Result<Pool<M>, ConnectionError> {
        Pool::connect(manager, self.pool_config).await
    }
}

pub struct PoolInner<M: Manager> {
    manager: M,
    config: PoolConfig,
    state: PoolState,
    semaphore: Semaphore,
    idle_connections: ArrayQueue<IdleConnection<M>>,
}

impl<M: Manager> PoolInner<M> {
    pub fn new_arc(manager: M, pool_config: PoolConfig) -> Arc<Self> {
        let inner = Self {
            manager,
            config: pool_config,
            state: Default::default(),
            semaphore: Semaphore::new(pool_config.max_size as usize),
            idle_connections: ArrayQueue::new(pool_config.max_size as usize),
        };

        let pool = Arc::new(inner);
        spawn_maintenance_tasks(&pool);

        pool
    }

    pub fn try_acquire_from_idle(
        self: &Arc<Self>,
    ) -> Option<ConnectionGuard<M, IdleConnection<M>>> {
        let permit = self.semaphore.try_acquire_many(1).ok()?;
        self.pop_idle(permit).ok()
    }

    fn pop_idle<'a>(
        self: &'a Arc<Self>,
        permit: SemaphorePermit<'a>,
    ) -> Result<ConnectionGuard<M, IdleConnection<M>>, SemaphorePermit<'a>> {
        if let Some(idle_connection) = self.idle_connections.pop() {
            self.state.idle_count.fetch_sub(1, Ordering::AcqRel);
            Ok(ConnectionGuard::idle(idle_connection, self.clone(), permit))
        } else {
            Err(permit)
        }
    }

    pub fn release(&self, connection: ConnectionGuard<M, LiveConnection<M>>) {
        let ConnectionGuard {
            inner: connection,
            guard,
        } = connection.into_idle();

        if self.idle_connections.push(connection).is_err() {
            panic!("connection pool overflow when releasing a connection");
        }
        guard.release_without_resizing();
        self.state.idle_count.fetch_add(1, Ordering::AcqRel);
    }

    pub async fn acquire(
        self: &Arc<Self>,
    ) -> Result<ConnectionGuard<M, LiveConnection<M>>, ConnectionError> {
        let acquired = tokio::time::timeout(self.config.timeouts.acquire, async {
            loop {
                let acquire_permit = self.semaphore.acquire_many(1).await?;
                let guard = match self.pop_idle(acquire_permit) {
                    Ok(idle) => match self.manager.check(idle.inner.connection).await {
                        Ok(raw) => {
                            return Ok(
                                LiveConnection::new(raw, idle.inner.state).with_guard(idle.guard)
                            );
                        }
                        Err(_) => idle.guard,
                    },
                    Err(permit) => {
                        if let Ok(guard) = self.try_increment_connection_count(permit) {
                            guard
                        } else {
                            tokio::task::yield_now().await;
                            continue;
                        }
                    }
                };
                return self.connect(guard).await;
            }
        })
        .await?;

        acquired
    }

    pub fn try_increment_connection_count<'a>(
        self: &'a Arc<Self>,
        permit: SemaphorePermit<'a>,
    ) -> Result<ConnectionPoolGuard<M>, SemaphorePermit<'a>> {
        let result = self
            .state
            .connection_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
                size.checked_add(1)
                    .filter(|size| size <= &self.config.max_size)
            })
            .ok();

        match result {
            Some(_) => Ok(ConnectionPoolGuard::from_permit(self.clone(), permit)),
            None => Err(permit),
        }
    }

    pub async fn connect(
        self: &Arc<Self>,
        guard: ConnectionPoolGuard<M>,
    ) -> Result<ConnectionGuard<M, LiveConnection<M>>, ConnectionError> {
        let mut retry = 0;
        loop {
            let duration = self.config.timeouts.connect;
            let connection = tokio::time::timeout(duration, self.manager.connect()).await?;

            match connection {
                Ok(connection) => {
                    return Ok(LiveConnection::new(connection, ConnectionState::default())
                        .with_guard(guard));
                }
                Err(e) => {
                    if retry > 3 {
                        return Err(ConnectionError::PoolConnect(e.to_string()));
                    }
                    retry += 1;
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    pub fn idle_count(&self) -> u32 {
        self.state.idle_count.load(Ordering::Acquire)
    }

    pub fn connection_count(&self) -> u32 {
        self.state.connection_count.load(Ordering::Acquire)
    }

    pub async fn try_min_connections(self: &Arc<Self>) -> Result<(), ConnectionError> {
        while self.connection_count() < self.config.min_size {
            let Some(permit) = self.semaphore.try_acquire_many(1).ok() else {
                break;
            };
            let Some(guard) = self.try_increment_connection_count(permit).ok() else {
                break;
            };
            let connection = self.connect(guard).await?;
            self.release(connection);
        }
        Ok(())
    }
}

fn spawn_maintenance_tasks<M: Manager>(pool: &Arc<PoolInner<M>>) {
    let pool_weak = Arc::downgrade(pool);
    let period = pool.config.pool_maintenance_interval;

    tokio::task::spawn(async move {
        while let Some(pool) = pool_weak.upgrade() {
            let next_run = Instant::now() + period;

            for _ in 0..pool.idle_count() {
                if let Some(conn) = pool.try_acquire_from_idle() {
                    match pool
                        .manager
                        .should_recycle(&conn.inner.connection, &conn.inner.state)
                        .await
                    {
                        Ok(_) => {
                            pool.release(conn.into_live());
                        }
                        Err(_) => {
                            let _ = conn.close();
                            let _ = pool.try_min_connections().await;
                        }
                    }
                }
            }

            drop(pool);

            if let Some(duration) = next_run.checked_duration_since(Instant::now()) {
                tokio::time::sleep(duration).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    });
}
