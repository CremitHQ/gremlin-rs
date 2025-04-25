pub mod config;
pub mod connection;
pub mod error;
pub mod guard;
pub mod manager;
pub mod state;

use std::{
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};

use config::PoolConfig;
use connection::{IdleConnection, LiveConnection};
use crossbeam_queue::ArrayQueue;
use error::ConnectionError;
use guard::ConnectionPoolGuard;
use manager::Manager;
use state::PoolState;
use tokio::sync::{Semaphore, SemaphorePermit};

pub struct Pool<M: Manager> {
    inner: Arc<PoolInner<M>>,
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
}

struct PoolInner<M: Manager> {
    manager: M,
    config: PoolConfig,
    state: PoolState,
    semaphore: Semaphore,
    idle_connections: ArrayQueue<IdleConnection<M>>,
    connect_timeout: Option<Duration>,
}

impl<M: Manager> PoolInner<M> {
    pub fn new_arc(manager: M, pool_config: PoolConfig) -> Arc<Self> {
        let inner = Self {
            manager,
            config: pool_config,
            state: Default::default(),
            semaphore: Semaphore::new(pool_config.max_size),
            idle_connections: ArrayQueue::new(pool_config.max_size),
            connect_timeout: pool_config.timeouts.create,
        };
        Arc::new(inner)
    }

    pub fn try_acquire_from_idle(self: &Arc<Self>) -> Option<LiveConnection<M>> {
        let permit = self.semaphore.try_acquire_many(1).ok()?;
        self.pop_idle(permit).ok()
    }

    fn pop_idle<'a>(
        self: &'a Arc<Self>,
        permit: SemaphorePermit<'a>,
    ) -> Result<LiveConnection<M>, SemaphorePermit<'a>> {
        if let Some(idle_connection) = self.idle_connections.pop() {
            self.state.idle_count.fetch_sub(1, Ordering::AcqRel);
            Ok(LiveConnection::from_idle(
                idle_connection,
                self.clone(),
                permit,
            ))
        } else {
            Err(permit)
        }
    }

    pub fn release(&self, mut connection: LiveConnection<M>) {
        connection.state.set_idle();
        let idle_connection = IdleConnection {
            connection: connection.connection,
            state: connection.state,
        };
        let guard = connection.guard;

        if self.idle_connections.push(idle_connection).is_err() {
            panic!("connection pool overflow when releasing a connection");
        }
        guard.release_without_resizing();
        self.state.idle_count.fetch_add(1, Ordering::AcqRel);
    }

    pub async fn acquire(self: &Arc<Self>) -> Result<LiveConnection<M>, ConnectionError> {
        let acquired = tokio::time::timeout(
            self.connect_timeout.unwrap_or(Duration::from_secs(30)),
            async {
                loop {
                    let acquire_permit = self.semaphore.acquire_many(1).await?;
                    let guard = match self.pop_idle(acquire_permit) {
                        Ok(connection) => match self.manager.check(&connection.connection).await {
                            Ok(_) => return Ok(connection),
                            Err(_) => connection.close(),
                        },
                        Err(permit) => {
                            if let Ok(guard) = self.try_increment_size(permit) {
                                guard
                            } else {
                                tokio::task::yield_now().await;
                                continue;
                            }
                        }
                    };

                    return self.connect(guard).await;
                }
            },
        )
        .await?;

        acquired
    }

    pub fn try_increment_size<'a>(
        self: &'a Arc<Self>,
        permit: SemaphorePermit<'a>,
    ) -> Result<ConnectionPoolGuard<M>, SemaphorePermit<'a>> {
        let result =
            self.state
                .current_size
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |size| {
                    size.checked_add(1)
                        .filter(|size| size <= &self.config.max_size)
                });

        match result {
            Ok(_) => Ok(ConnectionPoolGuard::from_permit(self.clone(), permit)),
            Err(_) => Err(permit),
        }
    }

    pub async fn connect(
        self: &Arc<Self>,
        guard: ConnectionPoolGuard<M>,
    ) -> Result<LiveConnection<M>, ConnectionError> {
        let mut retry = 0;
        loop {
            let duration = self.connect_timeout.unwrap_or(Duration::from_secs(10));
            let connection = tokio::time::timeout(duration, self.manager.connect()).await?;

            match connection {
                Ok(connection) => return Ok(LiveConnection::new(connection, guard)),
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

    pub fn idle_count(&self) -> usize {
        self.state.idle_count.load(Ordering::Acquire)
    }

    pub fn connection_count(&self) -> usize {
        self.state.current_size.load(Ordering::Acquire)
    }

    pub async fn try_min_connections(self: &Arc<Self>) -> Result<(), ConnectionError> {
        while self.connection_count() < self.config.min_size {
            let Some(permit) = self.semaphore.try_acquire_many(1).ok() else {
                break;
            };
            let Some(guard) = self.try_increment_size(permit).ok() else {
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
    let period = pool
        .config
        .timeouts
        .idle
        .unwrap_or(Duration::from_secs(600));

    tokio::task::spawn(async move {
        // If the last handle to the pool was dropped while we were sleeping
        while let Some(pool) = pool_weak.upgrade() {
            let next_run = Instant::now() + period;

            for _ in 0..pool.idle_count() {
                if let Some(conn) = pool.try_acquire_from_idle() {
                    if pool.config.timeouts.idle.map_or(false, |idle_timeout| {
                        conn.state.is_beyond_idle_timeout(idle_timeout)
                    }) {
                        let _ = conn.close();
                        let _ = pool.try_min_connections().await;
                    } else {
                        pool.release(conn);
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
