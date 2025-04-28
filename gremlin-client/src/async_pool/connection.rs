use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use tokio::sync::SemaphorePermit;

use super::{guard::ConnectionPoolGuard, manager::Manager, state::ConnectionState, PoolInner};

#[non_exhaustive]
pub struct IdleConnection<M: Manager> {
    pub connection: M::Connection,
    pub state: ConnectionState,
}

#[non_exhaustive]
pub struct LiveConnection<M: Manager> {
    pub connection: M::Connection,
    pub state: ConnectionState,
}

pub struct ConnectionGuard<M: Manager, C> {
    pub inner: C,
    pub guard: ConnectionPoolGuard<M>,
}
impl<M: Manager> IdleConnection<M> {
    pub fn new(connection: M::Connection, mut state: ConnectionState) -> Self {
        state.set_idle();
        Self { connection, state }
    }

    pub fn into_live(mut self) -> LiveConnection<M> {
        self.state.set_live();
        LiveConnection {
            connection: self.connection,
            state: self.state,
        }
    }

    pub fn with_guard(self, guard: ConnectionPoolGuard<M>) -> ConnectionGuard<M, Self> {
        ConnectionGuard { inner: self, guard }
    }
}

impl<M: Manager> LiveConnection<M> {
    pub fn new(connection: M::Connection, mut state: ConnectionState) -> Self {
        state.set_live();
        Self { connection, state }
    }

    pub fn into_idle(mut self) -> IdleConnection<M> {
        self.state.set_idle();
        IdleConnection {
            connection: self.connection,
            state: self.state,
        }
    }

    pub fn with_guard(self, guard: ConnectionPoolGuard<M>) -> ConnectionGuard<M, Self> {
        ConnectionGuard { inner: self, guard }
    }

    pub fn guard(self, pool: Arc<PoolInner<M>>) -> ConnectionGuard<M, Self> {
        ConnectionGuard {
            inner: self,
            guard: ConnectionPoolGuard::new(pool),
        }
    }
}

impl<M: Manager> ConnectionGuard<M, LiveConnection<M>> {
    pub fn live(connection: LiveConnection<M>, guard: ConnectionPoolGuard<M>) -> Self {
        Self {
            inner: connection,
            guard,
        }
    }

    pub fn into_pool_connection(self) -> PoolConnection<M> {
        let Self { inner, guard } = self;
        let pool = guard.pool.clone();
        guard.cancel();
        PoolConnection::new(inner, pool)
    }

    pub fn into_idle(self) -> ConnectionGuard<M, IdleConnection<M>> {
        ConnectionGuard {
            inner: self.inner.into_idle(),
            guard: self.guard,
        }
    }

    pub fn close(self) -> ConnectionPoolGuard<M> {
        self.guard
    }

    pub async fn return_to_pool(self) {
        self.guard.pool.clone().release(self);
    }
}

impl<M: Manager> ConnectionGuard<M, IdleConnection<M>> {
    pub fn idle(
        connection: IdleConnection<M>,
        pool: Arc<PoolInner<M>>,
        permit: SemaphorePermit<'_>,
    ) -> Self {
        Self {
            inner: connection,
            guard: ConnectionPoolGuard::from_permit(pool, permit),
        }
    }

    pub fn into_live(self) -> ConnectionGuard<M, LiveConnection<M>> {
        ConnectionGuard::live(self.inner.into_live(), self.guard)
    }

    pub fn close(self) -> ConnectionPoolGuard<M> {
        self.guard
    }
}

pub struct PoolConnection<M: Manager> {
    connection: Option<LiveConnection<M>>,
    pool: Arc<PoolInner<M>>,
}

impl<M: Manager> PoolConnection<M> {
    pub fn new(connection: LiveConnection<M>, pool: Arc<PoolInner<M>>) -> Self {
        Self {
            connection: Some(connection),
            pool,
        }
    }
}

impl<M: Manager> Deref for PoolConnection<M> {
    type Target = M::Connection;

    fn deref(&self) -> &Self::Target {
        &self
            .connection
            .as_ref()
            .expect("connection has already been taken")
            .connection
    }
}

impl<M: Manager> DerefMut for PoolConnection<M> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self
            .connection
            .as_mut()
            .expect("connection has already been taken")
            .connection
    }
}

impl<M: Manager> Drop for PoolConnection<M> {
    fn drop(&mut self) {
        let connection = self.connection.take().map(|c| c.guard(self.pool.clone()));
        if let Some(connection) = connection {
            tokio::task::spawn(connection.return_to_pool());
        } else {
            let pool = self.pool.clone();
            tokio::task::spawn(async move { pool.try_min_connections().await });
        }
    }
}
