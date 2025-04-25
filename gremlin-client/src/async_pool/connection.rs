use std::sync::Arc;

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
    pub guard: ConnectionPoolGuard<M>,
}

impl<M: Manager> LiveConnection<M> {
    pub fn new(connection: M::Connection, guard: ConnectionPoolGuard<M>) -> Self {
        Self {
            connection,
            state: ConnectionState::default(),
            guard,
        }
    }

    pub fn from_idle(
        idle: IdleConnection<M>,
        pool: Arc<PoolInner<M>>,
        permit: SemaphorePermit<'_>,
    ) -> Self {
        Self {
            connection: idle.connection,
            state: idle.state,
            guard: ConnectionPoolGuard::from_permit(pool, permit),
        }
    }

    pub fn close(self) -> ConnectionPoolGuard<M> {
        self.guard
    }
}
