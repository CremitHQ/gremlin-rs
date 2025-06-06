use std::{
    sync::atomic::AtomicU32,
    time::{Duration, Instant},
};

#[non_exhaustive]
pub struct PoolState {
    pub connection_count: AtomicU32,
    pub idle_count: AtomicU32,
}

impl Default for PoolState {
    fn default() -> Self {
        Self {
            connection_count: AtomicU32::new(0),
            idle_count: AtomicU32::new(0),
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectionState {
    pub created: Instant,
    pub idle_since: Option<Instant>,
}

impl Default for ConnectionState {
    fn default() -> Self {
        Self {
            created: Instant::now(),
            idle_since: None,
        }
    }
}

impl ConnectionState {
    pub fn is_idle(&self) -> bool {
        self.idle_since.is_some()
    }

    pub fn is_beyond_idle_timeout(&self, idle_timeout: Duration) -> bool {
        self.idle_since
            .map_or(false, |idle_since| idle_since.elapsed() > idle_timeout)
    }

    pub fn set_idle(&mut self) {
        self.idle_since = Some(Instant::now());
    }

    pub fn set_live(&mut self) {
        self.idle_since = None;
    }
}
