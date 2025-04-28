use std::time::Duration;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct PoolConfig {
    pub max_size: u32,
    pub min_size: u32,
    pub timeouts: Timeouts,
    pub pool_maintenance_interval: Duration,
}

impl PoolConfig {
    pub fn new(max_size: u32, min_size: u32) -> Self {
        Self {
            max_size,
            min_size,
            timeouts: Timeouts::default(),
            pool_maintenance_interval: Duration::from_secs(600),
        }
    }

    pub fn with_timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }

    pub fn with_pool_maintenance_interval(
        mut self,
        pool_maintenance_interval: Option<Duration>,
    ) -> Self {
        self.pool_maintenance_interval =
            pool_maintenance_interval.unwrap_or(Duration::from_secs(600));
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 10,
            min_size: 1,
            timeouts: Timeouts::default(),
            pool_maintenance_interval: Duration::from_secs(600),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub idle: Duration,
    pub acquire: Duration,
    pub connect: Duration,
}

impl Timeouts {
    pub fn with_idle(mut self, idle: Option<Duration>) -> Self {
        self.idle = idle.unwrap_or(Duration::from_secs(600));
        self
    }

    pub fn with_acquire(mut self, acquire: Option<Duration>) -> Self {
        self.acquire = acquire.unwrap_or(Duration::from_secs(30));
        self
    }

    pub fn with_connect(mut self, connect: Option<Duration>) -> Self {
        self.connect = connect.unwrap_or(Duration::from_secs(10));
        self
    }
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            idle: Duration::from_secs(600),
            acquire: Duration::from_secs(30),
            connect: Duration::from_secs(10),
        }
    }
}
