use std::time::Duration;

#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct PoolConfig {
    pub max_size: usize,
    pub min_size: usize,
    pub timeouts: Timeouts,
}

impl PoolConfig {
    pub fn new(max_size: usize, min_size: usize) -> Self {
        Self {
            max_size,
            min_size,
            timeouts: Timeouts::default(),
        }
    }

    pub fn with_timeouts(mut self, timeouts: Timeouts) -> Self {
        self.timeouts = timeouts;
        self
    }
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: 8,
            min_size: 1,
            timeouts: Timeouts::default(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Timeouts {
    pub idle: Option<Duration>,
    pub create: Option<Duration>,
}

impl Timeouts {
    pub fn with_idle(mut self, idle: Option<Duration>) -> Self {
        self.idle = idle;
        self
    }

    pub fn with_create(mut self, create: Option<Duration>) -> Self {
        self.create = create;
        self
    }
}

impl Default for Timeouts {
    fn default() -> Self {
        Self {
            idle: None,
            create: None,
        }
    }
}
