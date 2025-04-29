use std::time::Duration;

pub struct FlowConfig {
  messages_capacity: usize,
  interval: Option<Duration>,
}

impl FlowConfig {
  pub fn new(messages_capacity: usize, interval: Option<Duration>) -> Self {
    Self {
      messages_capacity,
      interval,
    }
  }

  pub fn get_messages_capacity(&self) -> usize {
    self.messages_capacity
  }

  pub fn get_interval(&self) -> Duration {
    if let Some(interval) = self.interval {
      interval
    } else {
      Duration::from_secs(1)
    }
  }
}
