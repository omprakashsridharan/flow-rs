use std::time::Duration;

pub struct FlowConfig {
  messages_capacity: usize,
  interval: Option<Duration>,
  name: String
}

impl FlowConfig {
  pub fn new(name: String,messages_capacity: usize, interval: Option<Duration>) -> Self {
    Self {
      name,
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
  
  pub fn get_name(&self) -> String {
    self.name.clone()
  }
}
