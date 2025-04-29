use std::time::Duration;
use tokio::time::sleep;
use async_trait::async_trait;

#[async_trait]
pub trait Trigger: Send + Sync + 'static {
    async fn execute(&self);
}

pub struct IntervalTrigger {
    duration: Duration
}

impl IntervalTrigger {
    pub fn new(duration: Duration) -> Self {
        Self { duration }
    }
}

#[async_trait]
impl Trigger for IntervalTrigger {
    async fn execute(&self) {
        sleep(self.duration).await;
    }
}

// pub struct ChannelTrigger {
//     channel: Channel<Message>
// }

// impl ChannelTrigger {
//     pub fn new(channel: Channel<Message>) -> Self {
//         Self { channel }
//     }
// }
