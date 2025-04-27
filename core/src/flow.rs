use crate::channel::Channel;
use crate::message_source::MessageSource;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::broadcast::channel;
use tokio::time::{sleep, Duration};

pub struct Flow<T: Clone + Send + 'static> {
    source: Arc<dyn MessageSource<T> + Send + Sync>,
}

impl<T: Clone + Send + 'static + Debug> Flow<T> {
    pub fn new(source: Arc<dyn MessageSource<T> + Send + Sync>) -> Self {
        Self { source }
    }

    pub fn start(&self, messages_capacity: usize) -> Channel<T> {
        let (broadcast_sender, broadcast_receiver) = channel(messages_capacity);
        let source_clone = Arc::clone(&self.source);
        let broadcast_sender_clone = broadcast_sender.clone();
        tokio::spawn(async move {
            loop {
                let message = source_clone.receive().expect("TODO: panic message");
                broadcast_sender_clone.send(message).unwrap();
                sleep(Duration::from_secs(1)).await;
            }
        });

        Channel::new(broadcast_sender, broadcast_receiver)
    }
}
