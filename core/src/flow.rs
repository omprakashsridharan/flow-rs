use crate::channel::Channel;
use crate::config::FlowConfig;
use crate::message_source::MessageSource;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::broadcast::channel;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

pub struct Flow<T: Clone + Send + 'static> {
    source: Arc<dyn MessageSource<T> + Send + Sync>,
    config: FlowConfig,
}

impl<T: Clone + Send + 'static + Debug> Flow<T> {
    pub fn new(source: Arc<dyn MessageSource<T> + Send + Sync>, config: FlowConfig) -> Self {
        Self { source, config }
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> Channel<T> {
        let (broadcast_sender, broadcast_receiver) = channel(self.config.get_messages_capacity());
        let source_clone = Arc::clone(&self.source);
        let broadcast_sender_clone = broadcast_sender.clone();
        let duration = self.config.get_interval();

        tokio::spawn(async move {
            loop {
                let bc = broadcast_sender_clone.clone();
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("cancelled flow");
                        break;
                    }
                    _ = sleep(duration) => {
                        let message = source_clone.receive().expect("TODO: panic message");
                        bc.send(message).unwrap();
                    }
                }
            }
        });

        Channel::new(broadcast_sender, broadcast_receiver)
    }
}
