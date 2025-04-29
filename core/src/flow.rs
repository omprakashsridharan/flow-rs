use crate::channel::Channel;
use crate::config::FlowConfig;
use crate::message_source::MessageSource;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::broadcast::channel;
use tokio_util::sync::CancellationToken;
use crate::trigger::Trigger;

pub struct Flow<T: Clone + Send + 'static> {
    source: Arc<dyn MessageSource<T> + Send + Sync>,
    config: FlowConfig,
}

impl<T: Clone + Send + 'static + Debug> Flow<T> {
    pub fn new(source: Arc<dyn MessageSource<T> + Send + Sync>, config: FlowConfig) -> Self {
        Self { source, config }
    }

    pub async fn start(&self, trigger: Arc<dyn Trigger>, cancellation_token: CancellationToken) -> Channel<T> {
        let flow_name = self.config.get_name();
        let (broadcast_sender, broadcast_receiver) = channel(self.config.get_messages_capacity());
        let source_clone = Arc::clone(&self.source);
        let broadcast_sender_clone = broadcast_sender.clone();

        tokio::spawn(async move {
            loop {
                let bc = broadcast_sender_clone.clone();
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("{}", format!("cancelled flow: {flow_name}"));
                        break;
                    }
                    _ = trigger.execute() => {
                        let message = source_clone.receive().expect("TODO: panic message");
                        bc.send(message).unwrap();
                    }
                }
            }
        });

        Channel::new(broadcast_sender, broadcast_receiver)
    }
}
