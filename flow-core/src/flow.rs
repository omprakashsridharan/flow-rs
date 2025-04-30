use crate::channel::Channel;
use crate::config::FlowConfig;

use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::broadcast::channel;
use tokio_util::sync::CancellationToken;

use crate::message::Message;

pub struct Flow<T: Clone + Send + 'static + Sync + Debug> {
    config: FlowConfig<T>,
}

impl<T: Clone + Send + 'static + Sync + Debug> Flow<T> {
    pub fn new(config: FlowConfig<T>) -> Self {
        Self { config }
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> Channel<T> {
        let flow_name = self.config.get_name();
        let (broadcast_sender, broadcast_receiver) = channel::<Message<T>>(self.config.get_messages_capacity());
        let source_clone = Arc::clone(&self.config.get_source());
        let broadcast_sender_clone = broadcast_sender.clone();
        let trigger = Arc::clone(&self.config.get_trigger());
        let filter_opt = self.config.get_filter();
        tokio::spawn(async move {
            loop {
                let bc = broadcast_sender_clone.clone();
                let filter_opt_clone = filter_opt.clone();
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        println!("{}", format!("cancelled flow: {flow_name}"));
                        break;
                    }
                    _ = trigger.execute() => {
                        let message = source_clone.receive().await.expect("TODO: panic message");

                        let should_send = match &filter_opt_clone {
                            Some(filter) => filter.filter(&message),
                            None => true,
                        };

                        if should_send {
                            bc.send(message.clone()).unwrap();
                        }
                    }
                }
            }
        });

        Channel::new(broadcast_sender, broadcast_receiver)
    }
}
