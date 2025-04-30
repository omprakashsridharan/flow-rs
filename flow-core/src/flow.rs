use crate::channel::Channel;
use crate::config::FlowConfig;
use tokio::sync::broadcast::channel;
use tokio_util::sync::CancellationToken;
use log::{debug, error};

use crate::message::Message;

pub struct Flow<T: Clone + Send + 'static + Sync> {
    config: FlowConfig<T>,
}

impl<T: Clone + Send + 'static + Sync> Flow<T> {
    pub fn new(config: FlowConfig<T>) -> Self {
        Self { config }
    }

    pub fn get_name(&self) -> String {
        self.config.get_name()
    }

    pub async fn start(&self, cancellation_token: CancellationToken) -> Channel<T> {
        let flow_name = self.config.get_name();
        let (broadcast_sender, internal_receiver) = channel::<Message<T>>(self.config.get_messages_capacity());

        let source = self.config.get_source();
        let mut source_receiver = source.subscribe();

        let broadcast_sender_clone = broadcast_sender.clone();
        let filter_opt = self.config.get_filter();

        tokio::spawn(async move {
            loop {
                let bc_sender = broadcast_sender_clone.clone();
                let filter_opt_clone = filter_opt.clone();

                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("{}", format!("Flow {}: Cancellation received. Stopping receive loop.", flow_name));
                        break;
                    }
                    result = source_receiver.recv() => {
                        match result {
                            Ok(message) => {
                                let should_send = match &filter_opt_clone {
                                    Some(filter) => filter.filter(&message),
                                    None => true,
                                };

                                if should_send {
                                    if let Err(e) = bc_sender.send(message.clone()) {
                                        error!("Flow {}: Failed to send message to output channel: {}. No downstream receivers?", flow_name, e);
                                    }
                                }
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                                debug!("Flow {}: Lagged behind source by {} messages.", flow_name, n);
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                                debug!("Flow {}: Source channel closed. Stopping receive loop.", flow_name);
                                break;
                            }
                        }

                    }
                }
            }
            debug!("Flow {}: Receive loop finished.", flow_name);
        });

        Channel::new(broadcast_sender, internal_receiver)
    }
}
