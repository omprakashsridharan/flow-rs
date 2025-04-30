use crate::message::Message;
use crate::message_source::MessageSource;
use crate::trigger::Trigger;
use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast::{self, Receiver, Sender};
use tokio_util::sync::CancellationToken;
use log::{debug, error};

/// Represents a source that can broadcast messages to multiple subscribers.
#[async_trait]
pub trait BroadcastSource: Send + Sync {
    type Payload: Clone + Send + Sync + 'static;

    /// Subscribes to the broadcast channel of this source.
    /// Each call returns a new receiver that will receive messages
    /// sent *after* the subscription call.
    fn subscribe(&self) -> Receiver<Message<Self::Payload>>;

    /// Starts the underlying message generation/polling process.
    /// This should typically spawn a task that runs until the cancellation token is triggered.
    /// It takes a CancellationToken to allow for graceful shutdown.
    async fn run(&self, cancellation_token: CancellationToken);
}

/// Wraps a `MessageSource` and a `Trigger` to implement `BroadcastSource`.
///
/// It uses the trigger to poll the underlying `MessageSource` and broadcasts
/// the received messages via a `tokio::sync::broadcast` channel.
pub struct PollingSourceWrapper<S: MessageSource> {
    source: Arc<S>,
    trigger: Arc<dyn Trigger>,
    sender: Sender<Message<S::Payload>>,
    // Receiver is not stored here, new ones are created via sender.subscribe()
}

impl<S> PollingSourceWrapper<S>
where
    S: MessageSource + 'static, // Ensure S can live for the 'static lifetime required by tokio::spawn
{
    /// Creates a new `PollingSourceWrapper`.
    ///
    /// # Arguments
    ///
    /// * `source` - The `MessageSource` to wrap.
    /// * `trigger` - The `Trigger` that dictates when to poll the source.
    /// * `capacity` - The capacity of the broadcast channel. If a receiver lags,
    ///                it might miss messages.
    pub fn new(source: Arc<S>, trigger: Arc<dyn Trigger>, capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self {
            source,
            trigger,
            sender,
        }
    }
}

#[async_trait]
impl<S> BroadcastSource for PollingSourceWrapper<S>
where
    S: MessageSource + 'static, // 'static bound needed here too
{
    type Payload = S::Payload;

    fn subscribe(&self) -> Receiver<Message<Self::Payload>> {
        self.sender.subscribe()
    }

    async fn run(&self, cancellation_token: CancellationToken) {
        let source = Arc::clone(&self.source);
        let trigger = Arc::clone(&self.trigger);
        let sender = self.sender.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                        debug!("PollingSourceWrapper: Cancellation received. Stopping poll loop.");
                        break;
                    }
                    _ = trigger.execute() => {
                        match source.receive().await {
                            Ok(message) => {
                                // If sending fails, it means there are no active receivers.
                                // This is okay in a broadcast scenario.
                                if sender.send(message).is_err() {
                                   // println!("PollingSourceWrapper: No active receivers for message.");
                                   // Consider adding logging here if needed
                                }
                            }
                            Err(e) => {
                                error!("PollingSourceWrapper: Error receiving message from source: {}", e);
                                // Decide on error handling: continue, break, delay?
                                // For now, let's just log and continue.
                            }
                        }
                    }
                }
            }
            debug!("PollingSourceWrapper: Poll loop finished.");
        });
    }
}