use crate::message::Message;
use crate::message_source::MessageSource;
use async_trait::async_trait;
use std::{error::Error, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};

/// Wraps a `tokio::sync::broadcast::Receiver` to make it usable as a `MessageSource`.
///
/// This allows the output `Channel` of one flow to be used as the input source
/// for another flow, bridging the gap between the `&mut self` requirement of
/// `Receiver::recv` and the `&self` requirement of `MessageSource::receive`.
/// It uses an internal `Mutex` to manage access to the receiver, ensuring
/// thread safety (`Send + Sync`).
pub struct ChannelSource<T: Clone + Send + 'static> {
    // Arc<Mutex<>> is used to allow shared access (&self) while enabling
    // mutable access internally (&mut receiver) required by recv(),
    // and to satisfy Send + Sync bounds required by MessageSource.
    receiver: Arc<Mutex<Receiver<Message<T>>>>,
}

impl<T: Clone + Send + 'static> ChannelSource<T> {
    /// Creates a new `ChannelSource` taking ownership of a `Receiver`.
    ///
    /// The receiver is typically obtained from an existing `Channel`,
    /// potentially by cloning it if the `Channel` provides such a method.
    pub fn new(receiver: Receiver<Message<T>>) -> Self {
        Self {
            receiver: Arc::new(Mutex::new(receiver)),
        }
    }
}

#[async_trait]
impl<T: Clone + Send + Sync + 'static> MessageSource for ChannelSource<T> {

    type Payload = T;
    /// Receives a message from the underlying broadcast channel.
    ///
    /// This method acquires a lock on the internal `Mutex` to ensure exclusive
    /// access to the `Receiver` before calling `recv()`.
    async fn receive(&self) -> Result<Message<T>, Box<dyn Error>> {
        let mut guard = self.receiver.lock().await;
        // recv() can return RecvError which includes Lagged or Closed.
        // We map this error into a general Box<dyn Error>.
        guard.recv().await.map_err(|e| Box::new(e) as Box<dyn Error>)
    }
}