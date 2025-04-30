use crate::message::Message;
use std::error::Error;
use tokio::sync::broadcast::{Receiver, Sender};

pub struct Channel<T: Clone> {
    broadcast_sender: Sender<Message<T>>,
    broadcast_receiver: Receiver<Message<T>>,
}

impl<T: Clone> Channel<T> {
    pub fn new(
        broadcast_sender: Sender<Message<T>>,
        broadcast_receiver: Receiver<Message<T>>,
    ) -> Self {
        Channel {
            broadcast_receiver: broadcast_receiver,
            broadcast_sender: broadcast_sender,
        }
    }

    pub fn send(&self, message: Message<T>) {
        if let Err(e) = self.broadcast_sender.send(message) {
            panic!("Failed to send message: {}", e);
        }
    }

    pub async fn receive(&mut self) -> Result<Message<T>, Box<dyn Error + Send + Sync>> {
        self.broadcast_receiver
            .recv()
            .await
            .map_err(|e| Box::new(e) as Box<dyn Error + Send + Sync>)
    }

    /// Creates a new receiver subscribed to this channel's broadcast sender.
    pub fn subscribe(&self) -> Receiver<Message<T>> {
        self.broadcast_sender.subscribe()
    }

    pub fn broadcast_receiver(&self) -> &Receiver<Message<T>> {
        &self.broadcast_receiver
    }
}
