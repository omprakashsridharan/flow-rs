use std::{error::Error, fmt::Debug};
use crate::message::Message;
use tokio::sync::broadcast::{Receiver, Sender};

#[derive(Debug)]
pub struct Channel<T: Clone + Debug> {
    broadcast_sender: Sender<Message<T>>,
    broadcast_receiver: Receiver<Message<T>>,
}

impl<T: Clone + Debug> Channel<T> {
    pub fn new(broadcast_sender: Sender<Message<T>>, broadcast_receiver: Receiver<Message<T>>) -> Self {
        Channel {
            broadcast_receiver: broadcast_receiver,
            broadcast_sender: broadcast_sender,
        }
    }

    pub fn send(&self, message: Message<T>) {
        self.broadcast_sender.send(message).unwrap();
    }

    pub async fn receive(&mut self) -> Result<Message<T>, Box<dyn Error>> {
        self.broadcast_receiver.recv().await.map_err(|e| e.into())
    }
}