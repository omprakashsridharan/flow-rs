use std::error::Error;
use std::fmt::Debug;
use crate::message::Message;
use async_trait::async_trait;

#[async_trait]
pub trait MessageSource<T: Clone + Debug + Send + 'static>: Send + Sync {
    async fn receive(&self) -> Result<Message<T>, Box<dyn Error>>;
}