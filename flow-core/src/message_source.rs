use std::error::Error;
use crate::message::Message;
use async_trait::async_trait;

#[async_trait]
pub trait MessageSource: Send + Sync {

    type Payload: Clone + Send + Sync + 'static;

    async fn receive(&self) -> Result<Message<Self::Payload>, Box<dyn Error>>;
}