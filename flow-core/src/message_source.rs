use std::error::Error;
use std::fmt::Debug;
use crate::message::Message;
use async_trait::async_trait;

#[async_trait]
pub trait MessageSource: Send + Sync {

    type Payload: Clone + Debug + Send + Sync + 'static;

    async fn receive(&self) -> Result<Message<Self::Payload>, Box<dyn Error>>;
}