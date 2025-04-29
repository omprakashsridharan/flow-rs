use std::error::Error;
use std::fmt::Debug;
use crate::message::Message;

pub trait MessageSource<T: Clone + Debug + Send + 'static>: Send + Sync {
    fn receive(&self) -> Result<Message<T>, Box<dyn Error>>;
}