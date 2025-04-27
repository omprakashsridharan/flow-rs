use std::error::Error;
use std::fmt::Debug;
use crate::message::Message;

pub trait MessageSource<T: Clone + Debug> {
    fn receive(&self) -> Result<Message<T>, Box<dyn Error>>;
}