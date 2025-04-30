use crate::message::Message;
use std::fmt::Debug;

pub trait Filter<T: Clone + Send + 'static + Sync + Debug>: Send + Sync {
    fn filter(&self, message: &Message<T>) -> bool;
}