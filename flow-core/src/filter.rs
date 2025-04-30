use crate::message::Message;

pub trait Filter<T: Clone + Send + 'static + Sync>: Send + Sync {
    fn filter(&self, message: &Message<T>) -> bool;
}