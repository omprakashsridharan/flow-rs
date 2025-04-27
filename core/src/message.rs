use std::fmt::Debug;



#[derive(Clone, Debug)]
pub struct Message<T: Clone + Debug> {
    payload: T,
}

impl<T: Clone + Debug> Message<T> {
    pub fn new(payload: T) -> Self {
        Self { payload }
    }

    pub fn payload(&self) -> &T {
        &self.payload
    }
}