#[derive(Clone)]
pub struct Message<T: Clone> {
    payload: T,
}

impl<T: Clone> Message<T> {
    pub fn new(payload: T) -> Self {
        Self { payload }
    }

    pub fn get_payload(&self) -> &T {
        &self.payload
    }
}
