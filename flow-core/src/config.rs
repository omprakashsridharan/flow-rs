use derive_builder::Builder;
use std::sync::Arc;

use crate::filter::Filter;

#[derive(Builder)]
pub struct FlowConfig<T: Clone + Send + 'static + Sync> {
    messages_capacity: usize,
    name: String,
    filter: Option<Arc<dyn Filter<T>>>,
}

impl<T: Clone + Send + 'static + Sync> FlowConfig<T> {
    pub fn get_messages_capacity(&self) -> usize {
        self.messages_capacity
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_filter(&self) -> Option<Arc<dyn Filter<T>>> {
        self.filter.clone()
    }
}
