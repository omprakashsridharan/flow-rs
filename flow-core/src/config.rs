use std::sync::Arc;
use derive_builder::Builder;

use crate::{filter::Filter, message_source::MessageSource, trigger::Trigger};

#[derive(Builder)]
pub struct FlowConfig<T: Clone + Send + 'static + Sync> {
    messages_capacity: usize,
    name: String,
    trigger: Arc<dyn Trigger>,
    source: Arc<dyn MessageSource<Payload = T>>,
    filter: Option<Arc<dyn Filter<T>>>,
}

impl<T: Clone + Send + 'static + Sync> FlowConfig<T> {

    pub fn get_messages_capacity(&self) -> usize {
        self.messages_capacity
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_trigger(&self) -> Arc<dyn Trigger> {
        self.trigger.clone()
    }

    pub fn get_source(&self) -> Arc<dyn MessageSource<Payload = T>> {
        self.source.clone()
    }

    pub fn get_filter(&self) -> Option<Arc<dyn Filter<T>>> {
        self.filter.clone()
    }
}
