use std::sync::Arc;

use crate::{message_source::MessageSource, trigger::Trigger};

pub struct FlowConfig<T: Clone + Send + 'static + Sync> {
    messages_capacity: usize,
    name: String,
    trigger: Arc<dyn Trigger>,
    source: Arc<dyn MessageSource<T>>,
}

impl<T: Clone + Send + 'static + Sync> FlowConfig<T> {
    pub fn new(
        name: String,
        messages_capacity: usize,
        trigger: Arc<dyn Trigger>,
        source: Arc<dyn MessageSource<T>>,
    ) -> Self {
        Self {
            name,
            messages_capacity,
            trigger,
            source,
        }
    }

    pub fn get_messages_capacity(&self) -> usize {
        self.messages_capacity
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_trigger(&self) -> Arc<dyn Trigger> {
        self.trigger.clone()
    }

    pub fn get_source(&self) -> Arc<dyn MessageSource<T>> {
        self.source.clone()
    }
}
