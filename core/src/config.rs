pub struct FlowConfig {
    messages_capacity: usize,
    name: String,
}

impl FlowConfig {
    pub fn new(
        name: String,
        messages_capacity: usize,
    ) -> Self {
        Self {
            name,
            messages_capacity,
        }
    }

    pub fn get_messages_capacity(&self) -> usize {
        self.messages_capacity
    }

    pub fn get_name(&self) -> String {
        self.name.clone()
    }
}
