use crate::broadcast_source::BroadcastSource;
use crate::channel::Channel;
use crate::flow::Flow;
use log::debug;
use std::collections::HashMap;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Manages the lifecycle of a BroadcastSource and multiple connected Flows.
pub struct Pipeline<T: Clone + Send + Sync + 'static> {
    source: Arc<dyn BroadcastSource<Payload = T>>,
    flows: Vec<Flow<T>>,
}

impl<T: Clone + Send + Sync + 'static> Pipeline<T> {
    /// Creates a new Pipeline.
    ///
    /// # Arguments
    ///
    /// * `source` - The shared `BroadcastSource` for all flows in this pipeline.
    /// * `flows` - A vector of `Flow` instances that will subscribe to the source.
    pub fn new(source: Arc<dyn BroadcastSource<Payload = T>>, flows: Vec<Flow<T>>) -> Self {
        Self { source, flows }
    }

    /// Starts the pipeline.
    ///
    /// This starts the broadcast source's polling loop and starts each flow's
    /// processing loop in separate Tokio tasks, managed by the provided CancellationToken.
    ///
    /// # Arguments
    ///
    /// * `cancellation_token` - Token used to signal shutdown for the source and all flows.
    ///
    /// # Returns
    ///
    /// A `HashMap` where keys are the names of the flows and values are the
    /// corresponding `Channel` instances for their output.
    pub async fn start(
        &self,
        cancellation_token: CancellationToken,
    ) -> HashMap<String, Channel<T>> {
        // Start the source runner task
        let source_runner_token = cancellation_token.clone();
        let source_to_run = self.source.clone();
        tokio::spawn(async move {
            source_to_run.run(source_runner_token).await;
            debug!("Pipeline: Source runner task finished.");
        });

        // Start each flow and collect their output channels in a HashMap
        let mut output_channels = HashMap::with_capacity(self.flows.len());
        for flow in &self.flows {
            let flow_cancel_token = cancellation_token.clone();
            // Subscribe to the source for this specific flow
            let input_receiver = self.source.subscribe();
            // Pass the receiver to the modified flow.start
            let output_channel = flow.start(flow_cancel_token, input_receiver).await;
            // Use flow name as the key
            output_channels.insert(flow.get_name(), output_channel);
        }

        output_channels
    }
}
