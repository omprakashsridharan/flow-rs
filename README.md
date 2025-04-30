# Flow RS

A Rust library for building asynchronous data processing pipelines.

`flow-rs` provides components to create flexible and efficient pipelines where data flows from a source, optionally gets filtered, and is processed by different flows concurrently.

## Core Concepts

*   **`Message`**: The basic unit of data flowing through the pipeline.
*   **`MessageSource`**: Generates messages to be fed into the pipeline (e.g., reading from a sensor, network socket, or generating data).
*   **`BroadcastSource`**: A source that can broadcast messages to multiple consumers (Flows). Often wraps a `MessageSource` with a trigger mechanism (e.g., polling interval).
*   **`Filter`**: Selectively allows messages to pass through based on a condition.
*   **`Flow`**: Represents a processing stage in the pipeline. It receives messages from a `BroadcastSource`, potentially filters them, and makes them available for consumption.
*   **`Pipeline`**: Manages the overall structure, connecting the `BroadcastSource` to multiple `Flows` and handling the lifecycle (start, stop).

## Getting Started

This example demonstrates a simple pipeline with one source broadcasting messages to two flows, one with a filter.

### 1. Define a Message Source

Implement the `MessageSource` trait to define how your messages are generated.

```rust
use flow_core::message::Message;
use flow_core::message_source::MessageSource;
use std::error::Error;

struct MyMessageSource {
    pub name: String,
}

#[async_trait::async_trait]
impl MessageSource for MyMessageSource {
    type Payload = String;

    async fn receive(&self) -> Result<Message<String>, Box<dyn Error>> {
        let name = self.name.clone();
        let msg = format!("Hello {} at {:?}", name, std::time::Instant::now());
        Ok(Message::new(msg))
    }
}
```

### 2. Define a Filter (Optional)

Implement the `Filter` trait if you need to selectively process messages in a flow.

```rust
use flow_core::filter::Filter;
use flow_core::message::Message;

struct HelloFilter {}

impl Filter<String> for HelloFilter {
    fn filter(&self, message: &Message<String>) -> bool {
        message.get_payload().starts_with("Hello")
    }
}
```

### 3. Build the Pipeline

Configure and assemble the pipeline components.

```rust
use flow_core::broadcast_source::{BroadcastSource, PollingSource};
use flow_core::config::FlowConfigBuilder;
use flow_core::flow::Flow;
use flow_core::pipeline::Pipeline;
use flow_core::trigger::IntervalTrigger;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use log::info;
use tokio::signal;
// ... include MyMessageSource and HelloFilter definitions ...

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    info!("Basic example starting...");

    let app_cancel_token = CancellationToken::new();

    // 1. Create the underlying source
    let my_source = Arc::new(MyMessageSource {
        name: String::from("Omprakash (Shared Source)"),
    });

    // 2. Create a trigger (e.g., poll every second)
    let source_trigger = Arc::new(IntervalTrigger::new(Duration::from_secs(1)));

    // 3. Wrap the source and trigger in a BroadcastSource (using PollingSource)
    let broadcast_source = Arc::new(PollingSource::new(my_source, source_trigger, 100));

    // 4. Configure Flow 1 (with filter) - Source is no longer passed here
    let flow1_config = FlowConfigBuilder::default()
        .name("Flow 1".to_string())
        .messages_capacity(100)
        .filter(Some(Arc::new(HelloFilter {}))) // Apply the filter
        .build()
        .unwrap();
    let flow1: Flow<String> = Flow::new(flow1_config);

    // 5. Configure Flow 2 (no filter) - Source is no longer passed here
    let flow2_config = FlowConfigBuilder::default()
        .name("Flow 2".to_string())
        .messages_capacity(100)
        .filter(None) // No filter
        .build()
        .unwrap();
    let flow2: Flow<String> = Flow::new(flow2_config);

    // 6. Create the Pipeline
    let pipeline = Pipeline::new(broadcast_source, vec![flow1, flow2]);

    // 7. Start the pipeline and get output channels
    let mut output_channels = pipeline.start(app_cancel_token.clone()).await;
    let mut flow1_channel = output_channels.remove("Flow 1").unwrap();
    let mut flow2_channel = output_channels.remove("Flow 2").unwrap();

    // 8. Spawn tasks to consume messages from each flow's channel (using info!)
    tokio::spawn(async move {
        while let Ok(message) = flow1_channel.receive().await {
            info!("Flow 1 RCV: {}", message.get_payload());
        }
        info!("Flow 1 finished receiving.");
    });

    tokio::spawn(async move {
        while let Ok(message) = flow2_channel.receive().await {
            info!("Flow 2 RCV: {}", message.get_payload());
            // tokio::time::sleep(Duration::from_millis(50)).await; // Optional delay
        }
        info!("Flow 2 finished receiving.");
    });

    // Keep the application running until Ctrl+C is pressed (updated signal handling)
    signal::ctrl_c().await.expect("Failed to listen for ctrl-c");
    info!("Ctrl+C received. Cancelling pipeline...");
    app_cancel_token.cancel();

    // Allow time for shutdown
    tokio::time::sleep(Duration::from_secs(1)).await;
    info!("Shutdown complete.");
}
```

### 4. Run the Example

You can find this basic example in the `examples/basic` directory. To run it:

```bash
# Make sure to enable info logs to see output
RUST_LOG=info cargo run --example basic
```

This will start the pipeline, and you'll see messages being printed (via logger) from both flows, with Flow 1 only showing messages that start with "Hello". Press Ctrl+C to stop.

## Future Plans

We plan to expand `flow-rs` with additional modules and plugins, including:

*   **Kafka Integration**: Connectors for producing and consuming messages from Kafka topics.
*   **SQL Database Integration**: Sources and sinks for interacting with various SQL databases.
*   **More Data Source Adapters**: Support for other common data sources and messaging systems.
*   **Enhanced Monitoring & Metrics**: Built-in capabilities for observing pipeline performance.