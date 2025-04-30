use env_logger::Env;
use flow_core::broadcast_source::PollingSource;
use flow_core::config::FlowConfigBuilder;
use flow_core::filter::Filter;
use flow_core::flow::Flow;
use flow_core::message::Message;
use flow_core::message_source::MessageSource;
use flow_core::pipeline::Pipeline;
use flow_core::trigger::IntervalTrigger;
use log::{error, info};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;

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

struct HelloFilter {}

impl Filter<String> for HelloFilter {
    fn filter(&self, message: &Message<String>) -> bool {
        message.get_payload().starts_with("Hello")
    }
}

#[tokio::main]
async fn main() {
    // Configure env_logger programmatically
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    info!("Basic example with Pipeline");

    let app_cancel_token = CancellationToken::new();

    let my_source = Arc::new(MyMessageSource {
        name: String::from("Omprakash (Shared Source)"),
    });

    let source_trigger = Arc::new(IntervalTrigger::new(Duration::from_secs(1)));

    let broadcast_source = Arc::new(PollingSource::new(my_source, source_trigger, 100));

    let flow1_config = FlowConfigBuilder::default()
        .name("Flow 1".to_string())
        .messages_capacity(100)
        .filter(Some(Arc::new(HelloFilter {})))
        .build()
        .unwrap();

    let flow1: Flow<String> = Flow::new(flow1_config);

    let flow2_config = FlowConfigBuilder::default()
        .name("Flow 2".to_string())
        .messages_capacity(100)
        .filter(None)
        .build()
        .unwrap();

    let flow2: Flow<String> = Flow::new(flow2_config);

    let pipeline = Pipeline::new(broadcast_source, vec![flow1, flow2]);

    let mut output_channels = pipeline.start(app_cancel_token.clone()).await;
    let mut flow1_channel = output_channels
        .remove("Flow 1")
        .expect("Flow 1 channel not found");
    let mut flow2_channel = output_channels
        .remove("Flow 2")
        .expect("Flow 2 channel not found");

    tokio::spawn(async move {
        while let Ok(message) = flow1_channel.receive().await {
            info!("Flow 1 RCV: {}", message.get_payload());
        }
        info!("Flow 1 finished receiving.");
    });

    tokio::spawn(async move {
        while let Ok(message) = flow2_channel.receive().await {
            info!("Flow 2 RCV: {}", message.get_payload());
            sleep(Duration::from_millis(50)).await;
        }
        info!("Flow 2 finished receiving.");
    });

    match signal::ctrl_c().await {
        Ok(()) => {
            info!("Ctrl+C received. Cancelling pipeline...");
            app_cancel_token.cancel();
        }
        Err(err) => {
            error!("Unable to listen for shutdown signal: {}", err);
            app_cancel_token.cancel();
        }
    }

    sleep(Duration::from_secs(1)).await;
    info!("Shutdown complete.");
}
