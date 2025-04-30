use flow_core::channel_source::ChannelSource;
use flow_core::config::FlowConfigBuilder;
use flow_core::filter::Filter;
use flow_core::flow::Flow;
use flow_core::message::Message;
use flow_core::message_source::MessageSource;
use flow_core::trigger::IntervalTrigger;
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
        Ok(Message::new(format!("Hello {name}")))
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
    println!("Basic examples");

    let cancellation_token = CancellationToken::new();

    let flow1_config = FlowConfigBuilder::default()
        .name("Flow 1".to_string())
        .messages_capacity(100)
        .trigger(Arc::new(IntervalTrigger::new(Duration::from_secs(1))))
        .source(Arc::new(MyMessageSource {
            name: String::from("flow 1 Omprakash"),
        }))
        .filter(Some(Arc::new(HelloFilter {})))
        .build()
        .unwrap();

    let flow1: Flow<String> = Flow::new(flow1_config);
    let mut flow1_channel = flow1.start(cancellation_token.clone()).await;

    let flow2_config = FlowConfigBuilder::default()
        .name("Flow 2".to_string())
        .messages_capacity(100)
        .trigger(Arc::new(IntervalTrigger::new(Duration::from_secs(1))))
        .source(Arc::new(ChannelSource::new(flow1_channel.subscribe())))
        .filter(None)
        .build()
        .unwrap();

    let flow2: Flow<String> = Flow::new(flow2_config);
    let mut flow2_channel = flow2.start(cancellation_token.clone()).await;
    tokio::spawn(async move {
        while let Ok(message) = flow1_channel.receive().await {
            println!("Received message from flow 1: {}", message.get_payload());
        }
    });

    tokio::spawn(async move {
        while let Ok(message) = flow2_channel.receive().await {
            println!("Received message from flow 2: {}", message.get_payload());
            sleep(Duration::from_secs(2)).await;
        }
    });

    match signal::ctrl_c().await {
        Ok(()) => {
            cancellation_token.cancel();
        }
        Err(err) => {
            eprintln!("Unable to listen for shutdown signal: {}", err);
            // we also shut down in case of error
        }
    }
}
