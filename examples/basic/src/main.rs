
use flow_core::config::FlowConfig;
use flow_core::flow::Flow;
use flow_core::message::Message;
use flow_core::message_source::MessageSource;
use flow_core::trigger::IntervalTrigger;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct MyMessageSource {
    pub name: String,
}

#[async_trait::async_trait]
impl MessageSource<String> for MyMessageSource {
    async fn receive(&self) -> Result<Message<String>, Box<dyn Error>> {
        let name = self.name.clone();
        Ok(Message::new(format!("Hello {name}")))
    }
}

#[tokio::main]
async fn main() {
    println!("Basic examples");

    let cancellation_token = CancellationToken::new();

    let flow1: Flow<String> = Flow::new(FlowConfig::new(
        "Flow 1".to_string(),
        100,
        Arc::new(IntervalTrigger::new(Duration::from_secs(1))),
        Arc::new(MyMessageSource {
            name: String::from("Omprakash"),
        })
    ));
    let mut flow1_channel = flow1.start(cancellation_token.clone()).await;


    tokio::spawn(async move {
        while let Ok(message) = flow1_channel.receive().await {
            println!("Received message: {}", message.payload());
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
