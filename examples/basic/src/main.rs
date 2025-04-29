use core::config::FlowConfig;
use core::flow::Flow;
use core::message::Message;
use core::message_source::MessageSource;
use std::error::Error;
use std::sync::Arc;
use tokio::signal;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct MyMessageSource {
    pub name: String,
}

impl MessageSource<String> for MyMessageSource {
    fn receive(&self) -> Result<Message<String>, Box<dyn Error>> {
        let name = self.name.clone();
        Ok(Message::new(format!("Hello {name}")))
    }
}

#[tokio::main]
async fn main() {
    println!("Basic examples");
    let my_source = MyMessageSource {
        name: String::from("Omprakash"),
    };
    let flow: Flow<String> = Flow::new(Arc::new(my_source),
                                       FlowConfig::new("hello-world".to_string(),10, None));
    let cancellation_token = CancellationToken::new();

    let mut channel = flow.start(cancellation_token.clone()).await;
    tokio::spawn(async move {
        while let Ok(message) = channel.receive().await {
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
