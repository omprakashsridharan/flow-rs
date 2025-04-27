use core::flow::Flow;
use core::message::Message;
use core::message_source::MessageSource;
use std::error::Error;
use std::sync::Arc;
use tokio::signal;

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
    let flow: Flow<String> = Flow::new(Arc::new(my_source));
    let mut channel = flow.start(10);
    tokio::spawn(async move {
        while let Ok(message) = channel.receive().await {
            println!("Received message: {}", message.payload());
        }
    });

    // Wait for Ctrl+C signal
    signal::ctrl_c()
        .await
        .expect("Failed to listen for ctrl_c signal");
    println!("Ctrl+C received, shutting down.");
}
