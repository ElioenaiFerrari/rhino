use std::{pin::Pin, time::Duration};

use dotenv::dotenv;
use moka::sync::Cache;
use pb::{rhino_server::Rhino, SubscriptionResponse};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{self, transport::Server};

pub mod pb {
    use tonic::include_proto;

    include_proto!("rhino");
}

struct RhinoServer {
    pub cache: Cache<String, Vec<SubscriptionResponse>>,
}

#[tonic::async_trait]
impl Rhino for RhinoServer {
    async fn publish(
        &self,
        request: tonic::Request<pb::PublishRequest>,
    ) -> Result<tonic::Response<pb::PublishResponse>, tonic::Status> {
        let topic = request.get_ref().topic.clone();

        if self.cache.contains_key(&topic) {
            let mut messages = self.cache.get(&topic).unwrap().clone();
            let message = pb::SubscriptionResponse {
                topic: topic.clone(),
                data: request.get_ref().data.clone(),
                ..Default::default()
            };
            messages.push(message);
            self.cache.insert(topic.clone(), messages);

            Ok(tonic::Response::new(pb::PublishResponse {
                topic: topic,
                ..Default::default()
            }))
        } else {
            let message = pb::SubscriptionResponse {
                topic: topic.clone(),
                data: request.get_ref().data.clone(),
                ..Default::default()
            };
            self.cache.insert(topic.clone(), vec![message]);

            Ok(tonic::Response::new(pb::PublishResponse {
                topic: topic,
                ..Default::default()
            }))
        }
    }

    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<pb::SubscriptionResponse, tonic::Status>> + Send>>;

    async fn subscribe(
        &self,
        request: tonic::Request<pb::SubscriptionRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let repeat = self
            .cache
            .get(&request.get_ref().topic)
            .unwrap_or(vec![])
            .clone();
        let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_millis(200)));

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            loop {
                if let Some(item) = stream.next().await {
                    match tx.send(Ok(item)).await {
                        Ok(_) => {}
                        Err(_item) => {
                            // output_stream was build from rx and both are dropped
                            break;
                        }
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(tonic::Response::new(
            Box::pin(output_stream) as Self::SubscribeStream
        ))
    }
}

#[tokio::main]
async fn main() -> tonic::Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let cache = Cache::new(10_000);

    Server::builder()
        .add_service(pb::rhino_server::RhinoServer::new(RhinoServer { cache }))
        .serve(([127, 0, 0, 1], 50051).into())
        .await?;

    Ok(())
}
