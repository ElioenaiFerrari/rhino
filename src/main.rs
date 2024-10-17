use std::{pin::Pin, time::Duration};

use dotenv::dotenv;
use moka::sync::Cache;
use pb::{rhino_server::Rhino, SubscriptionResponse};
use prost::Message;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{self, transport::Server};
use uuid::Uuid;

pub mod pb {
    use tonic::include_proto;

    include_proto!("rhino");
}

#[derive(Debug, Clone)]
pub struct Data {
    pub messages: Vec<SubscriptionResponse>,
    pub offset: usize,
}

struct RhinoServer {
    pub cache: Cache<String, Data>,
}

#[tonic::async_trait]
impl Rhino for RhinoServer {
    async fn publish(
        &self,
        request: tonic::Request<pb::PublishRequest>,
    ) -> Result<tonic::Response<pb::PublishResponse>, tonic::Status> {
        let topic = request.get_ref().topic.clone();

        if self.cache.contains_key(&topic) {
            let id = Uuid::now_v7().to_string();
            let mut data = self.cache.get(&topic).unwrap().clone();
            let message = pb::SubscriptionResponse {
                id: id.clone(),
                topic: topic.clone(),
                data: request.get_ref().data.clone(),
                ..Default::default()
            };

            data.messages.push(message);
            self.cache.insert(topic.clone(), data);

            Ok(tonic::Response::new(pb::PublishResponse {
                id: id,
                topic: topic,
                ..Default::default()
            }))
        } else {
            let id = Uuid::now_v7().to_string();
            let message = pb::SubscriptionResponse {
                id: id.clone(),
                topic: topic.clone(),
                data: request.get_ref().data.clone(),
                ..Default::default()
            };
            self.cache.insert(
                topic.clone(),
                Data {
                    messages: vec![message],
                    offset: 0,
                },
            );

            Ok(tonic::Response::new(pb::PublishResponse {
                id: id,
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
            .unwrap_or(Data {
                messages: vec![],
                offset: 0,
            })
            .clone();
        let mut stream = Box::pin(tokio_stream::iter(repeat.messages.into_iter().skip(repeat.offset)).throttle(Duration::from_secs(1)));

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            loop {
                if let Some(item) = stream.next().await {
                    match tx.send(Ok(item.clone())).await {
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
