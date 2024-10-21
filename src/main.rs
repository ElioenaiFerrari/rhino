use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
    vec,
};

use dotenv::dotenv;
use moka::sync::Cache;
use pb::{rhino_server::Rhino, SubscriptionResponse};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{self, transport::Server};
use uuid::Uuid;

pub mod pb {
    use tonic::include_proto;

    include_proto!("rhino");
}

struct RhinoServer {
    pub cache: Arc<Mutex<Cache<String, Vec<SubscriptionResponse>>>>,
}

#[tonic::async_trait]
impl Rhino for RhinoServer {
    async fn ack(
        &self,
        request: tonic::Request<pb::AckRequest>,
    ) -> Result<tonic::Response<pb::AckResponse>, tonic::Status> {
        let topic = request.get_ref().topic.clone();
        let id = request.get_ref().id.clone();

        if self.cache.lock().unwrap().contains_key(&topic) {
            let mut messages = self.cache.lock().unwrap().get(&topic).unwrap().clone();
            let index = messages.iter().position(|x| x.id == id);

            if let Some(index) = index {
                messages.remove(index);
                self.cache.lock().unwrap().insert(topic.clone(), messages);

                Ok(tonic::Response::new(pb::AckResponse {
                    id: id,
                    topic: topic,
                    ..Default::default()
                }))
            } else {
                Err(tonic::Status::not_found("Message not found"))
            }
        } else {
            Err(tonic::Status::not_found("Topic not found"))
        }
    }

    async fn publish(
        &self,
        request: tonic::Request<pb::PublishRequest>,
    ) -> Result<tonic::Response<pb::PublishResponse>, tonic::Status> {
        let topic = request.get_ref().topic.clone();

        if self.cache.lock().unwrap().contains_key(&topic) {
            let id = Uuid::now_v7().to_string();
            let mut messages = self.cache.lock().unwrap().get(&topic).unwrap();
            let message = pb::SubscriptionResponse {
                id: id.clone(),
                topic: topic.clone(),
                data: request.get_ref().data.clone(),
                ..Default::default()
            };

            messages.push(message);

            self.cache
                .lock()
                .unwrap()
                .insert(topic.clone(), messages.clone());

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
            self.cache
                .lock()
                .unwrap()
                .insert(topic.clone(), vec![message]);

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
            .lock()
            .unwrap()
            .get(&request.get_ref().topic)
            .unwrap_or(vec![])
            .clone();
        let mut stream =
            Box::pin(tokio_stream::iter(repeat.clone()).throttle(Duration::from_millis(200)));

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(Ok(item.clone())).await {
                    Ok(_) => {}
                    Err(_item) => {
                        println!("Error sending message");
                        break;
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

    let cache = Arc::new(Mutex::new(Cache::new(10_000)));

    Server::builder()
        .add_service(pb::rhino_server::RhinoServer::new(RhinoServer { cache }))
        .serve(([127, 0, 0, 1], 50051).into())
        .await?;

    Ok(())
}
