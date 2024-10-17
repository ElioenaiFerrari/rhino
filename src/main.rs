use std::{pin::Pin, time::Duration};

use dotenv::dotenv;
use pb::{rhino_server::Rhino, SubscriptionResponse};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{self, transport::Server};

pub mod pb {
    use tonic::include_proto;

    include_proto!("rhino");
}

#[derive(Default)]
struct RhinoServer {}

#[tonic::async_trait]
impl Rhino for RhinoServer {
    async fn publish(
        &self,
        request: tonic::Request<pb::PublishRequest>,
    ) -> Result<tonic::Response<pb::PublishResponse>, tonic::Status> {
        println!("Got a request: {:?}", request);
        Ok(tonic::Response::new(pb::PublishResponse {
            ..Default::default()
        }))
    }

    type SubscribeStream =
        Pin<Box<dyn Stream<Item = Result<pb::SubscriptionResponse, tonic::Status>> + Send>>;

    async fn subscribe(
        &self,
        request: tonic::Request<pb::SubscriptionRequest>,
    ) -> Result<tonic::Response<Self::SubscribeStream>, tonic::Status> {
        let repeat = std::iter::repeat(Ok(pb::SubscriptionResponse {
            ..Default::default()
        }));
        let mut stream = Box::pin(tokio_stream::iter(repeat).throttle(Duration::from_millis(200)));

        let (tx, rx) = mpsc::channel(128);
        tokio::spawn(async move {
            while let Some(item) = stream.next().await {
                match tx.send(item).await {
                    Ok(_) => {
                        // item (server response) was queued to be send to client
                    }
                    Err(_item) => {
                        // output_stream was build from rx and both are dropped
                        break;
                    }
                }
            }
            println!("\tclient disconnected");
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

    Server::builder()
        .add_service(pb::rhino_server::RhinoServer::new(RhinoServer::default()))
        .serve(([127, 0, 0, 1], 50051).into())
        .await?;

    Ok(())
}
