use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, unbounded, Sender};
use ndarray::Array4;
use tokio::sync::oneshot;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info};

use md5rs::md5rs_server::{Md5rs, Md5rsServer};
use md5rs::{AuthRequest, AuthResponse, Bbox, DetectRequest, DetectResponse};

pub mod md5rs {
    tonic::include_proto!("md5rs");
}

mod auth;
mod detect;
mod log;

struct DecodeTask {
    uuid: String,
    image_data: Vec<u8>,
    width: i32,
    height: i32,
    iou: f32,
    score: f32,
    response_sender: oneshot::Sender<DetectResponse>,
}

pub struct DetectTask {
    uuid: String,
    image_array: Array4<f32>,
    ratio: f32,
    width: i32,
    height: i32,
    pad: i32, // minus is pad height
    iou: f32,
    score: f32,
    response_sender: oneshot::Sender<DetectResponse>,
}

#[derive(Debug)]
pub struct Md5rsService {
    sender: Sender<DecodeTask>,
    db: sqlx::MySqlPool,
}

#[tonic::async_trait]
impl Md5rs for Md5rsService {
    type DetectStream =
        Pin<Box<dyn Stream<Item = Result<DetectResponse, Status>> + Send + 'static>>;

    async fn detect(
        &self,
        request: Request<tonic::Streaming<DetectRequest>>,
    ) -> Result<Response<Self::DetectStream>, Status> {
        let mut stream = request.into_inner();

        let sender = self.sender.clone();

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                match req {
                    Ok(detect_request) => {
                        let (task_response_sender, task_response_receiver) = oneshot::channel();

                        let uuid = detect_request.uuid.clone();
                        let image_data = detect_request.image.clone();
                        let iou = detect_request.iou;
                        let score = detect_request.score;
                        let width = detect_request.width;
                        let height = detect_request.height;

                        let task = DecodeTask {
                            uuid,
                            image_data,
                            width,
                            height,
                            iou,
                            score,
                            response_sender: task_response_sender,
                        };

                        if sender.send(task).is_err() {
                            error!("Failed to send inference task");
                            continue;
                        }

                        let response_tx = response_tx.clone();
                        tokio::spawn(async move {
                            if let Ok(response) = task_response_receiver.await {
                                let _ = response_tx.send(Ok(response)).await;
                            }
                        });
                    }
                    Err(e) => {
                        error!("Error receiving request: {:?}", e);
                        break;
                    }
                }
            }
        });

        let response_stream = ReceiverStream::new(response_rx);

        Ok(Response::new(
            Box::pin(response_stream) as Self::DetectStream
        ))
    }

    async fn auth(&self, request: Request<AuthRequest>) -> Result<Response<AuthResponse>, Status> {
        auth::authenticate(request, &self.db).await
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Log level
    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    log_level: LogLevel,

    /// Log file
    #[arg(long, default_value = "md5rs-server.log")]
    log_file: String,

    /// decode worker threads
    #[arg(long, default_value = "32")]
    decode_workers: usize,

    /// detect worker threads
    #[arg(long, default_value = "4")]
    detect_workers: usize,

    /// gRPC server host
    #[arg(long, default_value = "0.0.0.0:50051")]
    host: String,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "kebab-case")]
enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl LogLevel {
    fn to_string(&self) -> String {
        match self {
            LogLevel::Trace => "trace",
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
        }
        .to_string()
    }
}

#[tokio::main]
async fn run(args: Args) -> Result<()> {
    let (decode_q_s, decode_q_r) = unbounded::<DecodeTask>();

    let (detect_q_s, detect_q_r) = bounded::<DetectTask>(8);

    let decode_q_r = Arc::new(decode_q_r);
    let detect_q_s = Arc::new(detect_q_s);

    let detect_q_r = Arc::new(detect_q_r);

    // Create worker threads
    let decode_workers = args.decode_workers;
    let detect_workers = args.detect_workers;

    for _ in 0..decode_workers {
        let r = Arc::clone(&decode_q_r);
        let s = Arc::clone(&detect_q_s);
        detect::decode_worker(r, s);
        // detect::detect_worker(receiver);
    }

    for _ in 0..detect_workers {
        let r = Arc::clone(&detect_q_r);
        detect::detect_worker(r);
    }

    // Start the gRPC server
    let addr = args.host.parse()?;

    let db_pool =
        sqlx::MySqlPool::connect("mysql://shanshui:cat2022%40ShanShui@localhost:33306/cat").await?;

    let svc = Md5rsServer::new(Md5rsService {
        sender: decode_q_s,
        db: db_pool,
    });

    info!("Server started at {}", args.host);

    Server::builder().add_service(svc).serve(addr).await?;

    Ok(())
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let guard = log::init_logger(args.log_level.clone(), args.log_file.clone())?;

    run(args)?;

    drop(guard);

    Ok(())
}
