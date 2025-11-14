use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use clap::{Parser, ValueEnum};
use crossbeam_channel::{bounded, Sender};
use ndarray::Array4;
use tokio::sync::{oneshot, Mutex};
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{debug, error, info};

use md5rs::md5rs_server::{Md5rs, Md5rsServer};
use md5rs::{
    AuthRequest, AuthResponse, Bbox, DetectRequest, DetectResponse, HealthRequest, HealthResponse,
};

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
    iframe: bool,
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
    iframe: bool,
    response_sender: oneshot::Sender<DetectResponse>,
}

#[derive(Debug, Clone)]
pub struct Md5rsService {
    sender: Sender<DecodeTask>,
    db: sqlx::MySqlPool,
    redis: redis::Client,
}

#[tonic::async_trait]
impl Md5rs for Md5rsService {
    type DetectStream =
        Pin<Box<dyn Stream<Item = Result<DetectResponse, Status>> + Send + 'static>>;

    async fn detect(
        &self,
        request: Request<tonic::Streaming<DetectRequest>>,
    ) -> Result<Response<Self::DetectStream>, Status> {
        let username;

        let quota_sync_count = Arc::new(Mutex::new(0));

        let mut redis_conn = self.redis.get_multiplexed_tokio_connection().await.unwrap();

        let db_pool = self.db.clone();

        match request.metadata().get("authorization") {
            Some(token) => {
                let token = token.to_str().unwrap();
                // let mut redis_conn = self.redis.get_multiplexed_tokio_connection().await.unwrap();
                match auth::get_token(&mut redis_conn, token).await {
                    Ok(Some(username_)) => {
                        username = username_;
                    }
                    Ok(None) => {
                        return Err(Status::unauthenticated("Invalid token"));
                    }
                    Err(_) => {
                        return Err(Status::internal("Internal server error"));
                    }
                }
            }
            None => {
                return Err(Status::unauthenticated("No token provided"));
            }
        }
        let mut stream = request.into_inner();

        let sender = self.sender.clone();

        let (response_tx, response_rx) = tokio::sync::mpsc::channel(4);

        tokio::spawn(async move {
            while let Some(req) = stream.next().await {
                match req {
                    Ok(detect_request) => {
                        // consume quota
                        let mut user = auth::get_quota(&mut redis_conn, &username)
                            .await
                            .unwrap()
                            .unwrap();

                        let remaining_quota = user.remaining_quota();

                        if remaining_quota == 0 {
                            auth::update_quota(&db_pool, &user).await.unwrap();
                            let _ = response_tx
                                .send(Err(Status::resource_exhausted("Quota exhausted")))
                                .await;
                            break;
                        } else if remaining_quota > 0 {
                            user.quota_used += 1;
                            auth::set_quota(&mut redis_conn, &user).await.unwrap();
                            let mut lock = quota_sync_count.lock().await;
                            *lock += 1;
                            if *lock % 100 == 0 {
                                auth::update_quota(&db_pool, &user).await.unwrap();
                            }
                        }

                        let (task_response_sender, task_response_receiver) = oneshot::channel();

                        let uuid = detect_request.uuid.clone();
                        let image_data = detect_request.image.clone();
                        let iou = detect_request.iou;
                        let score = detect_request.score;
                        let width = detect_request.width;
                        let height = detect_request.height;
                        let iframe = detect_request.iframe;

                        let task = DecodeTask {
                            uuid,
                            image_data,
                            width,
                            height,
                            iou,
                            score,
                            iframe,
                            response_sender: task_response_sender,
                        };

                        let sender_clone = sender.clone();

                        if let Err(e) =
                            tokio::task::spawn_blocking(move || sender_clone.send(task)).await
                        {
                            error!("Failed to queue decode task: {:?}", e);
                            break;
                        }

                        let response_tx_clone = response_tx.clone();
                        tokio::spawn(async move {
                            match task_response_receiver.await {
                                Ok(response) => {
                                    if response_tx_clone.send(Ok(response)).await.is_err() {
                                        // Downstream receiver has been dropped, client disconnected.
                                        debug!("Client disconnected, cannot send response.");
                                    }
                                }
                                Err(_) => {
                                    // The oneshot sender was dropped, likely because the worker thread panicked.
                                    error!("Decode/Detect worker may have panicked. Task dropped.");
                                }
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
        let mut redis_conn = self.redis.get_multiplexed_tokio_connection().await.unwrap();
        auth::authenticate(request, &self.db, &mut redis_conn, 604_800).await
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        debug!("Health check");
        Ok(Response::new(HealthResponse { status: true }))
    }
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// model path
    #[arg(long, short, default_value = "models/md_v5a_d_pp_fp16.onnx")]
    model: String,

    /// device
    #[arg(long, short, default_value = "0")]
    device: Vec<i32>,

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
    let (decode_q_s, decode_q_r) = bounded::<DecodeTask>(args.decode_workers * 2);

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

    for d in args.device {
        for _ in 0..detect_workers {
            let model = args.model.clone();
            let r = Arc::clone(&detect_q_r);
            detect::detect_worker(model, d, r);
        }
    }

    let sql_url = std::env::var("MYSQL_URL").expect("MYSQL_URL env var is not set");

    let redis_url = std::env::var("REDIS_URL").expect("REDIS_URL env var is not set");

    // Start the gRPC server
    let addr = args.host.parse()?;

    let db_pool = sqlx::MySqlPool::connect(&sql_url).await?;

    let redis_client = redis::Client::open(redis_url)?;

    let svc = Md5rsServer::new(Md5rsService {
        sender: decode_q_s,
        db: db_pool,
        redis: redis_client,
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
