use anyhow::Result;
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use rand::Rng;
use redis::AsyncCommands;
use sqlx::MySqlPool;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::{AuthRequest, AuthResponse};

pub async fn authenticate(
    req: Request<AuthRequest>,
    db_pool: &MySqlPool,
    redis_client: &mut redis::aio::MultiplexedConnection,
    expire: u64,
) -> Result<Response<AuthResponse>, Status> {
    let token = req.into_inner().token;

    match get_user_from_db(db_pool, &token).await {
        Ok(Some(uuid)) => {
            let session_token = set_token(redis_client, &uuid, expire).await.map_err(|e| {
                error!("Error setting token in Redis: {:?}", e);
                Status::internal("Internal server error")
            })?;
            Ok(Response::new(AuthResponse {
                success: true,
                token: session_token,
            }))
        }
        Ok(None) => Ok(Response::new(AuthResponse {
            success: false,
            token: "".to_string(),
        })),
        Err(_) => Err(Status::internal("Internal server error")),
    }
}

async fn get_user_from_db(db_pool: &MySqlPool, token: &str) -> Result<Option<String>> {
    info!("Querying database for token: {}", token);

    let row: sqlx::Result<(String,)> = sqlx::query_as("SELECT username FROM user WHERE token = ?")
        .bind(token)
        .fetch_one(db_pool)
        .await;

    match row {
        Ok((uuid,)) => Ok(Some(uuid)),
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(e) => {
            error!("Error querying database: {:?}", e);
            Err(e.into())
        }
    }
}

pub fn generate_bearer_token() -> String {
    // Generate 32 random bytes
    let random_bytes: [u8; 32] = rand::thread_rng().gen();

    // Encode the bytes to a Base64 string
    let token = URL_SAFE.encode(&random_bytes);

    // Prepend "Bearer " to the token
    format!("Bearer {}", token)
}

pub async fn set_token(
    client: &mut redis::aio::MultiplexedConnection,
    username: &str,
    expire: u64,
) -> redis::RedisResult<String> {
    let token = generate_bearer_token();
    let _: () = client.set_ex(token.clone(), username, expire).await?;
    Ok(token)
}

pub async fn get_token(
    client: &mut redis::aio::MultiplexedConnection,
    token: &str,
) -> Result<Option<String>> {
    let result: Option<String> = redis::cmd("GET").arg(token).query_async(client).await?;
    Ok(result)
}
