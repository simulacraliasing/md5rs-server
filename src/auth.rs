use anyhow::Result;
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use rand::Rng;
use redis::AsyncCommands;
use sqlx::MySqlPool;
use tonic::{Request, Response, Status};
use tracing::{debug, error, info};

use crate::{AuthRequest, AuthResponse};

#[derive(Debug)]
pub struct User {
    pub username: String,
    pub quota: i32,
    pub quota_used: i32,
}

impl User {
    pub fn new(username: String, quota: i32, quota_used: i32) -> Self {
        User {
            username,
            quota,
            quota_used,
        }
    }

    pub fn remaining_quota(&self) -> i32 {
        if self.quota < 0 {
            -1
        } else {
            self.quota - self.quota_used
        }
    }
}

pub async fn authenticate(
    req: Request<AuthRequest>,
    db_pool: &MySqlPool,
    redis_client: &mut redis::aio::MultiplexedConnection,
    expire: u64,
) -> Result<Response<AuthResponse>, Status> {
    let token = req.into_inner().token;

    match get_user_from_db(db_pool, &token).await {
        Ok(Some(user)) => {
            let session_token = set_token(redis_client, &user.username, expire)
                .await
                .map_err(|e| {
                    error!("Error setting token in Redis: {:?}", e);
                    Status::internal("Internal server error")
                })?;

            set_quota(redis_client, &user).await.map_err(|e| {
                error!("Error setting quota in Redis: {:?}", e);
                Status::internal("Internal server error")
            })?;
            debug!("Authenticated user: {}", user.username);
            Ok(Response::new(AuthResponse {
                success: true,
                token: session_token,
                quota: user.quota - user.quota_used,
            }))
        }
        Ok(None) => {
            debug!("User not found");
            Ok(Response::new(AuthResponse {
                success: false,
                token: "".to_string(),
                quota: 0,
            }))
        }
        Err(_) => Err(Status::internal("Internal server error")),
    }
}

async fn get_user_from_db(db_pool: &MySqlPool, token: &str) -> Result<Option<User>> {
    info!("Querying database for token: {}", token);

    let row: sqlx::Result<(String, i32, i32)> =
        sqlx::query_as("SELECT username, COALESCE(dayLimitNo, -1), COALESCE(dayUploadedNo, -1) FROM user WHERE token = ?")
            .bind(token)
            .fetch_one(db_pool)
            .await;

    match row {
        Ok((username, quota, quota_used)) => {
            Ok(Some(User::new(username, quota * 5, quota_used * 5)))
        }
        Err(sqlx::Error::RowNotFound) => Ok(None),
        Err(e) => {
            error!("Error querying database: {:?}", e);
            Err(e.into())
        }
    }
}

pub fn generate_bearer_token() -> String {
    // Generate 32 random bytes
    let random_bytes: [u8; 32] = rand::rng().random();

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

pub async fn set_quota(
    client: &mut redis::aio::MultiplexedConnection,
    user: &User,
) -> redis::RedisResult<()> {
    let items = vec![("quota", user.quota), ("quota_used", user.quota_used)];
    let _: () = client.hset_multiple(user.username.clone(), &items).await?;
    let _: () = client.expire(user.username.clone(), 86400).await?;
    Ok(())
}

pub async fn get_quota(
    client: &mut redis::aio::MultiplexedConnection,
    username: &str,
) -> Result<Option<User>> {
    let result: Option<(i32, i32)> = client.hget(username, &["quota", "quota_used"]).await?;
    if let Some((quota, quota_used)) = result {
        Ok(Some(User::new(username.to_string(), quota, quota_used)))
    } else {
        Ok(None)
    }
}

pub async fn update_quota(db_pool: &MySqlPool, user: &User) -> Result<()> {
    let _ = sqlx::query("UPDATE user SET dayUploadedNo = ? WHERE username = ?")
        .bind(user.quota_used / 5)
        .bind(user.username.clone())
        .execute(db_pool)
        .await?;
    Ok(())
}
