use anyhow::Result;
use sqlx::MySqlPool;
use tonic::{Request, Response, Status};
use tracing::{error, info};

use crate::{AuthRequest, AuthResponse};

pub async fn authenticate(
    req: Request<AuthRequest>,
    db_pool: &MySqlPool,
) -> Result<Response<AuthResponse>, Status> {
    let token = req.into_inner().token;

    match get_uuid_from_db(db_pool, &token).await {
        Ok(Some(_uuid)) => Ok(Response::new(AuthResponse { success: true })),
        Ok(None) => Ok(Response::new(AuthResponse { success: false })),
        Err(_) => Err(Status::internal("Internal server error")),
    }
}

async fn get_uuid_from_db(db_pool: &MySqlPool, token: &str) -> Result<Option<String>> {
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
