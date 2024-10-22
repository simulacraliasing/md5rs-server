use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use rand::Rng;
use sqlx::{query, query_as, Error, MySqlPool};

fn generate_bearer_token() -> String {
    // Generate 32 random bytes
    let random_bytes: [u8; 32] = rand::thread_rng().gen();

    // Encode the bytes to a Base64 string
    let token = URL_SAFE.encode(&random_bytes);

    // Prepend "Bearer " to the token
    format!("Bearer {}", token)
}

async fn register_token(username: &str, db_pool: &MySqlPool) -> Result<String, Error> {
    // Generate a new token
    let token = generate_bearer_token();

    // Start a transaction
    let mut transaction = db_pool.begin().await?;

    // Check if the username exists
    let user_exists: (bool,) = query_as("SELECT EXISTS(SELECT 1 FROM user WHERE username = ?)")
        .bind(username)
        .fetch_one(db_pool)
        .await?;

    if user_exists.0 {
        println!("User exists");
        // Update the token for the existing user
        query("UPDATE user SET token = ? WHERE username = ?")
            .bind(&token)
            .bind(username)
            .execute(&mut *transaction)
            .await?;

        // Commit the transaction
        transaction.commit().await?;

        Ok(token)
    } else {
        // Rollback the transaction if user does not exist
        transaction.rollback().await?;
        Err(Error::RowNotFound)
    }
}

#[tokio::main]
async fn main() {
    // Create a MySQL connection pool
    let database_url = "mysql://shanshui:cat2022%40ShanShui@localhost:33306/cat";
    let db_pool = MySqlPool::connect(database_url)
        .await
        .expect("Failed to connect to the database");

    // Example usage
    let username = "superadmin";
    match register_token(username, &db_pool).await {
        Ok(token) => println!("Generated and registered Bearer Token: {}", token),
        Err(err) => eprintln!("Error registering token: {:?}", err),
    }
}
