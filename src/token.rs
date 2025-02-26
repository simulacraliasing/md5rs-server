use anyhow::Result;
use base64::{engine::general_purpose::URL_SAFE, Engine as _};
use clap::{Args, Parser, Subcommand};
use rand::Rng;
use sqlx::{query, query_as, MySqlPool};

async fn create_user_table(db_pool: &MySqlPool) -> Result<()> {
    query(
        r#"
        CREATE TABLE IF NOT EXISTS user (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL UNIQUE,
            token VARCHAR(255)
        )
        "#,
    )
    .execute(db_pool)
    .await?;

    Ok(())
}

async fn create_user(username: &str, db_pool: &MySqlPool) -> Result<()> {
    query("INSERT INTO user (username) VALUES (?)")
        .bind(username)
        .execute(db_pool)
        .await?;

    Ok(())
}

pub fn generate_bearer_token() -> String {
    // Generate 32 random bytes
    let random_bytes: [u8; 32] = rand::rng().random();

    // Encode the bytes to a Base64 string
    URL_SAFE.encode(&random_bytes)
}

async fn register_token(username: &str, db_pool: &MySqlPool) -> Result<String> {
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
        anyhow::bail!("User does not exist");
    }
}

#[derive(Parser, Debug)]
#[clap(name = "md5rs-server", version, about, long_about = None)]
struct App {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init,
    Register(RegisterArgs),
    GenerateToken(GenerateTokenArgs),
}

#[derive(Debug, Args)]
struct RegisterArgs {
    username: String,
}

#[derive(Debug, Args)]
struct GenerateTokenArgs {
    username: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let database_url = std::env::var("MYSQL_URL").expect("MYSQL_URL env var is not set");
    let db_pool = MySqlPool::connect(&database_url)
        .await
        .expect("Failed to connect to the database");

    let args = App::parse();

    match args.command {
        Command::Init => {
            create_user_table(&db_pool).await?;
            Ok(())
            // Initialize the database
        }
        Command::Register(RegisterArgs { username }) => {
            create_user(&username, &db_pool).await?;
            Ok(())
        }
        Command::GenerateToken(GenerateTokenArgs { username }) => {
            match register_token(&username, &db_pool).await {
                Ok(token) => {
                    println!("Generated and registered Bearer Token: {}", token);
                    Ok(())
                }
                Err(err) => {
                    eprintln!("Error registering token: {:?}", err);
                    Ok(())
                }
            }
        }
    }
}
