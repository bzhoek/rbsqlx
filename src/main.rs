#![allow(non_snake_case)]
use dotenvy::dotenv;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions};
use std::env;
use std::str::FromStr;

#[derive(Debug, sqlx::FromRow)]
#[allow(unused)]
struct Content {
  ID: String,
  FileNameL: String,
  Rating: i64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  connect().await?;
  Ok(())
}

async fn connect() -> anyhow::Result<()> {
  dotenv().ok();
  let key = format!("'{}'", env::var("SQLCIPHER_KEY") // key must be SQL 'quoted'
    .expect("SQLCIPHER_KEY must be set"));

  let mut conn =
    SqliteConnectOptions::from_str("encrypted.db")?
      .pragma("key", key)
      .connect().await?;

  let row = sqlx::query_as::<_, Content>("SELECT * FROM djmdContent WHERE FileNameL like ?")
    .bind(format!("%[{}]%", "918205852"))
    .fetch_one(&mut conn).await?;

  println!("{:?}", row);
  Ok(())
}
