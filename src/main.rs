#![allow(non_snake_case)]
use dotenvy::dotenv;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Row};
use std::env;
use std::str::FromStr;
use uuid::Uuid;

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

  let content = sqlx::query_as::<_, Content>("SELECT * FROM djmdContent WHERE FileNameL like ?")
    .bind(format!("%[{}]%", "918205852"))
    .fetch_one(&mut conn).await?;
  println!("{:?}", content);

  sqlx::query("UPDATE agentRegistry SET int_1 = int_1 + 1 WHERE registry_id = 'localUpdateCount'")
    .execute(&mut conn).await?;
  let usn = sqlx::query("SELECT int_1 from agentRegistry WHERE registry_id = 'localUpdateCount'")
    .fetch_one(&mut conn).await?;
  let next_usn: i64 = usn.try_get("int_1")?;
  println!("next usn {}", next_usn);
  // let next_usn = sqlx::query_as::<_, String>("SELECT int_1 from agentRegistry WHERE registry_id = 'localUpdateCount'").fetch_one(&mut conn).await?;

  let insert = r#"
      WITH
        tag AS (SELECT ID, ParentID FROM djmdMyTag WHERE name = ?)
      INSERT INTO djmdSongMyTag (ID, MyTagID, ContentID, UUID, rb_local_usn, created_at, updated_at)
        SELECT ?, tag.ID, ?, ?, ?, datetime(), datetime()
        FROM tag
      WHERE NOT EXISTS(SELECT * FROM djmdSongMyTag WHERE MyTagID = tag.ID AND ContentID = ?)
  "#;
  let tags = sqlx::query(insert)
    .bind("edrive")
    .bind(Uuid::new_v4().to_string())
    .bind(&content.ID)
    .bind(Uuid::new_v4().to_string())
    .bind(next_usn)
    .bind(&content.ID)
    .execute(&mut conn).await?;

  Ok(())
}
