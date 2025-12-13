#![allow(non_snake_case)]
use dotenvy::dotenv;
use sqlx::{sqlite::SqliteConnectOptions, ConnectOptions, Row, SqliteConnection};
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

  let exists = tag_exists(&mut conn, &content, "vocals").await?;
  if !exists {
    increment_usn(&mut conn).await?;
    let next_usn = next_usn(&mut conn).await?;
    println!("next usn {}", next_usn);
    insert_tag(&mut conn, &content, next_usn, "vocals").await?;
  }

  Ok(())
}

async fn insert_tag(conn: &mut SqliteConnection, content: &Content, next_usn: i64, name: &str) -> anyhow::Result<()> {
  let insert = r#"
      WITH
        tag AS (SELECT ID, ParentID FROM djmdMyTag WHERE name = ?)
      INSERT INTO djmdSongMyTag (ID, MyTagID, ContentID, UUID, rb_local_usn, created_at, updated_at)
        SELECT ?, tag.ID, ?, ?, ?, datetime(), datetime()
        FROM tag
  "#;
  sqlx::query(insert)
    .bind(name)
    .bind(Uuid::new_v4().to_string())
    .bind(&content.ID)
    .bind(Uuid::new_v4().to_string())
    .bind(next_usn)
    .execute(conn).await?;
  Ok(())
}

async fn tag_exists(conn: &mut SqliteConnection, content: &Content, name: &str) -> anyhow::Result<bool> {
  let exists = r#"
      SELECT EXISTS(SELECT * FROM djmdSongMyTag AS st, djmdMyTag as t WHERE st.MyTagID = t.ID AND t.Name = ? AND ContentID = ?)
  "#;
  let exists: bool = sqlx::query_scalar(exists)
    .bind(name)
    .bind(&content.ID)
    .fetch_one(conn).await?;
  Ok(exists)
}

async fn next_usn(conn: &mut SqliteConnection) -> anyhow::Result<i64> {
  let usn = sqlx::query("SELECT int_1 from agentRegistry WHERE registry_id = 'localUpdateCount'")
    .fetch_one(conn).await?;
  let next_usn: i64 = usn.try_get("int_1")?;
  Ok(next_usn)
}

async fn increment_usn(conn: &mut SqliteConnection) -> anyhow::Result<()> {
  sqlx::query("UPDATE agentRegistry SET int_1 = int_1 + 1 WHERE registry_id = 'localUpdateCount'")
    .execute(conn).await?;
  Ok(())
}
