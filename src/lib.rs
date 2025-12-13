#![allow(non_snake_case)]
use std::env;
use std::str::FromStr;
use dotenvy::dotenv;
use sqlx::sqlite::{SqliteConnectOptions, SqliteQueryResult};
use sqlx::{ConnectOptions, Error, Row, SqliteConnection};
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
#[allow(unused)]
pub struct Content {
  ID: String,
  FileNameL: String,
  Rating: i64,
}

pub struct Database {
  conn: SqliteConnection,
}

impl Database {
  pub async fn connect() -> anyhow::Result<Self> {
    dotenv().ok();
    let key = format!("'{}'", env::var("SQLCIPHER_KEY") // key must be SQL 'quoted'
      .expect("SQLCIPHER_KEY must be set"));

    let conn =
      SqliteConnectOptions::from_str("encrypted.db")?
        .pragma("key", key)
        .connect().await?;
    Ok(Self { conn })
  }

  pub async fn content(&mut self, id: &str) -> Result<Content, Error> {
    sqlx::query_as::<_, Content>("SELECT * FROM djmdContent WHERE FileNameL like ?")
      .bind(format!("%[{}]%", id))
      .fetch_one(&mut self.conn).await
  }

  pub async fn tag_content(&mut self, content: &Content, name: &str) -> anyhow::Result<()> {
    if !self.tag_exists(content, name).await? {
      self.increment_usn().await?;
      let next_usn = self.next_usn().await?;
      println!("next usn {}", next_usn);
      self.insert_tag(content, next_usn, name).await?;
    }
    Ok(())
  }

  async fn tag_exists(&mut self, content: &Content, name: &str) -> Result<bool, Error> {
    let exists = r#"
      SELECT EXISTS(SELECT * FROM djmdSongMyTag AS st, djmdMyTag as t WHERE st.MyTagID = t.ID AND t.Name = ? AND ContentID = ?)
  "#;
    sqlx::query_scalar(exists)
      .bind(name)
      .bind(&content.ID)
      .fetch_one(&mut self.conn).await
  }

  async fn insert_tag(&mut self, content: &Content, next_usn: i64, name: &str) -> Result<SqliteQueryResult, Error> {
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
      .execute(&mut self.conn).await
  }

  async fn increment_usn(&mut self) -> Result<SqliteQueryResult, Error> {
    sqlx::query("UPDATE agentRegistry SET int_1 = int_1 + 1 WHERE registry_id = 'localUpdateCount'")
      .execute(&mut self.conn).await
  }
  async fn next_usn(&mut self) -> Result<i64, Error> {
    let usn = sqlx::query("SELECT int_1 from agentRegistry WHERE registry_id = 'localUpdateCount'")
      .fetch_one(&mut self.conn).await?;
    usn.try_get("int_1")
  }

}
