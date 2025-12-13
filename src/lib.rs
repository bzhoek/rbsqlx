#![allow(non_snake_case)]
use dotenvy::dotenv;
use sqlx::sqlite::{SqlitePoolOptions, SqliteQueryResult};
use sqlx::{Error, Row, SqlitePool};
use std::env;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
#[allow(unused)]
pub struct Content {
  ID: String,
  FileNameL: String,
  pub Rating: i64,
}

#[derive(Clone)]
pub struct Database {
  pool: SqlitePool,
}

impl Database {
  pub async fn connect(url: &str) -> anyhow::Result<Self> {
    dotenv().ok();

    let pool: SqlitePool = SqlitePoolOptions::new()
      .max_connections(5)
      .after_connect(|conn, _meta| {
        let pragma = format!("PRAGMA key = '{}';", env::var("SQLCIPHER_KEY") // key must be SQL 'quoted'
          .expect("SQLCIPHER_KEY must be set"));
        Box::pin(async move {
          sqlx::query(&pragma)
            .execute(conn)
            .await?;
          Ok(())
        })
      })
      .connect(url)
      .await?;

    Ok(Self { pool })
  }

  pub async fn content(&mut self, id: &str) -> Result<Content, Error> {
    sqlx::query_as::<_, Content>("SELECT * FROM djmdContent WHERE FileNameL like ?")
      .bind(format!("%[{}]%", id))
      .fetch_one(&self.pool).await
  }

  pub async fn tag_content(&mut self, content: &Content, tag: &str) -> anyhow::Result<()> {
    if !self.tag_exists(content, tag).await? {
      self.increment_usn().await?;
      let next_usn = self.next_usn().await?;
      println!("next usn {} for {}", next_usn, tag);
      self.insert_tag(content, next_usn, tag).await?;
    }
    Ok(())
  }

  async fn tag_exists(&mut self, content: &Content, tag: &str) -> Result<bool, Error> {
    let exists = r#"
      SELECT EXISTS(SELECT * FROM djmdSongMyTag AS st, djmdMyTag as t WHERE st.MyTagID = t.ID AND t.Name = ? AND ContentID = ?)
  "#;
    sqlx::query_scalar(exists)
      .bind(tag)
      .bind(&content.ID)
      .fetch_one(&self.pool).await
  }

  async fn insert_tag(&mut self, content: &Content, next_usn: i64, tag: &str) -> Result<SqliteQueryResult, Error> {
    let insert = r#"
      WITH
        tag AS (SELECT ID, ParentID FROM djmdMyTag WHERE name = ?)
      INSERT INTO djmdSongMyTag (ID, MyTagID, ContentID, UUID, rb_local_usn, created_at, updated_at)
        SELECT ?, tag.ID, ?, ?, ?, datetime(), datetime()
        FROM tag
  "#;
    sqlx::query(insert)
      .bind(tag)
      .bind(Uuid::new_v4().to_string())
      .bind(&content.ID)
      .bind(Uuid::new_v4().to_string())
      .bind(next_usn)
      .execute(&self.pool).await
  }

  async fn increment_usn(&mut self) -> Result<SqliteQueryResult, Error> {
    sqlx::query("UPDATE agentRegistry SET int_1 = int_1 + 1 WHERE registry_id = 'localUpdateCount'")
      .execute(&self.pool).await
  }
  async fn next_usn(&mut self) -> Result<i64, Error> {
    let usn = sqlx::query("SELECT int_1 from agentRegistry WHERE registry_id = 'localUpdateCount'")
      .fetch_one(&self.pool).await?;
    usn.try_get("int_1")
  }
}
