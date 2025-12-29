#![allow(non_snake_case)]
use dotenvy::dotenv;
use sqlx::sqlite::{SqlitePoolOptions, SqliteQueryResult};
use sqlx::{Error, Row, SqlitePool};
use std::env;
use std::time::Duration;
use sqlx::types::chrono::Utc;
use tracing::debug;
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
#[allow(unused)]
pub struct Content {
  pub ID: String,
  pub FileNameL: String,
  pub Rating: i64,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(unused)]
pub struct Tag {
  ID: String,
  pub Name: String,
}

#[derive(Clone)]
pub struct Database {
  pool: SqlitePool,
}

impl Database {
  pub async fn connect(url: &str) -> anyhow::Result<Self> {
    dotenv().ok();

    let pool: SqlitePool = SqlitePoolOptions::new()
      .max_connections(6)
      .acquire_slow_threshold(Duration::from_secs(12))
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

  pub async fn filepath(&self, path: &str) -> Result<Content, Error> {
    sqlx::query_as::<_, Content>("SELECT * FROM djmdContent WHERE FolderPath like ?")
      .bind(format!("%{}", path))
      .fetch_one(&self.pool).await
  }

  pub async fn content(&self, id: &str) -> Result<Content, Error> {
    sqlx::query_as::<_, Content>("SELECT * FROM djmdContent WHERE FileNameL like ?")
      .bind(format!("%[{}]%", id))
      .fetch_one(&self.pool).await
  }

  pub async fn content_tags(&self, content: &Content) -> Result<Vec<Tag>, Error> {
    let sql = r#"
      SELECT st.ID, t.name FROM djmdSongMyTag AS st, djmdMyTag as t
      WHERE st.MyTagID = t.ID AND ContentID = ? ORDER by t.name
    "#;
    sqlx::query_as::<_, Tag>(sql)
      .bind(content.ID.clone())
      .fetch_all(&self.pool).await
  }

  pub async fn rate_content(&self, content: &Content, rating: u8) -> Result<SqliteQueryResult, Error> {
    sqlx::query("UPDATE djmdContent SET Rating = ? WHERE ID = ?")
      .bind(rating)
      .bind(&content.ID)
      .execute(&self.pool).await
  }

  async fn next_usn(&self) -> Result<i64, Error> {
    let sql = r#"
      UPDATE agentRegistry
      SET int_1 = int_1 + 1
      WHERE registry_id = 'localUpdateCount'
      RETURNING int_1;
    "#;
    let usn = sqlx::query(sql)
      .fetch_one(&self.pool).await?;
    usn.try_get("int_1")
  }

  async fn next_id(&self, table: &str) -> anyhow::Result<u32> {
    let sql = format!("SELECT COUNT(*) FROM {} WHERE ID = ?", table);
    let mut buf = [0u8; 4];
    loop {
      getrandom::getrandom(&mut buf).unwrap();
      let id: u32 = ((buf[0] as u32) << 24) + ((buf[1] as u32) << 16) + ((buf[2] as u32) << 8) + buf[3] as u32;
      if id < 100 { continue; }
      let (count, ): (i32,) = sqlx::query_as(&sql)
        .bind(id)
        .fetch_one(&self.pool).await?;
      if count == 0 {
        return Ok(id);
      }
    }
  }

  fn now_timestamp() -> String {
    let now_utc = Utc::now();
    now_utc.format("%Y-%m-%d %H:%M:%S%.3f %:z").to_string()
  }

  pub async fn checkpoint(&self) -> anyhow::Result<()> {
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE);")
      .execute(&self.pool)
      .await?;
    Ok(())
  }
}

impl Database {
  pub async fn clear_tags(&self, content: &Content) -> Result<SqliteQueryResult, Error> {
    let sql = r#"
      DELETE FROM djmdSongMyTag
      WHERE ID IN (
        SELECT st.ID
        FROM djmdSongMyTag AS st
        JOIN djmdMyTag AS t ON st.MyTagID = t.ID
        WHERE st.ContentID = ?
      );
      "#;
    sqlx::query(sql)
      .bind(&content.ID)
      .execute(&self.pool).await
  }

  pub async fn tag_content(&self, content: &Content, tag: &str) -> anyhow::Result<Option<i64>> {
    if !self.tag_exists(content, tag).await? {
      let next_usn = self.next_usn().await?;
      debug!("{} for {:?} usn {}", tag, content, next_usn);
      self.insert_tag(content, next_usn, tag).await?;
      return Ok(Some(next_usn));
    }
    Ok(None)
  }

  async fn tag_exists(&self, content: &Content, tag: &str) -> Result<bool, Error> {
    let exists = r#"
      SELECT EXISTS (
        SELECT * FROM djmdSongMyTag AS st, djmdMyTag as t
        WHERE st.MyTagID = t.ID AND t.Name = ? AND ContentID = ?)
  "#;
    sqlx::query_scalar(exists)
      .bind(tag)
      .bind(&content.ID)
      .fetch_one(&self.pool).await
  }

  async fn insert_tag(&self, content: &Content, next_usn: i64, tag: &str) -> Result<SqliteQueryResult, Error> {
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

  pub async fn untag_content(&self, content: &Content, tag: &str) -> Result<SqliteQueryResult, Error> {
    let sql = r#"
      DELETE FROM djmdSongMyTag
      WHERE ID IN (
        SELECT st.ID
        FROM djmdSongMyTag AS st
        JOIN djmdMyTag AS t ON st.MyTagID = t.ID
        WHERE st.ContentID = ?
          AND t.name = ?
      );
      "#;
    sqlx::query(sql)
      .bind(&content.ID)
      .bind(tag)
      .execute(&self.pool).await
  }
}

impl Database {
  pub async fn playlist_create(&self, name: &str) -> anyhow::Result<()> {
    let next_id = self.next_id("djmdPlaylist").await?;
    let next_usn = self.next_usn().await?;
    let timestamp = Self::now_timestamp();
    let sql = r#"
      INSERT INTO djmdPlaylist (Seq, ID, Name, Attribute, ParentID, UUID, rb_local_usn, created_at, updated_at)
      SELECT
        (SELECT MAX(Seq) + 1 FROM djmdPlaylist WHERE ParentID = 'root'),
        ?, ?, 0, 'root', ?, ?, ?, ?
      WHERE NOT EXISTS(SELECT ID FROM djmdPlaylist WHERE Name = ?);
      "#;
    sqlx::query(sql)
      .bind(next_id)
      .bind(name)
      .bind(Uuid::new_v4().to_string())
      .bind(next_usn)
      .bind(&timestamp)
      .bind(&timestamp)
      .bind(name)
      .execute(&self.pool).await?;
    Ok(())
  }

  pub async fn playlist_add(&self, playlist: &str, content: &Content) -> anyhow::Result<()> {
    let sql = r#"
      INSERT INTO djmdSongPlaylist (ID, PlaylistID, ContentID, UUID, created_at, updated_at, rb_local_usn, TrackNo)
      SELECT ?, pl.ID, c.ID, ?, ?, ?, ?,
        row_number() OVER (ORDER BY c.created_at) +
          COALESCE((SELECT MAX(TrackNo) FROM djmdSongPlaylist WHERE PlaylistID = pl.ID), 0)
      FROM djmdContent AS c, djmdPlaylist AS pl
      WHERE c.ID = ?
        AND pl.Name = ?
        AND NOT EXISTS(SELECT ContentID FROM djmdSongPlaylist WHERE PlaylistID = pl.ID AND ContentID = c.ID)
      ORDER BY c.rating desc, c.created_at DESC
      "#;
    let next_usn = self.next_usn().await?;
    let timestamp = Self::now_timestamp();
    sqlx::query(sql)
      .bind(Uuid::new_v4().to_string())
      .bind(Uuid::new_v4().to_string())
      .bind(&timestamp)
      .bind(&timestamp)
      .bind(next_usn)
      .bind(&content.ID)
      .bind(playlist)
      .execute(&self.pool).await?;
    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  async fn database_content() -> (Database, Content) {
    let database = Database::connect("encrypted.db").await.unwrap();
    let content = database.content("918205852").await.unwrap();
    (database, content)
  }

  #[tokio::test]
  async fn test_find_content() {
    let (_database, content) = database_content().await;
    assert_eq!(content.ID, "43970339");
  }

  #[tokio::test]
  async fn test_playlist_create() {
    let (database, _content) = database_content().await;
    database.playlist_create("2026").await.unwrap();
    database.checkpoint().await.unwrap();
  }

  #[tokio::test]
  async fn test_playlist_add() {
    let (database, content) = database_content().await;
    database.playlist_add("Oefenen", &content).await.unwrap();
    database.checkpoint().await.unwrap();
  }

  #[tokio::test]
  async fn test_content_tags() {
    let (database, content) = database_content().await;
    database.clear_tags(&content).await.unwrap();
    let names = content_tag_names(&database, &content).await;
    let empty: Vec<String> = vec![];
    assert_eq!(empty, names);
    database.tag_content(&content, "eatmos").await.unwrap();
    let names = content_tag_names(&database, &content).await;
    assert_eq!(vec!["eatmos"], names);
    database.untag_content(&content, "eatmos").await.unwrap();
    let names = content_tag_names(&database, &content).await;
    assert_eq!(empty, names);
  }

  async fn content_tag_names(database: &Database, content: &Content) -> Vec<String> {
    let tags = database.content_tags(&content).await.unwrap();
    let names = tags.iter().map(|t| t.Name.to_owned()).collect::<Vec<_>>();
    names
  }

}
