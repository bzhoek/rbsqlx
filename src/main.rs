use rbsqlx::Database;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let mut db = Database::connect().await?;
  let content = db.content("918205852").await?;
  println!("{:?}", content);
  db.tag_content(&content, "edrive").await?;
  Ok(())
}
