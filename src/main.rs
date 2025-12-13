use rbsqlx::Database;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
  let mut db = Database::connect("encrypted.db").await?;
  let content = db.content("918205852").await?;
  println!("{:?}", content);
  db.rate_content(&content, 4).await?;
  
  let lists = ["eatmos", "ebup", "edrive", "epeak", "ebang", "ebdown"];
  for list in lists.iter() {
    db.tag_content(&content, list).await?;
  }
  Ok(())
}
