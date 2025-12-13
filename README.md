# Use encrypted SQLite database with sqlx

Requires `libsqlite3-sys` with `bundled-sqlcipher` feature. https://github.com/launchbadge/sqlx/pull/2014

NOTE: the key must be SQL 'quoted', otherwise you get

```
error returned from database: (code: 1) unrecognized token: "..."
```