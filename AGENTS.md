# AI Agent Instructions for rbsqlx

This document provides essential context for AI agents working on `rbsqlx`, a Rust crate for interacting with Pioneer Rekordbox 6+ encrypted SQLite master databases.

## Architecture & Big Picture
- **Domain**: Reverses/maps to the Pioneer Rekordbox 6/7 database schema.
- **Database Backend**: Uses `sqlx` built against `libsqlite3-sys` with the `bundled-sqlcipher` feature to support SQLCipher encryption.
- **Core Components**: 
  - `Database` struct manages the connection pool, handling the application of encryption pragmas (`key`, `cipher_compatibility=4`).
  - Structs directly map to Rekordbox tables (e.g., `Content` -> `djmdContent`, `Playlist` -> `djmdPlaylist`).

## Conventions & Patterns
- **Naming**: The project uses `#![allow(non_snake_case)]` globally. Struct fields rigorously mirror the Rekordbox exact casing (e.g., `ID`, `FileNameL`, `ParentID`) so `sqlx::FromRow` correctly populates fields without rename annotations.
- **Update Handling (USN)**: Operations that mutate data MUST bump the `agentRegistry` sequence number (`rb_local_usn`). The `next_usn()` or `next_usn_tx()` functions update `registry_id = 'localUpdateCount'` and assign this updated number down to table mutations (e.g., playlists, tags).
- **ID Generation**:
  - Secondary UUIDs are generated via `uuid` (`Uuid::new_v4().to_string()`).
  - Rekordbox Table IDs tend to be generated via specific logic, e.g. `next_id_tx` loops random 4-byte numbers sequentially checking for collisions to emulate Rekordbox behavior.
- **Transactions**: For multiple dependent creations (like inserting Playlists or updating `agentRegistry` + data), prefer `sqlx` transactions (`pool.begin()`).
- **Committing changes**: Changes are made in `WAL` mode. Test assertions should use `database.checkpoint().await` to truncate WAL and commit.

## Developer Workflow & Testing
- **Configuration Environment**: Requires a `.env` file containing `SQLCIPHER_KEY`. The `README.md` emphasizes that this key *must* be SQL single-quoted.
- **Testing Requirements**: Tests expect an `encrypted.db` file in the project root.
- **Raw Queries**: The crate heavily utilizes runtime raw SQL (e.g., `sqlx::query_as::<_, Struct>("SELECT...")`) instead
  of the `query!` macro since the compile-time SQL execution might be hard to orchestrate with SQLCipher and `.env`
  setup. Prefer using runtime bindings with `$1` placeholders.

