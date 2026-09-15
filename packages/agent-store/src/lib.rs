//! Durable PostgreSQL storage for the agent platform.

mod error;
mod inspection;
mod models;
mod store;

pub use error::*;
pub use models::*;
pub use store::*;

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!();
