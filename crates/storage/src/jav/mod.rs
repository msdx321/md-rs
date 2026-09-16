//! JAV download ledger and scheduler state.
mod models;
mod repository;
pub use models::{Record, State};
pub use repository::Repository;

pub mod cookie;
