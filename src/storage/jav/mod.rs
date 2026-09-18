//! JAV download ledger and scheduler state.
mod models;
mod repository;
pub use models::{HistorySummary, Record, State};
pub use repository::Repository;

pub mod cookie;
