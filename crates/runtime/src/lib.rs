//! Shared engine hosting infrastructure. Download protocols and data models stay
//! in their engines; the host only needs routes and an owned shutdown operation.
pub mod schedule;
mod service;
mod task;
pub mod web;

pub use service::{RunningEngine, shutdown_all};
pub use task::BackgroundTask;
