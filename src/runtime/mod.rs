//! Shared engine hosting infrastructure. Download protocols and data models stay
//! in their engines; the host only needs routes and an owned shutdown operation.
pub mod download_limiter;
pub mod download_slots;
pub mod download_speed;
pub(crate) mod http_lifecycle;
pub mod schedule;
mod service;
mod task;
pub mod video_filename;
pub mod video_resolution;
pub(crate) mod video_tasks;
pub mod video_transfer;
pub mod web;

pub use service::{RunningEngine, shutdown_all};
pub use task::BackgroundTask;
