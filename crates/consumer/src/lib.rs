//! CDC consumer package.

pub mod config;
pub mod opensearch;
pub mod worker;

pub use config::ConsumerConfig;
pub use worker::run_fleet;
