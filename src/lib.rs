//! AHRB drives coding-agent harnesses through deterministic simulated workflows.

pub mod adapters;
pub mod cli;
pub mod driver;
pub mod error;
pub mod evaluate;
pub mod events;
pub mod evidence_collectors;
pub mod fake_model;
pub mod hbench;
pub mod manifest;
pub mod matrix_evidence;
pub mod mock_harness;
pub mod process;
pub mod report;
pub mod resource_certification;
pub mod runner;
pub mod sampler;
pub mod scenarios;
pub mod workflow;

pub use error::{AhrbError, Result};
