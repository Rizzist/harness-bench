//! Typed errors shared by the benchmark components.

use std::fmt::{Display, Formatter};

/// The common AHRB error type.
#[derive(Debug)]
pub enum AhrbError {
    /// A command-line usage error.
    Usage(String),
    /// A manifest or workflow failed validation.
    Validation(String),
    /// A protocol peer violated its contract.
    Protocol(String),
    /// A required capability is unavailable.
    Unsupported(String),
    /// A deadline elapsed.
    Timeout(String),
    /// An I/O operation failed.
    Io(std::io::Error),
    /// JSON serialization or parsing failed.
    Json(serde_json::Error),
    /// TOML parsing failed.
    Toml(toml::de::Error),
}

impl Display for AhrbError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Usage(message) => write!(f, "usage: {message}"),
            Self::Validation(message) => write!(f, "validation: {message}"),
            Self::Protocol(message) => write!(f, "protocol: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported: {message}"),
            Self::Timeout(message) => write!(f, "timeout: {message}"),
            Self::Io(source) => write!(f, "I/O: {source}"),
            Self::Json(source) => write!(f, "JSON: {source}"),
            Self::Toml(source) => write!(f, "TOML: {source}"),
        }
    }
}

impl std::error::Error for AhrbError {}

impl From<std::io::Error> for AhrbError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<serde_json::Error> for AhrbError {
    fn from(value: serde_json::Error) -> Self {
        Self::Json(value)
    }
}

impl From<toml::de::Error> for AhrbError {
    fn from(value: toml::de::Error) -> Self {
        Self::Toml(value)
    }
}

/// A result returned by AHRB operations.
pub type Result<T> = std::result::Result<T, AhrbError>;
