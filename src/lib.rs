//! Aspen S3 - S3-compatible API server
//!
//! This crate provides an S3-compatible API that can be backed by
//! various storage implementations.

pub mod api;
pub mod s3;

pub use s3::AspenS3Service;