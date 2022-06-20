//! Simple WARC to Parquet conversion tooling.
//!
//! Using [`RecordColumns`] to create the necessary columns from a provided
//! [`Record`](warc::Record) along with
//! [`schema()`] is enough to prepare a complete Arrow translation from WARC.
//! Leveraging [`ArrowWriter`](parquet::arrow::ArrowWriter), this representation
//! can then be persisted to disk as Parquet.
//!
//! This implementation assumes the WARC 1.0 format.

mod record_columns;
mod schema;

pub use record_columns::RecordColumns;
pub use schema::schema;
