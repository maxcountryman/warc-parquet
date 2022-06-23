#![deny(missing_docs)]
//! A small library providing a reader from WARC to Arrow.
//!
//! This implementation is written for the WARC Format 1.0 specification.
//!
//! Users will consume the [`Reader`] struct to create a new reader of a WARC
//! source. The reader expects some `BufRead` source which it will internally
//! wrap with a [`WarcReader`](warc::WarcReader). Once
//! created, the reader can be iterated in order to retrieve the Arrow
//! representation of the WARC records.
//!
//! The standard WARC schema is also provided via the [`struct@DEFAULT_SCHEMA`]
//! reference.
//!
//! The `warc-parquet` command line utility leverages this library directly.
//!
//! # Example
//!
//! ```rust
//! use std::io::{BufReader, Cursor};
//!
//! use warc_parquet::{Reader, DEFAULT_SCHEMA};
//!
//! # fn main() {
//! let file = BufReader::new(Cursor::new(b""));
//! let schema = DEFAULT_SCHEMA.clone();
//! let mut reader = Reader::new(file, schema);
//! for record in reader.iter_reader() {
//!     dbg!(record); // There won't be anything, since we provided an empty buffer.
//! }
//! # }
//! ```
mod reader;

pub use reader::{Reader, DEFAULT_SCHEMA};
