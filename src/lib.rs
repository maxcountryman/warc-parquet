#![deny(missing_docs)]
//! A small library providing a reader which translates a WARC source to Arrow.
//!
//! This implementation is written for the WARC Format 1.0 specification.
//!
//! Users will consume the [`WarcToArrowReader`] struct to create a new reader
//! of a WARC source. The reader expects some `BufRead` source which it will
//! internally wrap with a [`WarcReader`](warc::WarcReader). Once
//! created, the reader can be iterated in order to retrieve the Arrow
//! representation of the WARC records.
//!
//! The `warc-parquet` command line utility leverages this crate to provide
//! WARC-to-Parquet translation.
//!
//! # Example
//!
//! ```rust
//! use std::io::{BufReader, Cursor, Read};
//!
//! use arrow::array::{BinaryArray, StringArray};
//! use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
//! use tempfile::tempfile;
//! use warc_parquet::{Compression, WarcToArrowReader, WARC_1_0_SCHEMA};
//!
//! # fn main() {
//! let warc_content = b"\
//!     WARC/1.0\r\n\
//!     Warc-Type: response\r\n\
//!     Content-Length: 13\r\n\
//!     WARC-Record-Id: <urn:test:basic-record:record-0>\r\n\
//!     WARC-Date: 2020-07-08T02:52:55Z\r\n\
//!     \r\n\
//!     Hello, world!\r\n\
//!     \r\n\
//! ";
//!
//! let input = BufReader::new(Cursor::new(warc_content));
//! let mut output = tempfile().unwrap();
//!
//! let mut reader = WarcToArrowReader::builder(input)
//!     .with_batch_size(1024)
//!     .build();
//! let mut writer = ArrowWriter::try_new(&mut output, WARC_1_0_SCHEMA.clone(), None).unwrap();
//!
//! let record_batches = reader.iter_reader();
//! for record_batch in record_batches {
//!     writer.write(&record_batch.unwrap()).unwrap();
//! }
//! writer.close().unwrap();
//!
//! let mut parquet_reader = ParquetRecordBatchReaderBuilder::try_new(output)
//!     .unwrap()
//!     .with_batch_size(1024)
//!     .build()
//!     .unwrap();
//! let record_batch = parquet_reader.next().unwrap().unwrap();
//!
//! assert_eq!(
//!     record_batch
//!         .column_by_name("type")
//!         .unwrap()
//!         .as_any()
//!         .downcast_ref::<StringArray>()
//!         .unwrap(),
//!     &StringArray::from(vec!["response"])
//! );
//!
//! assert_eq!(
//!     record_batch
//!         .column_by_name("body")
//!         .unwrap()
//!         .as_any()
//!         .downcast_ref::<BinaryArray>()
//!         .unwrap(),
//!     &BinaryArray::from_vec(vec![b"Hello, world!"])
//! );
//! # }
//! ```

pub use parquet::basic::Compression;
pub use reader::WarcToArrowReader;
pub use schema::WARC_1_0_SCHEMA;

mod reader;
mod schema;
