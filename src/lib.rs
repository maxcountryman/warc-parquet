#![deny(missing_docs)]
//! A crate providing a reader from Web ARChive (WARC) to Apache Arrow format.
//!
//! Users will create a [`WarcToArrowReader`] over a WARC source. This source is
//! expected to be `BufRead`. Generally a new reader will be built via the
//! [`builder`](WarcToArrowReader::builder) method. With a reader constructed,
//! consumers can iterate over records by calling
//! [`iter_reader`](WarcToArrowReader::iter_reader).
//!
//! Internally, the reader uses [`WarcReader`](warc::WarcReader) to read from
//! the provided source. More specifically the streaming interface provided by
//! `WarcReader` is used. This allows the reader to consume very large or
//! indefinite streams. The reader also provides a facility for reading the WARC
//! records into batches of a given `batch_size` (this is useful for forming row
//! groups, e.g. with Parquet). These batches become
//! [`RecordBatch`](arrow::record_batch::RecordBatch).
//!
//! Once translated to Arrow, consumers may operate on the output however they
//! like. For use cases involving Parquet, the `warc-parquet` command line
//! utility is provided.
//!
//! Currently this crate provides a schema for WARC Format 1.0 as
//! [`WARC_1_0_SCHEMA`](static@WARC_1_0_SCHEMA).
//!
//! # Example
//!
//! ```rust
//! use std::io::{BufReader, Cursor, Read};
//!
//! use arrow::array::{BinaryArray, StringArray};
//! use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ArrowWriter};
//! use tempfile::tempfile;
//! use warc_parquet::{parquet::basic::Compression, WarcToArrowReader, WARC_1_0_SCHEMA};
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
#![warn(clippy::all, nonstandard_style, future_incompatible)]
#![deny(missing_docs)]
#![forbid(unsafe_code)]

pub use arrow;
pub use parquet;
pub use reader::{WarcToArrowReader, WarcToArrowReaderBuilder};
pub use schema::WARC_1_0_SCHEMA;

mod reader;
mod schema;
