use std::{io::BufRead, sync::Arc};

use arrow::{
    array::{ArrayRef, BinaryArray, StringArray, TimestampMillisecondArray, UInt32Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use chrono::NaiveDateTime;
use warc::{BufferedBody, Record, StreamingIter, WarcHeader, WarcReader};

use crate::schema::WARC_1_0_SCHEMA;

type ReaderResult<T> = Result<T, Box<dyn std::error::Error>>;

/// A builder used to constract [`WarcToArrowReader`] for a given reader of
/// WARC.
pub struct WarcToArrowReaderBuilder<R: BufRead> {
    reader: R,
    schema: SchemaRef,
    batch_size: usize,
}

impl<R: BufRead> WarcToArrowReaderBuilder<R> {
    /// Create a new WarcToArrowReaderBuilder.
    ///
    /// # Example
    ///
    /// ```rust
    /// let input = BufReader::new(Cursor::new(""));
    /// let reader_builder = WarcToArrowReaderBuilder::new(input).with_batch_size(1);
    /// let reader = reader_builder.build();
    /// ```
    pub fn new(reader: R) -> Self {
        Self {
            reader,
            schema: WARC_1_0_SCHEMA.clone(),
            batch_size: 8192,
        }
    }

    /// Sets the schema for the reader.
    pub fn with_schema(mut self, schema: SchemaRef) -> Self {
        self.schema = schema;
        self
    }

    /// Sets the batch size for the reader.
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Build a [`WarcToArrowReader`].
    pub fn build(self) -> WarcToArrowReader<R> {
        let reader = WarcReader::new(self.reader);
        WarcToArrowReader {
            reader,
            schema: self.schema,
            batch_size: self.batch_size,
        }
    }
}

/// A wrapper around a [`WarcReader`](warc::WarcReader) which provides a
/// translation from a WARC source to an Arrow representation. The Arrow
/// representation can then be used for different tasks, including persistence
/// via a format such as Parquet.
///
/// # Example
///
/// ```rust
/// use std::{
///     io::{BufReader, Cursor},
///     sync::Arc,
/// };
///
/// use arrow::array::StringArray;
/// use warc_parquet::WarcToArrowReader;
///
/// # fn main() {
/// let warc_content = b"\
///     WARC/1.0\r\n\
///     Warc-Type: response\r\n\
///     Content-Length: 13\r\n\
///     WARC-Record-Id: <urn:test:basic-record:record-0>\r\n\
///     WARC-Date: 2020-07-08T02:52:55Z\r\n\
///     \r\n\
///     Hello, world!\r\n\
///     \r\n\
/// ";
///
/// let input = BufReader::new(Cursor::new(warc_content));
/// let mut reader = WarcToArrowReader::builder(input)
///     .with_batch_size(1024)
///     .build();
/// let mut iter_reader = reader.iter_reader();
///
/// let record_batch = iter_reader.next().unwrap().unwrap();
/// assert_eq!(
///     record_batch
///         .column_by_name("id")
///         .unwrap()
///         .as_any()
///         .downcast_ref::<StringArray>()
///         .unwrap(),
///     &StringArray::from(vec!["<urn:test:basic-record:record-0>"])
/// );
/// # }
/// ```
pub struct WarcToArrowReader<R: BufRead> {
    schema: SchemaRef,
    reader: WarcReader<R>,
    batch_size: usize,
}

impl<R: BufRead> WarcToArrowReader<R> {
    /// Provides a builder for constructing a new `WarcToArrowReader` from a
    /// WARC source.
    pub fn builder(reader: R) -> WarcToArrowReaderBuilder<R> {
        WarcToArrowReaderBuilder::new(reader)
    }

    /// Returns an interface which can be used to iterate through record
    /// batches.
    pub fn iter_reader(&mut self) -> IterReader<'_, R> {
        IterReader::new(self.reader.stream_records(), &self.schema, self.batch_size)
    }
}

/// An iterator type for the underlying data. This consumes the streaming API of
/// the [`WarcReader`], producing record batches of up to `batch_size`.
pub struct IterReader<'r, R> {
    schema: &'r SchemaRef,
    stream_iter: StreamingIter<'r, R>,
    batch_size: usize,
    stream_ended: bool,
}

impl<'r, R: BufRead> IterReader<'r, R> {
    pub(crate) fn new(
        stream_iter: StreamingIter<'r, R>,
        schema: &'r SchemaRef,
        batch_size: usize,
    ) -> IterReader<'r, R> {
        Self {
            schema,
            stream_iter,
            batch_size,
            stream_ended: false,
        }
    }
}

impl<R: BufRead> Iterator for IterReader<'_, R> {
    type Item = ReaderResult<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut records = Vec::with_capacity(self.batch_size);
        while records.len() < self.batch_size && !self.stream_ended {
            match self.stream_iter.next_item() {
                Some(Ok(record)) => {
                    records.push(record.into_buffered().expect("Failed to buffer record."));
                }

                Some(Err(err)) => {
                    return Some(Err(err.into()));
                }

                None => {
                    self.stream_ended = true;
                    break;
                }
            }
        }

        if !records.is_empty() {
            Some(build_record_batch(self.schema, &records))
        } else {
            None
        }
    }
}

fn build_record_batch(
    schema: &SchemaRef,
    records: &[Record<BufferedBody>],
) -> ReaderResult<RecordBatch> {
    let mut columns = Vec::with_capacity(records.len());

    for field in schema.fields() {
        let field_name = field.name();
        let field_array: ArrayRef = match field_name.as_str() {
            "id" => {
                let id_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::RecordID)
                            .map(|h| h.to_string())
                            .expect("WARC-Record-ID header is mandatory.")
                    })
                    .collect();
                Arc::new(StringArray::from(id_values))
            }

            "content_length" => {
                let content_length_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::ContentLength)
                            .map(|h| h.to_string().parse::<u32>().unwrap())
                            .expect("Content-Length header is mandatory.")
                    })
                    .collect();
                Arc::new(UInt32Array::from(content_length_values))
            }

            "date" => {
                let date_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::Date)
                            .map(|h| {
                                NaiveDateTime::parse_from_str(&h, "%Y-%m-%dT%H:%M:%SZ")
                                    .unwrap()
                                    .timestamp_millis()
                            })
                            .expect("WARC-Date header is mandatory.")
                    })
                    .collect();
                Arc::new(TimestampMillisecondArray::from(date_values))
            }

            "type" => {
                let type_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::WarcType)
                            .map(|h| h.to_string())
                            .expect("WARC-Type header is mandatory.")
                    })
                    .collect();
                Arc::new(StringArray::from(type_values))
            }

            "content_type" => {
                let content_type_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::ContentType)
                            .map(|h| h.to_string())
                    })
                    .collect();
                Arc::new(StringArray::from(content_type_values))
            }

            "concurrent_to" => {
                let concurrent_to_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::ConcurrentTo)
                            .map(|h| h.to_string())
                    })
                    .collect();
                Arc::new(StringArray::from(concurrent_to_values))
            }

            "block_digest" => {
                let block_digest_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::BlockDigest)
                            .map(|h| h.to_string())
                    })
                    .collect();
                Arc::new(StringArray::from(block_digest_values))
            }

            "payload_digest" => {
                let payload_digest_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::PayloadDigest)
                            .map(|h| h.to_string())
                    })
                    .collect();
                Arc::new(StringArray::from(payload_digest_values))
            }

            "ip_address" => {
                let ip_address_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::IPAddress).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(ip_address_values))
            }

            "refers_to" => {
                let refers_to_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::RefersTo).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(refers_to_values))
            }

            "target_uri" => {
                let target_uri_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::TargetURI).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(target_uri_values))
            }

            "truncated" => {
                let truncated_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::Truncated).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(truncated_values))
            }

            "warc_info_id" => {
                let warc_info_id_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::WarcInfoID).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(warc_info_id_values))
            }

            "filename" => {
                let filename_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::Filename).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(filename_values))
            }

            "profile" => {
                let profile_values: Vec<_> = records
                    .iter()
                    .map(|record| record.header(WarcHeader::Profile).map(|h| h.to_string()))
                    .collect();
                Arc::new(StringArray::from(profile_values))
            }

            "identified_payload_type" => {
                let identified_payload_type_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::IdentifiedPayloadType)
                            .map(|h| h.to_string())
                    })
                    .collect();

                Arc::new(StringArray::from(identified_payload_type_values))
            }

            "segment_number" => {
                let segment_number_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record.header(WarcHeader::SegmentNumber).map(|h| {
                            h.to_string()
                                .parse::<u32>()
                                .expect("Malformed segment number.")
                        })
                    })
                    .collect();

                Arc::new(UInt32Array::from(segment_number_values))
            }

            "segment_origin_id" => {
                let segment_origin_id_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record
                            .header(WarcHeader::SegmentOriginID)
                            .map(|h| h.to_string())
                    })
                    .collect();

                Arc::new(StringArray::from(segment_origin_id_values))
            }

            "segment_total_length" => {
                let segment_total_length_values: Vec<_> = records
                    .iter()
                    .map(|record| {
                        record.header(WarcHeader::SegmentTotalLength).map(|h| {
                            h.to_string()
                                .parse::<u32>()
                                .expect("Malformed segment total length.")
                        })
                    })
                    .collect();

                Arc::new(UInt32Array::from(segment_total_length_values))
            }

            "body" => {
                let body_values: Vec<_> = records.iter().map(|record| record.body()).collect();

                Arc::new(BinaryArray::from(body_values))
            }

            _ => unimplemented!(),
        };

        columns.push(field_array);
    }

    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
