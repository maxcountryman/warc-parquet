use std::{io::BufRead, sync::Arc};

use arrow::{
    array::{ArrayRef, BinaryArray, StringArray, TimestampMillisecondArray, UInt32Array},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::Result,
    record_batch::RecordBatch,
};
use chrono::NaiveDateTime;
use lazy_static::lazy_static;
use warc::{BufferedBody, Record, WarcHeader, WarcReader};

lazy_static! {
    /// The WARC Format 1.0 schema.
    ///
    /// This specification is drawn from the standard
    /// [document](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/).
    pub static ref DEFAULT_SCHEMA: Arc<Schema> =
    Arc::new(Schema::new(vec![
        // Mandatory fields.
        Field::new("id", DataType::Utf8, false),
        Field::new("content_length", DataType::UInt32, false),
        Field::new(
            "date",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("type", DataType::Utf8, false),

        // Optional fields.
        Field::new("content_type", DataType::Utf8, true),
        Field::new("concurrent_to", DataType::Utf8, true),
        Field::new("block_digest", DataType::Utf8, true),
        Field::new("payload_digest", DataType::Utf8, true),
        Field::new("ip_address", DataType::Utf8, true),
        Field::new("refers_to", DataType::Utf8, true),
        Field::new("target_uri", DataType::Utf8, true),
        Field::new("truncated", DataType::Utf8, true),
        Field::new("warc_info_id", DataType::Utf8, true),
        Field::new("filename", DataType::Utf8, true),
        Field::new("profile", DataType::Utf8, true),
        Field::new("identified_payload_type", DataType::Utf8, true),
        Field::new("segment_number", DataType::UInt32, true),
        Field::new("segment_origin_id", DataType::Utf8, true),
        Field::new("segment_total_length", DataType::UInt32, true),
        Field::new("body", DataType::Binary, true),
    ]));
}

/// A reader which transforms the given `BufRead` source into an Arrow
/// representation.
pub struct Reader<R: BufRead> {
    schema: SchemaRef,
    reader: WarcReader<R>,
    records: Vec<Record<BufferedBody>>,
    iter_index: usize,
}

impl<R: BufRead> Reader<R> {
    pub fn new(reader: R, schema: SchemaRef) -> Self {
        Self {
            schema,
            reader: WarcReader::new(reader),
            records: vec![],
            iter_index: 0,
        }
    }

    fn collect_records(&mut self) {
        if self.records.is_empty() {
            let mut stream_iter = self.reader.stream_records();
            let mut records = vec![];
            while let Some(record) = stream_iter.next_item() {
                records.push(record.unwrap().into_buffered().unwrap());
            }
            self.records = records;
        }
    }
}

impl<R: BufRead> Iterator for Reader<R> {
    type Item = Result<RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.collect_records();

        if self.iter_index < self.records.len() {
            let batch = parse(&self.records[self.iter_index], self.schema.fields());
            self.iter_index += 1;
            Some(batch)
        } else {
            None
        }
    }
}

fn parse(record: &Record<BufferedBody>, fields: &[Field]) -> Result<RecordBatch> {
    let arrays: Result<Vec<ArrayRef>> = fields
        .iter()
        .map(|field| {
            Ok(match field.name().as_str() {
                "id" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::RecordID)
                    .map(|s| s.to_string())
                    .expect("WARC-Record-ID header is mandatory.")]))
                    as ArrayRef,

                "content_length" => Arc::new(UInt32Array::from(vec![record
                    .header(WarcHeader::ContentLength)
                    .map(|s| s.to_string().parse::<u32>().unwrap())
                    .expect("Content-Length header is mandatory.")]))
                    as ArrayRef,

                "date" => Arc::new(TimestampMillisecondArray::from_vec(
                    vec![record
                        .header(WarcHeader::Date)
                        .map(|s| {
                            NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%SZ")
                                .unwrap()
                                .timestamp_millis()
                        })
                        .expect("WARC-Date header is mandatory.")],
                    None,
                )) as ArrayRef,

                "type" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::WarcType)
                    .map(|s| s.to_string())
                    .expect("WARC-Type header is mandatory.")]))
                    as ArrayRef,

                "content_type" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::ContentType)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "concurrent_to" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::ConcurrentTo)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "block_digest" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::BlockDigest)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "payload_digest" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::PayloadDigest)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "ip_address" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::IPAddress)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "refers_to" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::RefersTo)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "target_uri" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::TargetURI)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "truncated" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::Truncated)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "warc_info_id" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::WarcInfoID)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "filename" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::Filename)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "profile" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::Profile)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "identified_payload_type" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::IdentifiedPayloadType)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "segment_number" => Arc::new(UInt32Array::from(vec![record
                    .header(WarcHeader::SegmentNumber)
                    .map(|s| {
                        s.to_string()
                            .parse::<u32>()
                            .expect("Malformed segment number.")
                    })])) as ArrayRef,

                "segment_origin_id" => Arc::new(StringArray::from(vec![record
                    .header(WarcHeader::SegmentOriginID)
                    .map(|s| s.to_string())
                    .as_deref()])) as ArrayRef,

                "segment_total_length" => Arc::new(UInt32Array::from(vec![record
                    .header(WarcHeader::SegmentNumber)
                    .map(|s| {
                        s.to_string()
                            .parse::<u32>()
                            .expect("Malformed segment total length.")
                    })])) as ArrayRef,

                "body" => Arc::new(BinaryArray::from(vec![record.body()])) as ArrayRef,

                _ => unimplemented!(),
            })
        })
        .collect();

    arrays.and_then(|arr| RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), arr))
}
