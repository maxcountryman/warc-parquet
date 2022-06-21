use std::sync::Arc;

use arrow::array::{ArrayRef, BinaryArray, StringArray, TimestampMillisecondArray, UInt32Array};
use chrono::NaiveDateTime;
use warc::{BufferedBody, Record, WarcHeader};

/// A container for converting a given [`Record<BufferedBody>`] to columns
/// formatted as [`Vec<ArrayRef>`].
///
/// This is the primary translation layer between the WARC record and the Arrow
/// representation. More specifically, these columns form a batch per read
/// record. Later batches are concatenated and then finally written via
/// [`ArrowWriter`](parquet::arrow::ArrowWriter).
///
/// # Example
///
/// ```rust
/// use warc::RecordBuilder;
/// use warc_parquet::RecordColumns;
///
/// # fn main() {
/// let record = RecordBuilder::default().build().unwrap();
/// let columns = RecordColumns::new(record);
/// # }
/// ```
pub struct RecordColumns {
    record: Record<BufferedBody>,
}

impl RecordColumns {
    /// Creates a new struct with the provided [`Record`].
    pub fn new(record: Record<BufferedBody>) -> Self {
        Self { record }
    }

    /// Returns the formatted columsn relative to the provided [`Record`].
    pub fn columns(self) -> Vec<ArrayRef> {
        vec![
            self.id(),
            self.content_length(),
            self.date(),
            self.r#type(),
            self.content_type(),
            self.concurrent_to(),
            self.block_digest(),
            self.payload_digest(),
            self.ip_address(),
            self.refers_to(),
            self.target_uri(),
            self.truncated(),
            self.warc_info_id(),
            self.filename(),
            self.profile(),
            self.identified_payload_type(),
            self.segment_number(),
            self.segment_origin_id(),
            self.segment_total_length(),
            self.body(),
        ]
    }

    fn id(&self) -> Arc<StringArray> {
        Arc::new(StringArray::from(vec![self.record.warc_id()]))
    }

    fn content_length(&self) -> Arc<UInt32Array> {
        Arc::new(UInt32Array::from(vec![self
            .record
            .header(WarcHeader::ContentLength)
            .map(|s| s.to_string().parse::<u32>().unwrap())
            .unwrap()]))
    }

    fn date(&self) -> Arc<TimestampMillisecondArray> {
        Arc::new(TimestampMillisecondArray::from_vec(
            vec![self
                .record
                .header(WarcHeader::Date)
                .map(|s| {
                    NaiveDateTime::parse_from_str(&s, "%Y-%m-%dT%H:%M:%SZ")
                        .unwrap()
                        .timestamp_millis()
                })
                .unwrap()],
            None,
        ))
    }

    fn r#type(&self) -> Arc<StringArray> {
        Arc::new(StringArray::from(vec![self.record.warc_type().to_string()]))
    }

    fn content_type(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::ContentType)
    }

    fn concurrent_to(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::ConcurrentTo)
    }

    fn block_digest(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::BlockDigest)
    }

    fn payload_digest(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::PayloadDigest)
    }

    fn ip_address(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::IPAddress)
    }

    fn refers_to(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::RefersTo)
    }

    fn target_uri(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::TargetURI)
    }

    fn truncated(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::Truncated)
    }

    fn warc_info_id(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::WarcInfoID)
    }

    fn filename(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::Filename)
    }

    fn profile(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::Profile)
    }

    fn identified_payload_type(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::IdentifiedPayloadType)
    }

    fn segment_number(&self) -> Arc<UInt32Array> {
        self.optional_uint32_header(WarcHeader::SegmentNumber)
    }

    fn segment_origin_id(&self) -> Arc<StringArray> {
        self.optional_string_header(WarcHeader::SegmentOriginID)
    }

    fn segment_total_length(&self) -> Arc<UInt32Array> {
        self.optional_uint32_header(WarcHeader::SegmentTotalLength)
    }

    fn body(&self) -> Arc<BinaryArray> {
        Arc::new(BinaryArray::from(vec![self.record.body()]))
    }

    fn optional_string_header(&self, header: WarcHeader) -> Arc<StringArray> {
        Arc::new(StringArray::from(vec![self
            .record
            .header(header)
            .map(|s| s.to_string())
            .as_deref()]))
    }

    fn optional_uint32_header(&self, header: WarcHeader) -> Arc<UInt32Array> {
        Arc::new(UInt32Array::from(vec![self
            .record
            .header(header)
            .map(|s| s.to_string().parse::<u32>().unwrap())]))
    }
}
