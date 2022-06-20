use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

fn fields() -> Vec<Field> {
    vec![
        // Mandatory fields.
        Field::new("id", DataType::Utf8, false),
        Field::new("content_length", DataType::UInt32, false),
        Field::new("date", DataType::Timestamp(TimeUnit::Second, None), false),
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
    ]
}

/// A producer for the required [`Schema`] used by Arrow.
///
/// This schema follows the [WARC 1.0 format specification](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/). The first four fields,
/// `id`, `content_length`, `date`, and `type` are required with the remainder
/// being optional.
///
/// With the schema, a program can translate an input format, such as WARC, into
/// Arrow. Once the Arrow representation is established, persisting to disk as
/// Parquet is straightfoward.
pub fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(fields()))
}
