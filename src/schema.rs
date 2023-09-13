use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use lazy_static::lazy_static;

lazy_static! {
    /// The WARC Format 1.0 schema.
    ///
    /// This specification is drawn from the standard
    /// [document](https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/).
    pub static ref WARC_1_0_SCHEMA: SchemaRef =
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
