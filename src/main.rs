use std::{
    fs::File,
    io::{BufRead, Error},
    path::PathBuf,
    sync::Arc,
};

use arrow::{datatypes::Schema, record_batch::RecordBatch};
use clap::{ArgEnum, Parser};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use warc::WarcReader;

#[derive(ArgEnum, Clone, Debug)]
enum OptCompression {
    Uncompressed,
    Snappy,
    Gzip,
    Lzo,
    Brotli,
    Lz4,
    Zstd,
}

impl From<OptCompression> for Compression {
    fn from(opt_compression: OptCompression) -> Self {
        match opt_compression {
            OptCompression::Uncompressed => Compression::UNCOMPRESSED,
            OptCompression::Snappy => Compression::SNAPPY,
            OptCompression::Gzip => Compression::GZIP,
            OptCompression::Lzo => Compression::LZO,
            OptCompression::Brotli => Compression::BROTLI,
            OptCompression::Lz4 => Compression::LZ4,
            OptCompression::Zstd => Compression::ZSTD,
        }
    }
}

/// A program for converting WARC-formatted files to Parquet.
#[derive(Parser, Debug)]
struct Args {
    /// Set if the WARC file is compressed with gzip.
    #[clap(long)]
    gzipped: bool,

    /// A path to a WARC-formatted file to be read from.
    #[clap(parse(from_os_str))]
    warc_input: PathBuf,

    /// A path to write Parquet to; existing data WILL be overwritten!
    #[clap(parse(from_os_str))]
    parquet_output: PathBuf,

    /// The compression used for the Parquet.
    #[clap(short, long, arg_enum, value_parser, default_value = "snappy")]
    compression: OptCompression,
}

fn process_records<R: BufRead>(schema: &Arc<Schema>, reader: WarcReader<R>) -> Vec<RecordBatch> {
    let mut batches = vec![];
    for record in reader.iter_records() {
        match record {
            Ok(record) => {
                let schema = schema.clone();
                let columns = warc_parquet::RecordColumns::new(record).columns();
                let batch = RecordBatch::try_new(schema, columns).unwrap();
                batches.push(batch);
            }

            Err(err) => {
                // TODO: Perhaps this should panic if a `strict` mode is provided.
                println!("Error: {}", err)
            }
        }
    }

    batches
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let schema = warc_parquet::schema();
    let warc_path = args.warc_input;
    let batches = if args.gzipped {
        let warc_reader = WarcReader::from_path_gzip(&warc_path)?;
        process_records(&schema, warc_reader)
    } else {
        let warc_reader = WarcReader::from_path(&warc_path)?;
        process_records(&schema, warc_reader)
    };

    let parquet_file = File::create(args.parquet_output)?;
    let batch = RecordBatch::concat(&schema, &batches[..]).unwrap();
    let props = Some(
        WriterProperties::builder()
            .set_compression(args.compression.into())
            .build(),
    );
    let mut writer = ArrowWriter::try_new(parquet_file, batch.schema(), props)?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
