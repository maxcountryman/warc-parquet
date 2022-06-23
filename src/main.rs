use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Error},
    path::PathBuf,
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use clap::{ArgEnum, Parser};
use libflate::gzip::MultiDecoder as GzipReader;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use warc_parquet::{Reader, DEFAULT_SCHEMA};

const MB: usize = 1_048_576;

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
#[clap(version)]
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
    #[clap(short, long, arg_enum, value_parser, default_value_t = OptCompression::Snappy)]
    compression: OptCompression,
}

fn concat_batches<R: BufRead>(mut reader: Reader<R>, schema: SchemaRef) -> RecordBatch {
    let mut batches = vec![];
    for batch in reader.iter_reader() {
        batches.push(batch.unwrap());
    }
    RecordBatch::concat(&schema, &batches[..]).unwrap()
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let file = OpenOptions::new().read(true).open(args.warc_input)?;
    let schema = DEFAULT_SCHEMA.clone();

    let batch = if args.gzipped {
        let gzip_stream = GzipReader::new(BufReader::with_capacity(MB, file))?;
        let reader = Reader::new(BufReader::new(gzip_stream), schema.clone());
        concat_batches(reader, schema)
    } else {
        let reader = Reader::new(BufReader::with_capacity(MB, file), schema.clone());
        concat_batches(reader, schema)
    };

    let parquet_file = File::create(args.parquet_output)?;
    let props = WriterProperties::builder()
        .set_compression(args.compression.into())
        .set_created_by(String::from("warc-parquet"))
        .build();
    let mut writer = ArrowWriter::try_new(parquet_file, batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
