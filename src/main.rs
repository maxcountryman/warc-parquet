use std::{
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Error},
    path::PathBuf,
};

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use clap::{ArgEnum, Parser};
use libflate::gzip::MultiDecoder as GzipReader;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use warc_parquet::{Reader, DEFAULT_SCHEMA};

const MB: usize = 1_048_576;
const STDIN_MARKER: &str = "-";

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

/// A utility for converting WARC to Parquet.
///
/// WARC may be provided either as a path to a WARC file or via STDIN. Parquet
/// is then printed to STDOUT.
///
/// With a provided path:
///
/// $ warc-parquet example.warc.gz --gzipped > example.snappy.parquet
///
/// Alternatively using STDIN:
///
/// $ cat example.warc.gz | gzip -d | warc-parquet > example.snappy.parquet
///
/// Various compression formats for the Parquet output are also supported:
///
/// $ cat example.warc.gz | warc-parquet --gzipped --compression brotli >
/// example.br.parquet
#[derive(Parser, Debug)]
#[clap(version)]
struct Args {
    /// Set if the WARC input is compressed with gzip.
    #[clap(long)]
    gzipped: bool,

    /// WARC input provided either as a path or via STDIN.
    #[clap(default_value = STDIN_MARKER, parse(from_os_str))]
    warc_input: PathBuf,

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

    let stream = if args.warc_input == PathBuf::from(STDIN_MARKER) {
        Box::new(BufReader::with_capacity(MB, io::stdin())) as Box<dyn BufRead>
    } else {
        Box::new(BufReader::with_capacity(
            MB,
            OpenOptions::new().read(true).open(args.warc_input)?,
        )) as Box<dyn BufRead>
    };

    let schema = DEFAULT_SCHEMA.clone();

    let batch = if args.gzipped {
        let gzip_stream = GzipReader::new(stream)?;
        let reader = Reader::new(BufReader::new(gzip_stream), schema.clone());
        concat_batches(reader, schema)
    } else {
        let reader = Reader::new(stream, schema.clone());
        concat_batches(reader, schema)
    };

    let props = WriterProperties::builder()
        .set_compression(args.compression.into())
        .set_created_by(String::from("warc-parquet"))
        .build();
    let mut writer = ArrowWriter::try_new(io::stdout(), batch.schema(), Some(props))?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
