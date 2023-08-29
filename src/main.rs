use std::{
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Error},
    path::PathBuf,
};

use clap::{Parser, ValueEnum};
use libflate::gzip::MultiDecoder as GzipReader;
use parquet::{
    arrow::ArrowWriter,
    basic::{BrotliLevel, Compression, GzipLevel, ZstdLevel},
    file::properties::WriterProperties,
};
use warc_parquet::{Reader, DEFAULT_SCHEMA};

const MB: usize = 1_048_576;
const STDIN_MARKER: &str = "-";

#[derive(ValueEnum, Clone, Debug)]
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
            OptCompression::Gzip => Compression::GZIP(GzipLevel::default()),
            OptCompression::Lzo => Compression::LZO,
            OptCompression::Brotli => Compression::BROTLI(BrotliLevel::default()),
            OptCompression::Lz4 => Compression::LZ4,
            OptCompression::Zstd => Compression::ZSTD(ZstdLevel::default()),
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
///     $ warc-parquet example.warc.gz --gzipped > example.snappy.parquet
///
/// Alternatively using STDIN:
///
///     $ cat example.warc.gz | gzip -d | warc-parquet > example.snappy.parquet
///
/// Various compression formats for the Parquet output are also supported:
///
///     $ cat example.warc.gz | warc-parquet --gzipped --compression brotli >
/// example.br.parquet
#[derive(Parser, Debug)]
#[clap(version)]
struct Args {
    /// Set if the WARC input is compressed with gzip.
    #[clap(long)]
    gzipped: bool,

    /// WARC input provided either as a path or via STDIN.
    #[clap(default_value = STDIN_MARKER, value_parser)]
    warc_input: PathBuf,

    /// The compression used for the Parquet.
    #[clap(short, long, value_enum, value_parser, default_value_t = OptCompression::Snappy)]
    compression: OptCompression,

    /// The batch size of Parquet records.
    #[clap(long, value_enum, value_parser, default_value = "1024")]
    batch_size: usize,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let stream: Box<dyn BufRead> = if args.warc_input == PathBuf::from(STDIN_MARKER) {
        Box::new(BufReader::with_capacity(MB, io::stdin()))
    } else {
        Box::new(BufReader::with_capacity(
            MB,
            OpenOptions::new().read(true).open(args.warc_input)?,
        ))
    };

    let schema = DEFAULT_SCHEMA.clone();

    let props = WriterProperties::builder()
        .set_compression(args.compression.into())
        .set_created_by(String::from("warc-parquet"))
        .build();

    let mut writer = ArrowWriter::try_new(io::stdout(), schema.clone(), Some(props))?;

    let batch_size = args.batch_size;
    if args.gzipped {
        let gzip_stream = GzipReader::new(stream)?;
        let mut reader = Reader::new(BufReader::new(gzip_stream), schema.clone(), batch_size);

        for batch in reader.iter_reader() {
            let batch = batch.expect("Failed to read batch from WARC");
            writer.write(&batch)?;
            writer.flush()?;
        }
    } else {
        let mut reader = Reader::new(stream, schema.clone(), batch_size);

        for batch in reader.iter_reader() {
            let batch = batch.expect("Failed to read batch from WARC");
            writer.write(&batch)?;
            writer.flush()?;
        }
    }

    writer.close()?;

    Ok(())
}
