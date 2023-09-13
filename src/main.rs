use std::{
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Write},
    path::PathBuf,
};

use clap::{Parser, ValueEnum};
use libflate::gzip::MultiDecoder as GzipReader;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use warc_parquet::{WarcToArrowReader, WARC_1_0_SCHEMA};

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
            OptCompression::Gzip => Compression::GZIP(Default::default()),
            OptCompression::Lzo => Compression::LZO,
            OptCompression::Brotli => Compression::BROTLI(Default::default()),
            OptCompression::Lz4 => Compression::LZ4,
            OptCompression::Zstd => Compression::ZSTD(Default::default()),
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

    /// Sets maximum number of rows in a row group.
    #[clap(long, value_enum, value_parser, default_value = "8192")]
    max_row_group_size: usize,

    /// Sets the maximum number of records to read from the WARC input at a
    /// time.
    #[clap(long, value_enum, value_parser, default_value = "8192")]
    batch_size: usize,
}

fn write_row_groups<W: Write + Send, R: BufRead>(
    writer: &mut ArrowWriter<W>,
    reader: &mut WarcToArrowReader<R>,
) -> Result<(), Box<dyn std::error::Error>> {
    for record_batch in reader.iter_reader() {
        writer.write(&record_batch?)?;
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let stream: Box<dyn BufRead> = if args.warc_input == PathBuf::from(STDIN_MARKER) {
        Box::new(BufReader::with_capacity(MB, io::stdin()))
    } else {
        Box::new(BufReader::with_capacity(
            MB,
            OpenOptions::new().read(true).open(args.warc_input)?,
        ))
    };

    let writer_props = WriterProperties::builder()
        .set_created_by(String::from("warc-parquet"))
        .set_compression(args.compression.into())
        .set_max_row_group_size(args.max_row_group_size)
        .build();
    let mut writer =
        ArrowWriter::try_new(io::stdout(), WARC_1_0_SCHEMA.clone(), Some(writer_props))?;

    let batch_size = args.batch_size;
    if args.gzipped {
        let gzip_stream = BufReader::new(GzipReader::new(stream)?);
        let mut reader = WarcToArrowReader::builder(gzip_stream)
            .with_schema(WARC_1_0_SCHEMA.clone())
            .with_batch_size(batch_size)
            .build();
        write_row_groups(&mut writer, &mut reader)?;
    } else {
        let mut reader = WarcToArrowReader::builder(stream)
            .with_schema(WARC_1_0_SCHEMA.clone())
            .with_batch_size(batch_size)
            .build();
        write_row_groups(&mut writer, &mut reader)?;
    }

    writer.close()?;

    Ok(())
}
