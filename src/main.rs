use std::{fs::File, io::Error, path::PathBuf};

use arrow::record_batch::RecordBatch;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use structopt::StructOpt;
use warc::WarcReader;

/// A program for converting WARC-formatted files to Parquet.
#[derive(StructOpt, Debug)]
#[structopt(name = "orca")]
struct Opt {
    /// A path to a WARC-formatted file to be read from.
    #[structopt(parse(from_os_str))]
    warc_input: PathBuf,

    /// A path to write Parquet to; existing data WILL be overwritten!
    #[structopt(parse(from_os_str))]
    parquet_output: PathBuf,
}

fn main() -> Result<(), Error> {
    let opt = Opt::from_args();

    let warc_reader = WarcReader::from_path(opt.warc_input)?;
    let schema = warc_parquet::schema();
    let mut batches = vec![];
    for record in warc_reader.iter_records() {
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

    let parquet_file = File::create(opt.parquet_output)?;
    let batch = RecordBatch::concat(&schema, &batches[..]).unwrap();
    let props = Some(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let mut writer = ArrowWriter::try_new(parquet_file, batch.schema(), props)?;

    writer.write(&batch)?;
    writer.close()?;

    Ok(())
}
