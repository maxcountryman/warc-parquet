<h1 align="center">
warc-parquet
</h1>

<p align="center">
🗄️ A simple tool for converting WARC files to Parquet files.
</p>

<div align="center">
<a href="https://crates.io/crates/warc-parquet">
<img src="https://img.shields.io/crates/v/warc-parquet.svg" />
</a>
<a href="https://docs.rs/warc-parquet">
<img src="https://docs.rs/warc-parquet/badge.svg" />
</a>
</div>

## 📦 Install

The binary may be installed via `cargo`:

```sh
$ cargo install warc-parquet
```

## 🤸 Usage

Once installed, WARC files can be passed to the program with a target output path which Parquet will be written to:

```sh
$ wget --warc-file example 'https://example.com'
$ warc-parquet --gzipped example.warc.gz example.snappy.parquet
```

> ⚠️ Note that the Parquet path **WILL** be overwritten.
