<h1 align="center">
warc-parquet
</h1>

<p align="center">
🗄️  A simple tool for converting <a href="https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/">WARC</a> files to Parquet files.
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

There are any number of ways to consume Parquet once you have it. However a natural fit might be
[DuckDB](https://duckdb.org):

```
$ duckdb
v0.3.3 fe9ba8003
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.
D select type, id from 'example.snappy.parquet';
┌──────────┬─────────────────────────────────────────────────┐
│   type   │                       id                        │
├──────────┼─────────────────────────────────────────────────┤
│ warcinfo │ <urn:uuid:A8063499-7675-4D8D-A736-A1D7DAE84C84> │
│ request  │ <urn:uuid:3EB20966-D74F-4949-AACB-23DB3A0733A7> │
│ response │ <urn:uuid:8B92CADC-F770-45BE-8B72-E13A61CD6D1C> │
│ metadata │ <urn:uuid:4C0E9E17-E21B-49E0-859A-D1016FBDE636> │
│ resource │ <urn:uuid:14F502A5-3BDE-4D0B-8A43-95F4BB8398C6> │
│ resource │ <urn:uuid:6B6D6ADD-52FF-4760-AA00-FB9E739CABBE> │
└──────────┴─────────────────────────────────────────────────┘
D describe select * from 'example.snappy.parquet';
┌─────────────────────────┬─────────────┬──────┬─────┬─────────┬───────┐
│       column_name       │ column_type │ null │ key │ default │ extra │
├─────────────────────────┼─────────────┼──────┼─────┼─────────┼───────┤
│ id                      │ VARCHAR     │ YES  │     │         │       │
│ content_length          │ UINTEGER    │ YES  │     │         │       │
│ date                    │ TIMESTAMP   │ YES  │     │         │       │
│ type                    │ VARCHAR     │ YES  │     │         │       │
│ content_type            │ VARCHAR     │ YES  │     │         │       │
│ concurrent_to           │ VARCHAR     │ YES  │     │         │       │
│ block_digest            │ VARCHAR     │ YES  │     │         │       │
│ payload_digest          │ VARCHAR     │ YES  │     │         │       │
│ ip_address              │ VARCHAR     │ YES  │     │         │       │
│ refers_to               │ VARCHAR     │ YES  │     │         │       │
│ target_uri              │ VARCHAR     │ YES  │     │         │       │
│ truncated               │ VARCHAR     │ YES  │     │         │       │
│ warc_info_id            │ VARCHAR     │ YES  │     │         │       │
│ filename                │ VARCHAR     │ YES  │     │         │       │
│ profile                 │ VARCHAR     │ YES  │     │         │       │
│ identified_payload_type │ VARCHAR     │ YES  │     │         │       │
│ segment_number          │ UINTEGER    │ YES  │     │         │       │
│ segment_origin_id       │ VARCHAR     │ YES  │     │         │       │
│ segment_total_length    │ UINTEGER    │ YES  │     │         │       │
│ body                    │ BLOB        │ YES  │     │         │       │
└─────────────────────────┴─────────────┴──────┴─────┴─────────┴───────┘
```
