<h1 align="center">
warc-parquet
</h1>

<p align="center">
🗄️  A utility for converting <a href="https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/">WARC</a> to Parquet.
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

To use the crate in your project, add the following to your `Cargo.toml` file:

```
[dependencies]
warc-parquet = "0.6.0"
```

## 🤸 Usage

### The Binary

Once installed, the `warc-parquet` utility can be used to transform WARC into Parquet:

```sh
$ wget --warc-file example 'https://example.com'
$ cat example.warc.gz | warc-parquet --gzipped > example.zstd.parquet
```

`warc-parquet` is meant to fit organically into the UNIX ecosystem. As such processing multiple WARCs at once is straightforward:

```sh
$ wget --warc-file github 'https://github.com'
$ cat example.warc.gz github.warc.gz | warc-parquet --gzipped > combined.zstd.parquet
```

It's also simple to preprocess via standard UNIX piping:

```sh
$ cat example.warc.gz | gzip -d | warc-parquet > example.zstd.parquet
```

Various compression options, including the option to forego compression altogether, are also available:

```sh
$ cat example.warc.gz | warc-parquet --gzipped --compression gzip > example.gz.parquet
```

> 💡 `warc-parquet --help` displays complete options and usage information.

### The Crate

Refer to [the docs](https://docs.rs/warc-parquet) for more details about how to use the `Reader` within your own programs.

### DuckDB

There are any number of ways to consume Parquet once you have it. However a natural fit might be
[DuckDB](https://duckdb.org):

```
$ duckdb
v0.3.3 fe9ba8003
Enter ".help" for usage hints.
Connected to a transient in-memory database.
Use ".open FILENAME" to reopen on a persistent database.

D select type, id from 'example.zstd.parquet';
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

D describe select * from 'example.zstd.parquet';
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
