# ngrrd-python

Python reader for the **ngrrd** (Nishi Grid Round-Robin Database) NGRR series
format produced by `nishi-utils-oss`.

The package is intentionally small: it uses Python's standard `mmap`, `struct`,
and `zlib` modules to validate and lazily read `.ngrr` files without loading all
ring buffers into memory.

## Installation

```bash
pip install .
```

For development:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/python -m pytest
```

The `.venv` directory is local-only and should not be committed.

## Python API

```python
from ngrrd_python import ConsolidationFunction, NgrrdReader

with NgrrdReader("series.ngrr") as reader:
    metadata = reader.get_metadata()
    print(metadata)

    data = reader.read_archive_as_dict("daily", ConsolidationFunction.AVERAGE)
    for point in data["in_bps"]:
        print(f"TS: {point['ts']}, Value: {point['value']}")

    rows = reader.read_archive_rows("daily", "AVERAGE")
    for row in rows:
        print(row["ts"], row["in_bps"])
```

`read_archive_as_dict()` keeps the original column-oriented shape:

```python
{
    "in_bps": [{"ts": 1700000900, "value": 10.0}],
    "out_bps": [{"ts": 1700000900, "value": 20.0}],
}
```

`read_archive_rows()` returns one dictionary per timestamp, which is often more
natural for APIs and data tooling:

```python
[{"ts": 1700000900, "in_bps": 10.0, "out_bps": 20.0}]
```

## CLI

After installation, the package exposes `ngrrd-dump`:

```bash
ngrrd-dump series.ngrr --archive daily --cf AVERAGE
ngrrd-dump series.ngrr --archive daily --cf AVERAGE --rows --format xml
```

The example script is a thin wrapper around the same command:

```bash
python examples/ngrrd_dump.py series.ngrr --archive daily --cf AVERAGE
```

## Supported Format

This reader supports NGRR format version `1`, matching the Java
`SeriesFileCodec` layout:

- big-endian fixed header (`MAGIC`, version, schema revision, geometry offsets);
- CRC32 validation for the fixed header and live-state section;
- data-source dictionary and flattened archive table (`rra x cf`);
- row-major ring buffers containing IEEE-754 doubles;
- `NaN` values represent missing points.

The reader is read-only. It does not create or mutate `.ngrr` files.
