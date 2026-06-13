import math
import struct
import zlib

import pytest

from ngrrd_python import (
    ConsolidationFunction,
    DataSourceType,
    NgrrdFormatError,
    NgrrdReader,
)


FIXED_HEADER_FORMAT = ">4sHHi32siiQQQQQi"
FIXED_HEADER_SIZE = struct.calcsize(FIXED_HEADER_FORMAT)
HEADER_CRC_OFFSET = 92


def align8(value):
    return (value + 7) & ~7


def crc32(data):
    return zlib.crc32(data) & 0xFFFFFFFF


def put_utf8(buf, value):
    encoded = value.encode("utf-8")
    buf.extend(struct.pack(">H", len(encoded)))
    buf.extend(encoded)


def live_state_size(d, a):
    return 8 + d * 16 + d * 48 + a * 12 + a * d * 16 + 4


def build_ngrr_image(
    *,
    columns=None,
    archives=None,
    pointers=None,
    ring_values=None,
    last_up_ms=1_700_000_000_000,
    base_step_sec=300,
    version=1,
):
    columns = columns or [("cpu", "cpu_raw", DataSourceType.GAUGE)]
    archives = archives or [("daily", ConsolidationFunction.AVERAGE, 300, 4, 0.5)]
    pointers = pointers or [(1, 1_700_001_200)]

    d = len(columns)
    a = len(archives)
    static_section_size = FIXED_HEADER_SIZE
    static_section_size += sum(
        2 + len(derived.encode("utf-8")) + 2 + len(raw.encode("utf-8")) + 1
        for derived, raw, _ in columns
    )
    static_section_size += sum(
        2 + len(name.encode("utf-8")) + 1 + 4 + 4 + 8
        for name, _, _, _, _ in archives
    )
    live_offset = align8(static_section_size)
    live_bytes = live_state_size(d, a)
    ring_offset = align8(live_offset + live_bytes)
    total_bytes = ring_offset + sum(rows * d * 8 for _, _, _, rows, _ in archives)

    image = bytearray(total_bytes)
    header = bytearray(
        struct.pack(
            FIXED_HEADER_FORMAT,
            b"NGRR",
            version,
            1,
            base_step_sec,
            b"x" * 32,
            d,
            a,
            static_section_size,
            live_offset,
            live_bytes,
            ring_offset,
            total_bytes,
            0,
        )
    )
    struct.pack_into(">I", header, HEADER_CRC_OFFSET, crc32(header[:HEADER_CRC_OFFSET]))
    image[:FIXED_HEADER_SIZE] = header

    static = bytearray()
    for derived, raw, ds_type in columns:
        put_utf8(static, derived)
        put_utf8(static, raw)
        static.extend(struct.pack(">B", int(ds_type)))
    for name, cf, step_sec, rows, xff in archives:
        put_utf8(static, name)
        static.extend(struct.pack(">Biid", int(cf), step_sec, rows, xff))
    image[FIXED_HEADER_SIZE : FIXED_HEADER_SIZE + len(static)] = static

    live = bytearray(live_bytes)
    offset = 0
    struct.pack_into(">q", live, offset, last_up_ms)
    offset += 8
    offset += d * 16
    offset += d * 48
    for row, epoch_sec in pointers:
        struct.pack_into(">iq", live, offset, row, epoch_sec)
        offset += 12
    offset += a * d * 16
    assert offset == live_bytes - 4
    struct.pack_into(">I", live, live_bytes - 4, crc32(live[:-4]))
    image[live_offset : live_offset + live_bytes] = live

    ring_cursor = ring_offset
    values_by_archive = ring_values or []
    for archive_idx, (_, _, _, rows, _) in enumerate(archives):
        archive_values = (
            values_by_archive[archive_idx]
            if archive_idx < len(values_by_archive)
            else [[float("nan")] * d for _ in range(rows)]
        )
        for row_idx in range(rows):
            row_values = (
                archive_values[row_idx]
                if row_idx < len(archive_values)
                else [float("nan")] * d
            )
            for col_idx in range(d):
                value = row_values[col_idx] if col_idx < len(row_values) else float("nan")
                struct.pack_into(">d", image, ring_cursor, value)
                ring_cursor += 8

    return bytes(image)


def create_mock_ngrr(path, **kwargs):
    path.write_bytes(build_ngrr_image(**kwargs))
    return path


def corrupt(path, offset, mask=0x7F):
    data = bytearray(path.read_bytes())
    data[offset] ^= mask
    path.write_bytes(data)


def test_reader_metadata(tmp_path):
    p = tmp_path / "test.ngrr"
    create_mock_ngrr(p)

    with NgrrdReader(str(p)) as reader:
        meta = reader.get_metadata()
        assert meta["version"] == 1
        assert meta["schema_revision"] == 1
        assert meta["base_step_sec"] == 300
        assert "cpu" in meta["columns"]
        assert "daily/AVERAGE" in meta["archives"]
        assert meta["last_update_ms"] == 1700000000000
        assert meta["definition_hash"] == (b"x" * 32).hex()


def test_read_archive_data(tmp_path):
    p = tmp_path / "test.ngrr"
    create_mock_ngrr(
        p,
        ring_values=[
            [
                [10.0],
                [20.0],
                [float("nan")],
                [float("nan")],
            ]
        ],
    )

    with NgrrdReader(str(p)) as reader:
        data = reader.read_archive_as_dict("daily")
        cpu_data = data["cpu"]

        assert len(cpu_data) == 4
        assert cpu_data[3]["ts"] == 1700001200
        assert cpu_data[3]["value"] == 20.0
        assert cpu_data[2]["ts"] == 1700000900
        assert cpu_data[2]["value"] == 10.0
        assert cpu_data[1]["ts"] == 1700000600
        assert math.isnan(cpu_data[1]["value"])
        assert cpu_data[0]["ts"] == 1700000300
        assert math.isnan(cpu_data[0]["value"])


def test_read_archive_rows_with_multiple_columns_and_cf_filter(tmp_path):
    p = tmp_path / "multi.ngrr"
    create_mock_ngrr(
        p,
        columns=[
            ("in_bps", "in_octets", DataSourceType.COUNTER),
            ("out_bps", "out_octets", DataSourceType.COUNTER),
        ],
        archives=[
            ("rra_5m", ConsolidationFunction.AVERAGE, 300, 3, 0.5),
            ("rra_5m", ConsolidationFunction.MAX, 300, 3, 0.5),
        ],
        pointers=[
            (0, 1_700_000_600),
            (2, 1_700_000_900),
        ],
        ring_values=[
            [[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]],
            [[100.0, 1000.0], [200.0, 2000.0], [300.0, 3000.0]],
        ],
    )

    with NgrrdReader(p) as reader:
        rows = reader.read_archive_rows("rra_5m", ConsolidationFunction.MAX)

    assert rows == [
        {"ts": 1700000300, "in_bps": 100.0, "out_bps": 1000.0},
        {"ts": 1700000600, "in_bps": 200.0, "out_bps": 2000.0},
        {"ts": 1700000900, "in_bps": 300.0, "out_bps": 3000.0},
    ]


def test_empty_archive_returns_empty_shapes(tmp_path):
    p = tmp_path / "empty.ngrr"
    create_mock_ngrr(p, pointers=[(-1, -1)])

    with NgrrdReader(p) as reader:
        assert reader.read_archive_rows("daily") == []
        assert reader.read_archive_as_dict("daily") == {"cpu": []}


def test_archive_not_found_raises_value_error(tmp_path):
    p = tmp_path / "test.ngrr"
    create_mock_ngrr(p)

    with NgrrdReader(p) as reader:
        with pytest.raises(ValueError, match="Archive not found"):
            reader.read_archive_as_dict("weekly")


def test_invalid_magic_raises_format_error(tmp_path):
    p = tmp_path / "bad-magic.ngrr"
    create_mock_ngrr(p)
    data = bytearray(p.read_bytes())
    data[:4] = b"NOPE"
    p.write_bytes(data)

    with pytest.raises(NgrrdFormatError, match="magic"):
        NgrrdReader(p).open()


def test_unsupported_version_raises_format_error(tmp_path):
    p = tmp_path / "bad-version.ngrr"
    create_mock_ngrr(p, version=2)

    with pytest.raises(NgrrdFormatError, match="Unsupported"):
        NgrrdReader(p).open()


def test_header_crc_mismatch_raises_format_error(tmp_path):
    p = tmp_path / "bad-header-crc.ngrr"
    create_mock_ngrr(p)
    corrupt(p, 8)

    with pytest.raises(NgrrdFormatError, match="Header CRC32"):
        NgrrdReader(p).open()


def test_live_state_crc_mismatch_raises_format_error(tmp_path):
    p = tmp_path / "bad-live-crc.ngrr"
    image = bytearray(build_ngrr_image())
    live_offset = struct.unpack_from(FIXED_HEADER_FORMAT, image)[8]
    image[live_offset] ^= 0x7F
    p.write_bytes(image)

    with pytest.raises(NgrrdFormatError, match="Live-state CRC32"):
        NgrrdReader(p).open()


def test_truncated_file_raises_format_error(tmp_path):
    p = tmp_path / "truncated.ngrr"
    p.write_bytes(b"NGRR")

    with pytest.raises(NgrrdFormatError, match="truncated"):
        NgrrdReader(p).open()
