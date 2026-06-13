"""Lazy reader for NGRR time-series files."""

from __future__ import annotations

import mmap
import os
import struct
import zlib
from pathlib import Path
from typing import Any, BinaryIO

from .errors import NgrrdFormatError
from .models import (
    ConsolidationFunction,
    DataSourceType,
    SeriesArchive,
    SeriesColumn,
    SeriesHeader,
)


class NgrrdReader:
    """Read a single NGRR series file using memory-mapped IO.

    The reader mirrors the Java ``SeriesFileCodec`` layout: big-endian fields, a
    96-byte fixed header, immutable dictionaries, mutable live-state, and
    row-major ring buffers. Header and live-state CRCs are validated when the
    file is opened, while ring values are materialized only when requested.
    """

    MAGIC = b"NGRR"
    CURRENT_VERSION = 1
    FIXED_HEADER = struct.Struct(">4sHHi32siiQQQQQi")
    FIXED_HEADER_SIZE = FIXED_HEADER.size
    HEADER_CRC_OFFSET = 92

    _U8 = struct.Struct(">B")
    _U16 = struct.Struct(">H")
    _I32 = struct.Struct(">i")
    _I64 = struct.Struct(">q")
    _F64 = struct.Struct(">d")
    _RRA_PTR = struct.Struct(">iq")

    def __init__(self, file_path: str | os.PathLike[str]):
        self.file_path = Path(file_path)
        self._file: BinaryIO | None = None
        self._mmap: mmap.mmap | None = None
        self.header: SeriesHeader | None = None
        self.columns: list[SeriesColumn] = []
        self.archives: list[SeriesArchive] = []
        self._live_state: dict[str, Any] = {}

    def __enter__(self) -> "NgrrdReader":
        self.open()
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()

    def open(self) -> None:
        """Open and validate the NGRR file.

        Calling ``open()`` while the reader is already open is a no-op. Prefer
        the context-manager form for predictable cleanup.
        """

        if self._mmap is not None:
            return

        self._file = self.file_path.open("rb")
        try:
            file_size = os.fstat(self._file.fileno()).st_size
            if file_size < self.FIXED_HEADER_SIZE:
                raise NgrrdFormatError(
                    f"NGRR file is truncated: {file_size} bytes, expected at least {self.FIXED_HEADER_SIZE}"
                )

            self._mmap = mmap.mmap(self._file.fileno(), 0, access=mmap.ACCESS_READ)
            self.header = self._parse_header(file_size)
            self.columns, self.archives = self._parse_dictionaries(self.header)
            self._live_state = self._parse_live_state(self.header)
        except Exception:
            self.close()
            raise

    def close(self) -> None:
        """Close the memory map and underlying file handle."""

        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._file is not None:
            self._file.close()
            self._file = None

    def get_metadata(self) -> dict[str, Any]:
        """Return file-level metadata without materializing ring data."""

        header = self._require_header()
        return {
            "version": header.version,
            "schema_revision": header.schema_revision,
            "base_step_sec": header.base_step_sec,
            "definition_hash": header.definition_hash.hex(),
            "column_count": header.column_count,
            "archive_count": header.archive_count,
            "columns": [c.derived_name for c in self.columns],
            "archives": [f"{a.rra_name}/{a.cf.name}" for a in self.archives],
            "last_update_ms": self._live_state["last_up_ms"],
            "file_total_bytes": header.file_total_bytes,
        }

    def describe_geometry(self) -> dict[str, Any]:
        """Return the full structural geometry without materializing ring data.

        Unlike :meth:`get_metadata`, which is a compact summary, this exposes the
        decoded header (including section offsets), every derived column with its
        raw data source and type, and every physical archive with its step, row
        count, ``xff`` and ring layout. No ring buffers are read.
        """

        header = self._require_header()
        return {
            "file": str(self.file_path),
            "header": header.to_dict(),
            "last_update_ms": self._live_state["last_up_ms"],
            "columns": [column.to_dict() for column in self.columns],
            "archives": [
                {**archive.to_dict(), "ring_bytes": archive.rows * header.column_count * self._F64.size}
                for archive in self.archives
            ],
        }

    def read_archive_as_dict(
        self, archive_name: str, cf: ConsolidationFunction | str | None = None
    ) -> dict[str, list[dict[str, Any]]]:
        """Read an archive as ``{column: [{"ts": epoch_sec, "value": float}]}``.

        ``archive_name`` is the RRA name from the Java definition. Pass ``cf``
        when that RRA has multiple physical archives, one per consolidation
        function.
        """

        result: dict[str, list[dict[str, Any]]] = {c.derived_name: [] for c in self.columns}
        for row in self._archive_rows(archive_name, cf):
            ts = row["ts"]
            for column in self.columns:
                result[column.derived_name].append({"ts": ts, "value": row[column.derived_name]})
        return result

    def read_archive_rows(
        self, archive_name: str, cf: ConsolidationFunction | str | None = None
    ) -> list[dict[str, Any]]:
        """Read an archive as row-oriented dictionaries.

        Each item contains ``ts`` plus one key per persisted column, a shape that
        is convenient for JSON APIs and tabular analysis.
        """

        return self._archive_rows(archive_name, cf)

    def _parse_header(self, file_size: int) -> SeriesHeader:
        data = self._bytes(0, self.FIXED_HEADER_SIZE)
        (
            magic,
            version,
            schema_rev,
            base_step,
            def_hash,
            column_count,
            archive_count,
            static_bytes,
            live_offset,
            live_bytes,
            ring_offset,
            total_bytes,
            stored_crc,
        ) = self.FIXED_HEADER.unpack(data)

        if magic != self.MAGIC:
            raise NgrrdFormatError(f"Invalid NGRR magic: expected {self.MAGIC!r}, got {magic!r}")
        if version != self.CURRENT_VERSION:
            raise NgrrdFormatError(
                f"Unsupported NGRR version: {version}; expected {self.CURRENT_VERSION}"
            )

        actual_crc = self._crc32(data[: self.HEADER_CRC_OFFSET])
        if actual_crc != (stored_crc & 0xFFFFFFFF):
            raise NgrrdFormatError(
                "Header CRC32 mismatch: "
                f"stored=0x{stored_crc & 0xFFFFFFFF:08X} calculated=0x{actual_crc:08X}"
            )

        header = SeriesHeader(
            version=version,
            schema_revision=schema_rev,
            base_step_sec=base_step,
            definition_hash=def_hash,
            column_count=column_count,
            archive_count=archive_count,
            static_section_bytes=static_bytes,
            live_state_offset=live_offset,
            live_state_bytes=live_bytes,
            ring_data_offset=ring_offset,
            file_total_bytes=total_bytes,
        )
        self._validate_header_layout(header, file_size)
        return header

    def _parse_dictionaries(
        self, header: SeriesHeader
    ) -> tuple[list[SeriesColumn], list[SeriesArchive]]:
        offset = self.FIXED_HEADER_SIZE
        columns: list[SeriesColumn] = []
        archives: list[SeriesArchive] = []

        for _ in range(header.column_count):
            derived_name, offset = self._read_utf8(offset, header.live_state_offset)
            raw_name, offset = self._read_utf8(offset, header.live_state_offset)
            ds_type, offset = self._unpack(self._U8, offset, header.live_state_offset)
            try:
                raw_type = DataSourceType(ds_type)
            except ValueError as exc:
                raise NgrrdFormatError(f"Invalid data-source type ordinal: {ds_type}") from exc
            columns.append(SeriesColumn(derived_name, raw_name, raw_type))

        ring_cursor = header.ring_data_offset
        for _ in range(header.archive_count):
            rra_name, offset = self._read_utf8(offset, header.live_state_offset)
            cf_val, offset = self._unpack(self._U8, offset, header.live_state_offset)
            step_sec, offset = self._unpack(self._I32, offset, header.live_state_offset)
            rows, offset = self._unpack(self._I32, offset, header.live_state_offset)
            xff, offset = self._unpack(self._F64, offset, header.live_state_offset)

            if step_sec <= 0:
                raise NgrrdFormatError(f"Archive {rra_name!r} has invalid stepSec: {step_sec}")
            if rows < 0:
                raise NgrrdFormatError(f"Archive {rra_name!r} has invalid rows: {rows}")
            if step_sec % header.base_step_sec != 0:
                raise NgrrdFormatError(
                    f"Archive {rra_name!r} stepSec={step_sec} is not a multiple of "
                    f"baseStepSec={header.base_step_sec}"
                )
            try:
                cf = ConsolidationFunction(cf_val)
            except ValueError as exc:
                raise NgrrdFormatError(f"Invalid consolidation function ordinal: {cf_val}") from exc

            archives.append(
                SeriesArchive(
                    rra_name=rra_name,
                    cf=cf,
                    step_sec=step_sec,
                    rows=rows,
                    xff=xff,
                    group_size=step_sec // header.base_step_sec,
                    ring_base_offset=ring_cursor,
                )
            )
            ring_cursor += rows * header.column_count * self._F64.size

        if offset != header.static_section_bytes:
            raise NgrrdFormatError(
                f"Static section length mismatch: parsed {offset}, header has {header.static_section_bytes}"
            )
        if ring_cursor != header.file_total_bytes:
            raise NgrrdFormatError(
                f"Archive ring size mismatch: calculated {ring_cursor}, header has {header.file_total_bytes}"
            )

        return columns, archives

    def _parse_live_state(self, header: SeriesHeader) -> dict[str, Any]:
        live = self._bytes(header.live_state_offset, header.live_state_bytes)
        stored_crc = struct.unpack_from(">i", live, header.live_state_bytes - 4)[0] & 0xFFFFFFFF
        actual_crc = self._crc32(live[:-4])
        if actual_crc != stored_crc:
            raise NgrrdFormatError(
                "Live-state CRC32 mismatch: "
                f"stored=0x{stored_crc:08X} calculated=0x{actual_crc:08X}"
            )

        d = header.column_count
        a = header.archive_count
        offset = 0
        last_up_epoch_ms, offset = self._unpack_from_bytes(self._I64, live, offset)
        offset += d * (self._F64.size + self._I64.size)
        offset += d * (
            self._F64.size
            + self._I32.size
            + self._F64.size
            + self._F64.size
            + self._F64.size
            + self._I32.size
            + self._I64.size
        )

        cur_row: list[int] = []
        cur_row_epoch_sec: list[int] = []
        for _ in range(a):
            row, epoch_sec = self._RRA_PTR.unpack_from(live, offset)
            cur_row.append(row)
            cur_row_epoch_sec.append(epoch_sec)
            offset += self._RRA_PTR.size

        return {
            "last_up_ms": last_up_epoch_ms,
            "cur_row": cur_row,
            "cur_row_epoch_sec": cur_row_epoch_sec,
        }

    def _archive_rows(
        self, archive_name: str, cf: ConsolidationFunction | str | None
    ) -> list[dict[str, Any]]:
        header = self._require_header()
        mmap_obj = self._require_mmap()
        archive_idx = self._find_archive_index(archive_name, self._coerce_cf(cf))
        arch = self.archives[archive_idx]
        cur_row = self._live_state["cur_row"][archive_idx]
        anchor_sec = self._live_state["cur_row_epoch_sec"][archive_idx]

        if cur_row < 0:
            return []
        if cur_row >= arch.rows:
            raise NgrrdFormatError(f"Archive {arch.rra_name}/{arch.cf.name} has invalid curRow={cur_row}")

        rows: list[dict[str, Any]] = []
        for j in range(arch.rows):
            distance_from_newest = arch.rows - 1 - j
            ts_sec = anchor_sec - (distance_from_newest * arch.step_sec)
            row_idx = (cur_row - distance_from_newest) % arch.rows
            row_offset = arch.ring_base_offset + (row_idx * header.column_count * self._F64.size)

            row: dict[str, Any] = {"ts": ts_sec}
            for col_idx, column in enumerate(self.columns):
                cell_offset = row_offset + (col_idx * self._F64.size)
                if cell_offset + self._F64.size > header.file_total_bytes:
                    raise NgrrdFormatError(f"Ring cell exceeds fileTotalBytes at offset {cell_offset}")
                row[column.derived_name] = self._F64.unpack_from(mmap_obj, cell_offset)[0]
            rows.append(row)
        return rows

    def _find_archive_index(self, archive_name: str, cf: ConsolidationFunction | None) -> int:
        for i, arch in enumerate(self.archives):
            if arch.rra_name == archive_name and (cf is None or arch.cf == cf):
                return i
        suffix = f" with CF {cf.name}" if cf else ""
        raise ValueError(f"Archive not found: {archive_name}{suffix}")

    def _validate_header_layout(self, header: SeriesHeader, file_size: int) -> None:
        if header.base_step_sec <= 0:
            raise NgrrdFormatError(f"Invalid baseStepSec: {header.base_step_sec}")
        if header.column_count < 0 or header.archive_count < 0:
            raise NgrrdFormatError(
                f"Invalid column/archive counts: D={header.column_count}, A={header.archive_count}"
            )
        if header.static_section_bytes < self.FIXED_HEADER_SIZE:
            raise NgrrdFormatError(f"Invalid staticSectionBytes: {header.static_section_bytes}")
        if header.live_state_offset < header.static_section_bytes:
            raise NgrrdFormatError(
                f"liveStateOffset={header.live_state_offset} precedes static section "
                f"ending at {header.static_section_bytes}"
            )
        if header.ring_data_offset < header.live_state_offset + header.live_state_bytes:
            raise NgrrdFormatError(f"ringDataOffset={header.ring_data_offset} precedes live-state end")
        if header.live_state_offset % 8 or header.ring_data_offset % 8:
            raise NgrrdFormatError("liveStateOffset and ringDataOffset must be 8-byte aligned")
        if header.file_total_bytes > file_size:
            raise NgrrdFormatError(
                f"NGRR file is truncated: {file_size} bytes, header expects {header.file_total_bytes}"
            )

        expected_live_bytes = self._live_state_size(header.column_count, header.archive_count)
        if header.live_state_bytes != expected_live_bytes:
            raise NgrrdFormatError(
                f"Invalid liveStateBytes: {header.live_state_bytes}, expected {expected_live_bytes}"
            )

    def _read_utf8(self, offset: int, limit: int) -> tuple[str, int]:
        length, offset = self._unpack(self._U16, offset, limit)
        end = offset + length
        if end > limit:
            raise NgrrdFormatError(f"UTF-8 field at offset {offset} exceeds section limit {limit}")
        try:
            return self._bytes(offset, length).decode("utf-8"), end
        except UnicodeDecodeError as exc:
            raise NgrrdFormatError(f"Invalid UTF-8 field at offset {offset}") from exc

    def _unpack(self, fmt: struct.Struct, offset: int, limit: int) -> tuple[Any, int]:
        end = offset + fmt.size
        if end > limit:
            raise NgrrdFormatError(f"Field at offset {offset} exceeds section limit {limit}")
        return fmt.unpack(self._bytes(offset, fmt.size))[0], end

    @staticmethod
    def _unpack_from_bytes(fmt: struct.Struct, data: bytes, offset: int) -> tuple[Any, int]:
        end = offset + fmt.size
        if end > len(data):
            raise NgrrdFormatError(f"Field at offset {offset} exceeds buffer length {len(data)}")
        return fmt.unpack_from(data, offset)[0], end

    def _bytes(self, offset: int, length: int) -> bytes:
        mmap_obj = self._require_mmap()
        end = offset + length
        if offset < 0 or length < 0 or end > len(mmap_obj):
            raise NgrrdFormatError(f"Requested bytes [{offset}:{end}) exceed mapped size {len(mmap_obj)}")
        return mmap_obj[offset:end]

    def _require_header(self) -> SeriesHeader:
        if self.header is None:
            raise RuntimeError("NgrrdReader is not open")
        return self.header

    def _require_mmap(self) -> mmap.mmap:
        if self._mmap is None:
            raise RuntimeError("NgrrdReader is not open")
        return self._mmap

    @staticmethod
    def _coerce_cf(cf: ConsolidationFunction | str | None) -> ConsolidationFunction | None:
        if cf is None or isinstance(cf, ConsolidationFunction):
            return cf
        try:
            return ConsolidationFunction[cf.upper()]
        except KeyError as exc:
            raise ValueError(f"Unknown consolidation function: {cf}") from exc

    @staticmethod
    def _crc32(data: bytes) -> int:
        return zlib.crc32(data) & 0xFFFFFFFF

    @staticmethod
    def _live_state_size(d: int, a: int) -> int:
        counter_prev = d * (8 + 8)
        pdp = d * (8 + 4 + 8 + 8 + 8 + 4 + 8)
        ptr = a * (4 + 8)
        cdp = a * d * (8 + 4 + 4)
        return 8 + counter_prev + pdp + ptr + cdp + 4
