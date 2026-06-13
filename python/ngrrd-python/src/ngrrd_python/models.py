"""Public data model for the NGRR binary time-series format."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any


class DataSourceType(IntEnum):
    """Source type stored in the NGRR data-source dictionary."""

    COUNTER = 0
    GAUGE = 1
    DERIVE = 2
    ABSOLUTE = 3


class ConsolidationFunction(IntEnum):
    """Consolidation function attached to each physical archive."""

    AVERAGE = 0
    MAX = 1
    MIN = 2
    LAST = 3


@dataclass(frozen=True)
class SeriesColumn:
    """A persisted column and the raw data source that originated it."""

    derived_name: str
    raw_name: str
    raw_type: DataSourceType

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of this column."""

        return {
            "derived_name": self.derived_name,
            "raw_name": self.raw_name,
            "raw_type": self.raw_type.name,
            "raw_type_ordinal": int(self.raw_type),
        }


@dataclass(frozen=True)
class SeriesArchive:
    """A physical archive, equivalent to a flattened ``(rra, cf)`` pair."""

    rra_name: str
    cf: ConsolidationFunction
    step_sec: int
    rows: int
    xff: float
    group_size: int
    ring_base_offset: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of this archive."""

        return {
            "rra_name": self.rra_name,
            "cf": self.cf.name,
            "cf_ordinal": int(self.cf),
            "step_sec": self.step_sec,
            "rows": self.rows,
            "xff": self.xff,
            "group_size": self.group_size,
            "ring_base_offset": self.ring_base_offset,
        }


@dataclass(frozen=True)
class SeriesHeader:
    """Decoded 96-byte fixed header from an NGRR series file."""

    version: int
    schema_revision: int
    base_step_sec: int
    definition_hash: bytes
    column_count: int
    archive_count: int
    static_section_bytes: int
    live_state_offset: int
    live_state_bytes: int
    ring_data_offset: int
    file_total_bytes: int

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable view of the decoded header."""

        return {
            "version": self.version,
            "schema_revision": self.schema_revision,
            "base_step_sec": self.base_step_sec,
            "definition_hash": self.definition_hash.hex(),
            "column_count": self.column_count,
            "archive_count": self.archive_count,
            "static_section_bytes": self.static_section_bytes,
            "live_state_offset": self.live_state_offset,
            "live_state_bytes": self.live_state_bytes,
            "ring_data_offset": self.ring_data_offset,
            "file_total_bytes": self.file_total_bytes,
        }
