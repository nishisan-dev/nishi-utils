"""Public data model for the NGRR binary time-series format."""

from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum


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
