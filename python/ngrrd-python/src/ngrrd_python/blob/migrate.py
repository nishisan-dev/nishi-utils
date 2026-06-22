"""Migração de um diretório de arquivos .ngrr para um blob volume sharded.

Insight central: o conteúdo de um slot é byte-a-byte idêntico ao .ngrr standalone,
logo migrar = validar o header, alocar uma região e copiar os bytes verbatim.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from . import spec
from .validate import NgrrdMigrationError, validate_ngrr_header
from .volume import BlobVolumeWriter
from .walk import iter_series

DEFAULT_SHARD_COUNT = 64
DEFAULT_SEGMENT_BYTES = 1 << 30  # 1 GiB


@dataclass
class MigrationReport:
    migrated: int = 0
    skipped: int = 0
    total_object_bytes: int = 0
    per_shard: Counter = field(default_factory=Counter)
    unreadable: List[Tuple[str, str]] = field(default_factory=list)  # (key, motivo)
    dry_run: bool = False

    def as_dict(self) -> dict:
        return {
            "migrated": self.migrated,
            "skipped": self.skipped,
            "total_object_bytes": self.total_object_bytes,
            "per_shard": dict(sorted(self.per_shard.items())),
            "unreadable": [{"key": k, "reason": r} for k, r in self.unreadable],
            "dry_run": self.dry_run,
        }


def migrate(source_root: str | Path, target_dir: str | Path, *,
            series_prefix: str = "series",
            shard_count: int = DEFAULT_SHARD_COUNT,
            segment_bytes: int = DEFAULT_SEGMENT_BYTES,
            initial_capacity: Optional[int] = None,
            resume: bool = False,
            dry_run: bool = False,
            checkpoint_interval: int = 5000,
            on_progress: Optional[Callable[[int], None]] = None) -> MigrationReport:
    """Migra ``source_root/<series_prefix>/**/*.ngrr`` para o blob volume em ``target_dir``."""
    report = MigrationReport(dry_run=dry_run)

    if dry_run:
        for sf in iter_series(source_root, series_prefix):
            try:
                object_bytes = validate_ngrr_header(sf.path)
            except NgrrdMigrationError as e:
                report.unreadable.append((sf.catalog_key, str(e)))
                continue
            report.migrated += 1
            report.total_object_bytes += object_bytes
            report.per_shard[spec.shard_for(sf.catalog_key, shard_count)] += 1
        return report

    cap = initial_capacity if initial_capacity is not None else segment_bytes
    volume = BlobVolumeWriter.open_or_create(target_dir, shard_count, segment_bytes, cap)
    generation = 1
    try:
        for sf in iter_series(source_root, series_prefix):
            if resume and volume.has(sf.catalog_key):
                report.skipped += 1
                continue
            try:
                object_bytes = validate_ngrr_header(sf.path)
            except NgrrdMigrationError as e:
                report.unreadable.append((sf.catalog_key, str(e)))
                continue
            region_bytes = spec.align_page(object_bytes)
            offset = volume.allocate(sf.catalog_key, region_bytes)
            data = Path(sf.path).read_bytes()
            volume.write_series(sf.catalog_key, offset, region_bytes, object_bytes, data)
            report.migrated += 1
            report.total_object_bytes += object_bytes
            report.per_shard[volume.shard_id_of(sf.catalog_key)] += 1
            if on_progress is not None and report.migrated % 1000 == 0:
                on_progress(report.migrated)
            if report.migrated % checkpoint_interval == 0:
                volume.checkpoint(generation)
                generation += 1
        volume.checkpoint(generation)
    finally:
        volume.close()
    return report
