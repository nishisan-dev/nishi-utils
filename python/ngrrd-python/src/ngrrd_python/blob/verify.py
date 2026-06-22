"""Verificação de fidelidade pós-migração de um blob volume."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional, Tuple

from .validate import NgrrdMigrationError, validate_ngrr_bytes
from .volume import BlobVolumeWriter


@dataclass
class VerifyReport:
    checked: int = 0
    ok: int = 0
    per_shard: Counter = field(default_factory=Counter)
    failures: List[Tuple[str, str]] = field(default_factory=list)  # (key, motivo)

    @property
    def healthy(self) -> bool:
        return not self.failures

    def as_dict(self) -> dict:
        return {
            "checked": self.checked,
            "ok": self.ok,
            "per_shard": dict(sorted(self.per_shard.items())),
            "failures": [{"key": k, "reason": r} for k, r in self.failures],
            "healthy": self.healthy,
        }


def verify(target_dir: str | Path, *, source_root: Optional[str | Path] = None,
           series_prefix: str = "series") -> VerifyReport:
    """Valida cada slot do volume (header + tamanho) e, se ``source_root`` for dado,
    confere paridade byte-a-byte com o arquivo .ngrr de origem."""
    report = VerifyReport()
    volume = BlobVolumeWriter.open(target_dir)
    try:
        for key, entry in sorted(volume.catalog.items()):
            report.checked += 1
            report.per_shard[entry.shard_id] += 1
            data = volume.read_region(entry)
            try:
                if len(data) != entry.object_bytes:
                    raise NgrrdMigrationError(
                        f"slot devolveu {len(data)} bytes (esperado {entry.object_bytes})")
                validate_ngrr_bytes(data, where=key)
                if source_root is not None:
                    src = (Path(source_root) / key).read_bytes()
                    if src != data:
                        raise NgrrdMigrationError("conteúdo do slot difere do .ngrr de origem")
                report.ok += 1
            except (NgrrdMigrationError, OSError) as e:
                report.failures.append((key, str(e)))
    finally:
        volume.close()
    return report
