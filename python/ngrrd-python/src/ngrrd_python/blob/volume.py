"""Escritor de blob volume em Python (cria/abre, aloca regiões, grava .ngrr, checkpoint).

Produz exatamente o formato lido pelo Java ``BlobStorage`` (volume.meta +
shard-NN.blob + catalog.bin + catalog.wal vazio). A alocação espelha o
``ShardAllocator``/``BlobStorage`` Java: bump por shard com padding para não
cruzar fronteira de segmento e crescimento por múltiplos de segmento.
"""

from __future__ import annotations

import os
import uuid as uuidlib
from pathlib import Path
from typing import BinaryIO, Dict, List

from . import spec
from .spec import CatalogEntry


class _Shard:
    __slots__ = ("path", "file", "capacity", "bump")

    def __init__(self, path: Path, file: BinaryIO, capacity: int, bump: int):
        self.path = path
        self.file = file
        self.capacity = capacity
        self.bump = bump


class BlobVolumeWriter:
    """Cria/abre um blob volume e materializa séries como regiões nos shards."""

    def __init__(self, directory: Path, shard_count: int, segment_bytes: int,
                 volume_uuid: bytes, shards: List[_Shard], catalog: Dict[str, CatalogEntry]):
        self.directory = directory
        self.shard_count = shard_count
        self.segment_bytes = segment_bytes
        self.volume_uuid = volume_uuid
        self._shards = shards
        self.catalog = catalog

    # ------------------------------------------------------------------ ciclo de vida

    @classmethod
    def open_or_create(cls, directory: str | os.PathLike[str], shard_count: int,
                       segment_bytes: int, initial_capacity: int) -> "BlobVolumeWriter":
        directory = Path(directory)
        if (directory / "volume.meta").exists():
            return cls.open(directory, expected_shard_count=shard_count, expected_segment_bytes=segment_bytes)
        return cls.create(directory, shard_count, segment_bytes, initial_capacity)

    @classmethod
    def create(cls, directory: str | os.PathLike[str], shard_count: int,
               segment_bytes: int, initial_capacity: int) -> "BlobVolumeWriter":
        directory = Path(directory)
        if segment_bytes < spec.PAGE or segment_bytes % spec.PAGE != 0:
            raise ValueError(f"segment_bytes deve ser múltiplo de {spec.PAGE}: {segment_bytes}")
        directory.mkdir(parents=True, exist_ok=True)
        capacity = _round_up(max(initial_capacity, segment_bytes), segment_bytes)
        volume_uuid = uuidlib.uuid4().bytes
        _write_file_sync(directory / "volume.meta",
                         spec.encode_volume_meta(shard_count, volume_uuid, segment_bytes, 0))
        shards: List[_Shard] = []
        for i in range(shard_count):
            path = directory / spec.shard_file_name(i, shard_count)
            f = open(path, "w+b")
            f.truncate(capacity)
            f.seek(0)
            f.write(spec.encode_superblock(i, shard_count, capacity, spec.HEADER_BYTES, volume_uuid, 0))
            f.flush()
            shards.append(_Shard(path, f, capacity, spec.HEADER_BYTES))
        # catálogo vazio + WAL vazio (Java lê snapshot e dá replay no WAL)
        _write_file_sync(directory / "catalog.bin", spec.encode_catalog(shard_count, volume_uuid, 0, []))
        (directory / "catalog.wal").touch()
        return cls(directory, shard_count, segment_bytes, volume_uuid, shards, {})

    @classmethod
    def open(cls, directory: str | os.PathLike[str], *, expected_shard_count: int | None = None,
             expected_segment_bytes: int | None = None) -> "BlobVolumeWriter":
        directory = Path(directory)
        meta = spec.decode_volume_meta((directory / "volume.meta").read_bytes())
        shard_count = meta["shard_count"]
        segment_bytes = meta["segment_bytes"]
        volume_uuid = meta["volume_uuid"]
        if expected_shard_count is not None and expected_shard_count != shard_count:
            raise ValueError(f"volume tem shard_count={shard_count} (≠ {expected_shard_count})")
        if expected_segment_bytes is not None and expected_segment_bytes != segment_bytes:
            raise ValueError(f"volume tem segment_bytes={segment_bytes} (≠ {expected_segment_bytes})")

        catalog: Dict[str, CatalogEntry] = {}
        catalog_path = directory / "catalog.bin"
        if catalog_path.exists():
            for entry in spec.decode_catalog(catalog_path.read_bytes())["entries"]:
                if entry.state == spec.STATE_LIVE:
                    catalog[entry.key] = entry

        shards: List[_Shard] = []
        for i in range(shard_count):
            path = directory / spec.shard_file_name(i, shard_count)
            sb = spec.decode_superblock(path.read_bytes()[: spec.SUPERBLOCK_BYTES])
            f = open(path, "r+b")
            # bump autoritativo = high-water das entradas LIVE do shard (free-lists vazias)
            bump = spec.HEADER_BYTES
            for entry in catalog.values():
                if entry.shard_id == i:
                    bump = max(bump, entry.region_offset + entry.region_bytes)
            shards.append(_Shard(path, f, sb["capacity_bytes"], bump))
        return cls(directory, shard_count, segment_bytes, volume_uuid, shards, catalog)

    # ------------------------------------------------------------------ alocação/escrita

    def has(self, key: str) -> bool:
        return key in self.catalog

    def allocate(self, key: str, region_bytes: int) -> int:
        """Aloca uma região para ``key`` no shard preferencial (bump + grow). Retorna o offset."""
        if region_bytes > self.segment_bytes:
            raise ValueError(
                f"objeto ({region_bytes} bytes) maior que o segmento ({self.segment_bytes})"
            )
        shard_id = spec.shard_for(key, self.shard_count)
        shard = self._shards[shard_id]
        candidate = shard.bump
        seg = self.segment_bytes
        if candidate // seg != (candidate + region_bytes - 1) // seg:
            candidate = ((candidate // seg) + 1) * seg
        while candidate + region_bytes > shard.capacity:
            self._grow(shard, shard.capacity + seg)
        shard.bump = candidate + region_bytes
        return candidate

    def write_series(self, key: str, region_offset: int, region_bytes: int,
                     object_bytes: int, data: bytes) -> None:
        shard_id = spec.shard_for(key, self.shard_count)
        shard = self._shards[shard_id]
        shard.file.seek(region_offset)
        shard.file.write(data)
        self.catalog[key] = CatalogEntry(key, shard_id, region_offset, region_bytes,
                                         object_bytes, spec.STATE_LIVE)

    def read_region(self, entry: CatalogEntry, length: int | None = None) -> bytes:
        shard = self._shards[entry.shard_id]
        shard.file.seek(entry.region_offset)
        return shard.file.read(entry.object_bytes if length is None else length)

    def shard_id_of(self, key: str) -> int:
        return spec.shard_for(key, self.shard_count)

    def _grow(self, shard: _Shard, new_capacity: int) -> None:
        target = _round_up(new_capacity, self.segment_bytes)
        if target <= shard.capacity:
            return
        shard.file.truncate(target)
        shard.capacity = target

    # ------------------------------------------------------------------ durabilidade

    def checkpoint(self, generation: int = 1) -> None:
        """Persiste superblocks (capacidade/bump) e grava catalog.bin atomicamente."""
        for i, shard in enumerate(self._shards):
            shard.file.flush()
            os.fsync(shard.file.fileno())
            shard.file.seek(0)
            shard.file.write(spec.encode_superblock(i, self.shard_count, shard.capacity,
                                                    shard.bump, self.volume_uuid, generation))
            shard.file.flush()
            os.fsync(shard.file.fileno())
        image = spec.encode_catalog(self.shard_count, self.volume_uuid, generation,
                                    list(self.catalog.values()))
        tmp = self.directory / "catalog.bin.tmp"
        _write_file_sync(tmp, image)
        os.replace(tmp, self.directory / "catalog.bin")
        # WAL fica vazio após um snapshot (Java rotaciona/limpa)
        (self.directory / "catalog.wal").write_bytes(b"")

    def close(self) -> None:
        for shard in self._shards:
            shard.file.close()

    def __enter__(self) -> "BlobVolumeWriter":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


def _round_up(value: int, multiple: int) -> int:
    return ((value + multiple - 1) // multiple) * multiple


def _write_file_sync(path: Path, data: bytes) -> None:
    with open(path, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
