"""Binary format of an ngrrd blob volume (v1).

Byte-for-byte mirror of the Java codecs in
``dev.nishisan.utils.oss.storage.blob`` (VolumeMetadata, ShardSuperblock,
BlobCatalogCodec, BlobRouting). Big-endian everywhere; CRC-32 (zlib/IEEE).
See ``doc/oss/ngrrd-blob-volume.md``.
"""

from __future__ import annotations

import hashlib
import struct
import zlib
from dataclasses import dataclass
from typing import List

PAGE = 4096

# ---- volume.meta (§4) ----
VOLUME_MAGIC = b"NGRRVOL1"
VOLUME_VERSION = 1
VOLUME_BYTES = 56
_VOLUME_HEAD = struct.Struct(">8sHHI16sQHHQ")  # 52 bytes, sem o CRC final
VOLUME_CRC_OFFSET = 52

ROUTING_SHA256_PREFIX64 = 1
ROUTING_VERSION = 1

# ---- superblock do shard (§5) ----
SUPERBLOCK_MAGIC = b"NGRRBLOB"
SUPERBLOCK_VERSION = 1
SUPERBLOCK_BYTES = 4096
HEADER_BYTES = 4096  # início da data region
_SUPERBLOCK_HEAD = struct.Struct(">8sHHIIIQQQ16sQ")  # 72 bytes
SUPERBLOCK_CRC_OFFSET = 4092

# ---- catálogo (§6) ----
CATALOG_MAGIC = b"NGRRCTLG"
CATALOG_VERSION = 1
CATALOG_HEADER_BYTES = 64
_CATALOG_HEAD = struct.Struct(">8sHHI16sQI")  # 44 bytes (antes do reserved[16] + crc)
CATALOG_HEADER_CRC_OFFSET = 60
_ENTRY_FIXED = struct.Struct(">IQQQB")  # shardId, regionOffset, regionBytes, objectBytes, state

STATE_LIVE = 1
STATE_DELETED = 2


class NgrrdBlobFormatError(ValueError):
    """Raised when a blob volume structure is malformed or corrupt."""


@dataclass(frozen=True)
class CatalogEntry:
    key: str
    shard_id: int
    region_offset: int
    region_bytes: int
    object_bytes: int
    state: int = STATE_LIVE


def crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


def align_page(n: int) -> int:
    return ((n + PAGE - 1) // PAGE) * PAGE


def shard_for(catalog_key: str, shard_count: int) -> int:
    """SHA256_PREFIX64: uint64(SHA-256(key)[:8] big-endian) mod N. Ver §8."""
    if shard_count <= 0:
        raise ValueError(f"shard_count deve ser > 0: {shard_count}")
    digest = hashlib.sha256(catalog_key.encode("utf-8")).digest()
    h64 = int.from_bytes(digest[:8], "big")
    return h64 % shard_count


def shard_file_name(shard_id: int, shard_count: int) -> str:
    width = max(2, len(str(shard_count - 1)))
    return f"shard-{shard_id:0{width}d}.blob"


# --------------------------------------------------------------------- volume.meta

def encode_volume_meta(shard_count: int, volume_uuid: bytes, segment_bytes: int,
                       generation: int) -> bytes:
    head = _VOLUME_HEAD.pack(VOLUME_MAGIC, VOLUME_VERSION, 0, shard_count, volume_uuid,
                             segment_bytes, ROUTING_SHA256_PREFIX64, ROUTING_VERSION, generation)
    return head + struct.pack(">I", crc32(head))


def decode_volume_meta(image: bytes) -> dict:
    if len(image) < VOLUME_BYTES:
        raise NgrrdBlobFormatError(f"volume.meta truncado: {len(image)}")
    if image[:8] != VOLUME_MAGIC:
        raise NgrrdBlobFormatError("volume.meta com MAGIC inválido")
    stored = struct.unpack_from(">I", image, VOLUME_CRC_OFFSET)[0]
    if stored != crc32(image[:VOLUME_CRC_OFFSET]):
        raise NgrrdBlobFormatError("volume.meta com CRC inválido")
    (_, version, _, shard_count, uuid_b, segment_bytes,
     routing_algo, routing_version, generation) = _VOLUME_HEAD.unpack_from(image, 0)
    if version != VOLUME_VERSION:
        raise NgrrdBlobFormatError(f"volume.meta versão não suportada: {version}")
    return {
        "shard_count": shard_count,
        "volume_uuid": uuid_b,
        "segment_bytes": segment_bytes,
        "routing_algorithm": routing_algo,
        "routing_version": routing_version,
        "generation": generation,
    }


# --------------------------------------------------------------------- superblock

def encode_superblock(shard_id: int, shard_count: int, capacity_bytes: int,
                      bump_cursor: int, volume_uuid: bytes, generation: int) -> bytes:
    head = _SUPERBLOCK_HEAD.pack(SUPERBLOCK_MAGIC, SUPERBLOCK_VERSION, 0, shard_id, shard_count,
                                 0, HEADER_BYTES, capacity_bytes, bump_cursor, volume_uuid, generation)
    buf = bytearray(SUPERBLOCK_BYTES)
    buf[: len(head)] = head
    struct.pack_into(">I", buf, SUPERBLOCK_CRC_OFFSET, crc32(bytes(buf[:SUPERBLOCK_CRC_OFFSET])))
    return bytes(buf)


def decode_superblock(image: bytes) -> dict:
    if len(image) < SUPERBLOCK_BYTES:
        raise NgrrdBlobFormatError(f"superblock truncado: {len(image)}")
    if image[:8] != SUPERBLOCK_MAGIC:
        raise NgrrdBlobFormatError("superblock com MAGIC inválido")
    stored = struct.unpack_from(">I", image, SUPERBLOCK_CRC_OFFSET)[0]
    if stored != crc32(image[:SUPERBLOCK_CRC_OFFSET]):
        raise NgrrdBlobFormatError("superblock com CRC inválido")
    (_, version, _, shard_id, shard_count, _, header_bytes,
     capacity_bytes, bump_cursor, uuid_b, generation) = _SUPERBLOCK_HEAD.unpack_from(image, 0)
    if version != SUPERBLOCK_VERSION:
        raise NgrrdBlobFormatError(f"superblock versão não suportada: {version}")
    return {
        "shard_id": shard_id,
        "shard_count": shard_count,
        "header_bytes": header_bytes,
        "capacity_bytes": capacity_bytes,
        "bump_cursor": bump_cursor,
        "volume_uuid": uuid_b,
        "generation": generation,
    }


# --------------------------------------------------------------------- catálogo

def encode_catalog(shard_count: int, volume_uuid: bytes, generation: int,
                   entries: List[CatalogEntry]) -> bytes:
    head44 = _CATALOG_HEAD.pack(CATALOG_MAGIC, CATALOG_VERSION, 0, shard_count, volume_uuid,
                                generation, len(entries))
    header = head44 + b"\x00" * 16  # 60 bytes
    header += struct.pack(">I", crc32(header))  # 64 bytes

    parts = []
    for e in entries:
        kb = e.key.encode("utf-8")
        if len(kb) > 0xFFFF:
            raise NgrrdBlobFormatError(f"catalogKey excede 65535 bytes: {e.key}")
        body = struct.pack(">H", len(kb)) + kb + _ENTRY_FIXED.pack(
            e.shard_id, e.region_offset, e.region_bytes, e.object_bytes, e.state)
        parts.append(body + struct.pack(">I", crc32(body)))
    entries_blob = b"".join(parts)
    trailer = struct.pack(">I", crc32(entries_blob))
    return header + entries_blob + trailer


def decode_catalog(image: bytes) -> dict:
    if len(image) < CATALOG_HEADER_BYTES + 4:
        raise NgrrdBlobFormatError(f"catalog.bin truncado: {len(image)}")
    if image[:8] != CATALOG_MAGIC:
        raise NgrrdBlobFormatError("catalog.bin com MAGIC inválido")
    if struct.unpack_from(">I", image, CATALOG_HEADER_CRC_OFFSET)[0] != crc32(image[:CATALOG_HEADER_CRC_OFFSET]):
        raise NgrrdBlobFormatError("catalog.bin com header CRC inválido")
    trailer_off = len(image) - 4
    if struct.unpack_from(">I", image, trailer_off)[0] != crc32(image[CATALOG_HEADER_BYTES:trailer_off]):
        raise NgrrdBlobFormatError("catalog.bin com trailer CRC inválido")

    (_, version, _, shard_count, uuid_b, generation, entry_count) = _CATALOG_HEAD.unpack_from(image, 0)
    if version != CATALOG_VERSION:
        raise NgrrdBlobFormatError(f"catalog.bin versão não suportada: {version}")

    entries: List[CatalogEntry] = []
    pos = CATALOG_HEADER_BYTES
    for i in range(entry_count):
        entry_start = pos
        (key_len,) = struct.unpack_from(">H", image, pos)
        pos += 2
        key = image[pos:pos + key_len].decode("utf-8")
        pos += key_len
        shard_id, region_offset, region_bytes, object_bytes, state = _ENTRY_FIXED.unpack_from(image, pos)
        pos += _ENTRY_FIXED.size
        body = image[entry_start:pos]
        (stored_crc,) = struct.unpack_from(">I", image, pos)
        pos += 4
        if stored_crc != crc32(body):
            raise NgrrdBlobFormatError(f"catalog.bin com CRC inválido na entrada {i}")
        entries.append(CatalogEntry(key, shard_id, region_offset, region_bytes, object_bytes, state))
    if pos != trailer_off:
        raise NgrrdBlobFormatError("catalog.bin: entryCount inconsistente com o conteúdo")
    return {
        "shard_count": shard_count,
        "volume_uuid": uuid_b,
        "generation": generation,
        "entries": entries,
    }
