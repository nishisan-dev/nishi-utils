"""Round-trip e detecção de corrupção dos codecs do blob volume."""

import uuid

import pytest

from ngrrd_python.blob import spec
from ngrrd_python.blob.spec import CatalogEntry, NgrrdBlobFormatError


def _uuid16():
    return uuid.UUID("0123456789abcdef0123456789abcdef").bytes


def test_volume_meta_round_trip():
    image = spec.encode_volume_meta(64, _uuid16(), 1 << 30, 7)
    assert len(image) == spec.VOLUME_BYTES
    decoded = spec.decode_volume_meta(image)
    assert decoded["shard_count"] == 64
    assert decoded["segment_bytes"] == 1 << 30
    assert decoded["generation"] == 7
    assert decoded["volume_uuid"] == _uuid16()
    assert decoded["routing_algorithm"] == spec.ROUTING_SHA256_PREFIX64


def test_volume_meta_rejects_corruption():
    image = bytearray(spec.encode_volume_meta(64, _uuid16(), 1 << 30, 1))
    image[20] ^= 0xFF
    with pytest.raises(NgrrdBlobFormatError):
        spec.decode_volume_meta(bytes(image))


def test_superblock_round_trip():
    image = spec.encode_superblock(7, 64, 1 << 30, 4096 + 3 * 8192, _uuid16(), 42)
    assert len(image) == spec.SUPERBLOCK_BYTES
    decoded = spec.decode_superblock(image)
    assert decoded["shard_id"] == 7
    assert decoded["shard_count"] == 64
    assert decoded["capacity_bytes"] == 1 << 30
    assert decoded["bump_cursor"] == 4096 + 3 * 8192
    assert decoded["generation"] == 42


def test_catalog_round_trip_with_multibyte_keys():
    entries = [
        CatalogEntry("series/device:r1/iface:eth0.ngrr", 37, 4096, 8192, 8000, spec.STATE_LIVE),
        CatalogEntry("series/höst=π.ngrr", 15, 12288, 8192, 8100, spec.STATE_LIVE),
    ]
    image = spec.encode_catalog(64, _uuid16(), 99, entries)
    decoded = spec.decode_catalog(image)
    assert decoded["generation"] == 99
    assert decoded["entries"] == entries


def test_catalog_empty_round_trip():
    image = spec.encode_catalog(64, _uuid16(), 0, [])
    assert spec.decode_catalog(image)["entries"] == []


def test_catalog_rejects_entry_corruption():
    entries = [CatalogEntry("series/a.ngrr", 1, 4096, 8192, 8000, spec.STATE_LIVE)]
    image = bytearray(spec.encode_catalog(64, _uuid16(), 1, entries))
    image[-8] ^= 0xFF
    with pytest.raises(NgrrdBlobFormatError):
        spec.decode_catalog(bytes(image))


def test_shard_file_name_zero_padding():
    assert spec.shard_file_name(7, 64) == "shard-07.blob"
    assert spec.shard_file_name(7, 1) == "shard-07.blob"  # largura mínima 2
    assert spec.shard_file_name(5, 1000) == "shard-005.blob"
