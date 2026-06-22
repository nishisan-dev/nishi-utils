"""Migração e verificação end-to-end (lado Python)."""

from pathlib import Path

import pytest

from ngrrd_python.blob import spec
from ngrrd_python.blob.migrate import migrate
from ngrrd_python.blob.verify import verify
from ngrrd_python.blob.volume import BlobVolumeWriter

from _ngrr_factory import make_ngrr

CORPUS = {
    "series/device:r1/iface:eth0.ngrr": 8192,
    "series/device:r2/iface:eth1.ngrr": 12288,
    "series/device:r3/iface:eth2.ngrr": 5000,  # objeto < região alinhada (8192)
    "series/edge/x.ngrr": 4096,
}


def _write_corpus(root: Path, corpus=CORPUS) -> None:
    for relpath, size in corpus.items():
        p = root / relpath
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(make_ngrr(size, fill=size))


def test_migrate_then_verify_roundtrip(tmp_path):
    src = tmp_path / "src"
    vol = tmp_path / "vol"
    _write_corpus(src)

    report = migrate(src, vol, shard_count=4, segment_bytes=1 << 20, initial_capacity=1 << 20)
    assert report.migrated == len(CORPUS)
    assert report.skipped == 0
    assert not report.unreadable

    vreport = verify(vol, source_root=src)
    assert vreport.healthy
    assert vreport.checked == len(CORPUS)
    assert vreport.ok == len(CORPUS)


def test_get_returns_exact_object_bytes(tmp_path):
    src = tmp_path / "src"
    vol = tmp_path / "vol"
    _write_corpus(src)
    migrate(src, vol, shard_count=4, segment_bytes=1 << 20, initial_capacity=1 << 20)

    writer = BlobVolumeWriter.open(vol)
    try:
        entry = writer.catalog["series/device:r3/iface:eth2.ngrr"]
        assert entry.object_bytes == 5000
        assert entry.region_bytes == spec.align_page(5000)  # 8192
        data = writer.read_region(entry)
        assert len(data) == 5000
        assert data == (src / "series/device:r3/iface:eth2.ngrr").read_bytes()
    finally:
        writer.close()


def test_dry_run_writes_nothing(tmp_path):
    src = tmp_path / "src"
    vol = tmp_path / "vol"
    _write_corpus(src)
    report = migrate(src, vol, shard_count=4, segment_bytes=1 << 20, dry_run=True)
    assert report.dry_run
    assert report.migrated == len(CORPUS)
    assert report.total_object_bytes == sum(CORPUS.values())
    assert not (vol / "volume.meta").exists()


def test_resume_skips_already_migrated(tmp_path):
    src = tmp_path / "src"
    vol = tmp_path / "vol"
    _write_corpus(src)
    migrate(src, vol, shard_count=4, segment_bytes=1 << 20, initial_capacity=1 << 20)
    again = migrate(src, vol, shard_count=4, segment_bytes=1 << 20, initial_capacity=1 << 20, resume=True)
    assert again.migrated == 0
    assert again.skipped == len(CORPUS)


def test_unreadable_file_is_reported_not_fatal(tmp_path):
    src = tmp_path / "src"
    vol = tmp_path / "vol"
    _write_corpus(src)
    # arquivo corrompido (header truncado)
    bad = src / "series/corrupt.ngrr"
    bad.write_bytes(b"NGRR" + b"\x00" * 10)

    report = migrate(src, vol, shard_count=4, segment_bytes=1 << 20, initial_capacity=1 << 20)
    assert report.migrated == len(CORPUS)
    assert len(report.unreadable) == 1
    assert report.unreadable[0][0] == "series/corrupt.ngrr"


def test_growth_when_many_series_in_one_shard(tmp_path):
    src = tmp_path / "src"
    vol = tmp_path / "vol"
    # 1 shard, segmento de 64 KiB: várias séries de 8 KiB forçam crescimento.
    corpus = {f"series/s{i}.ngrr": 8192 for i in range(12)}
    _write_corpus(src, corpus)
    report = migrate(src, vol, shard_count=1, segment_bytes=64 * 1024, initial_capacity=64 * 1024)
    assert report.migrated == 12
    vreport = verify(vol, source_root=src)
    assert vreport.healthy
