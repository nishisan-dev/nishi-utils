"""CLI ``ngrrd-blob-migrate``: migra/verifica/inspeciona blob volumes.

Códigos de saída: 0 = ok; 1 = concluído com falhas (séries ilegíveis / verify
falhou); 2 = uso inválido.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import List, Optional

from . import migrate as migrate_mod
from . import verify as verify_mod
from .volume import BlobVolumeWriter


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ngrrd-blob-migrate",
        description="Migra um diretório de arquivos .ngrr para um blob volume sharded.")
    sub = parser.add_subparsers(dest="command", required=True)

    m = sub.add_parser("migrate", help="migra .ngrr -> blob volume")
    m.add_argument("source_root", help="raiz que contém <series_prefix>/**/*.ngrr")
    m.add_argument("target_volume", help="diretório do blob volume de destino")
    m.add_argument("--series-prefix", default="series")
    m.add_argument("--shards", type=int, default=migrate_mod.DEFAULT_SHARD_COUNT)
    m.add_argument("--segment-bytes", type=int, default=migrate_mod.DEFAULT_SEGMENT_BYTES)
    m.add_argument("--initial-capacity", type=int, default=None)
    m.add_argument("--resume", action="store_true", help="pula séries já migradas (no catálogo)")
    m.add_argument("--dry-run", action="store_true", help="relata sem escrever")
    m.add_argument("--verify", action="store_true", help="roda verify ao final")
    m.add_argument("--json", action="store_true")

    v = sub.add_parser("verify", help="verifica fidelidade de um blob volume")
    v.add_argument("target_volume")
    v.add_argument("--source-root", default=None,
                   help="se dado, confere paridade byte-a-byte com os .ngrr de origem")
    v.add_argument("--series-prefix", default="series")
    v.add_argument("--json", action="store_true")

    i = sub.add_parser("inspect", help="resumo do volume (shards, contagem, distribuição)")
    i.add_argument("target_volume")
    i.add_argument("--json", action="store_true")
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "migrate":
        return _do_migrate(args)
    if args.command == "verify":
        return _do_verify(args)
    if args.command == "inspect":
        return _do_inspect(args)
    return 2


def _do_migrate(args) -> int:
    report = migrate_mod.migrate(
        args.source_root, args.target_volume,
        series_prefix=args.series_prefix, shard_count=args.shards,
        segment_bytes=args.segment_bytes, initial_capacity=args.initial_capacity,
        resume=args.resume, dry_run=args.dry_run)
    failed = bool(report.unreadable)
    if args.verify and not args.dry_run:
        vreport = verify_mod.verify(args.target_volume, source_root=args.source_root)
        failed = failed or not vreport.healthy
        if args.json:
            print(json.dumps({"migrate": report.as_dict(), "verify": vreport.as_dict()}, indent=2))
        else:
            _print_migrate(report)
            _print_verify(vreport)
        return 1 if failed else 0
    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        _print_migrate(report)
    return 1 if failed else 0


def _do_verify(args) -> int:
    report = verify_mod.verify(args.target_volume, source_root=args.source_root,
                               series_prefix=args.series_prefix)
    if args.json:
        print(json.dumps(report.as_dict(), indent=2))
    else:
        _print_verify(report)
    return 0 if report.healthy else 1


def _do_inspect(args) -> int:
    volume = BlobVolumeWriter.open(args.target_volume)
    try:
        from collections import Counter
        per_shard: Counter = Counter()
        for entry in volume.catalog.values():
            per_shard[entry.shard_id] += 1
        info = {
            "shard_count": volume.shard_count,
            "segment_bytes": volume.segment_bytes,
            "series": len(volume.catalog),
            "per_shard": dict(sorted(per_shard.items())),
        }
    finally:
        volume.close()
    if args.json:
        print(json.dumps(info, indent=2))
    else:
        print(f"shards={info['shard_count']} segment_bytes={info['segment_bytes']} "
              f"series={info['series']}")
        for shard_id, count in info["per_shard"].items():
            print(f"  shard {shard_id}: {count}")
    return 0


def _print_migrate(report: migrate_mod.MigrationReport) -> None:
    tag = "[dry-run] " if report.dry_run else ""
    print(f"{tag}migrated={report.migrated} skipped={report.skipped} "
          f"bytes={report.total_object_bytes} unreadable={len(report.unreadable)}")
    for key, reason in report.unreadable:
        print(f"  ILEGÍVEL {key}: {reason}", file=sys.stderr)


def _print_verify(report: verify_mod.VerifyReport) -> None:
    print(f"verify: checked={report.checked} ok={report.ok} failures={len(report.failures)} "
          f"healthy={report.healthy}")
    for key, reason in report.failures:
        print(f"  FALHA {key}: {reason}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
