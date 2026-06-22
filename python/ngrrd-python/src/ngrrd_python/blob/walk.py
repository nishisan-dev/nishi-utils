"""Varredura preguiçosa de uma árvore de arquivos .ngrr (layout LocalDiskStorage)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator


@dataclass(frozen=True)
class SeriesFile:
    """Um .ngrr encontrado na varredura.

    ``catalog_key`` é o caminho relativo ao ``source_root`` (com separador '/'),
    idêntico à storage key usada pelo Java (ex.: ``series/device:r1/iface:eth0.ngrr``).
    """
    catalog_key: str
    path: Path


def iter_series(source_root: str | os.PathLike[str], series_prefix: str = "series") -> Iterator[SeriesFile]:
    """Itera (lazy) todos os ``*.ngrr`` sob ``source_root/series_prefix``.

    Ordena diretórios/arquivos para varredura determinística. Não materializa a
    lista — seguro para milhões de arquivos.
    """
    root = Path(source_root)
    base = root / series_prefix if series_prefix else root
    if not base.is_dir():
        return
    for dirpath, dirnames, filenames in os.walk(base):
        dirnames.sort()
        for filename in sorted(filenames):
            if not filename.endswith(".ngrr"):
                continue
            full = Path(dirpath) / filename
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            yield SeriesFile(rel, full)
