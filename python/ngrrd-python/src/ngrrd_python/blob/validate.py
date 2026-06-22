"""Validação leve do cabeçalho fixo de um arquivo .ngrr (header-only).

Reusa o struct/constantes do ``NgrrdReader`` (sem materializar dicionários/rings),
adequado para varrer milhões de arquivos durante a migração.
"""

from __future__ import annotations

import os
import zlib
from pathlib import Path

from ..reader import NgrrdReader


class NgrrdMigrationError(Exception):
    """Erro na migração de um arquivo .ngrr (ilegível/corrompido/inconsistente)."""


def _check_header(head: bytes, full_size: int, where: str) -> int:
    if len(head) < NgrrdReader.FIXED_HEADER_SIZE:
        raise NgrrdMigrationError(f"{where}: truncado ({len(head)} < {NgrrdReader.FIXED_HEADER_SIZE})")
    fields = NgrrdReader.FIXED_HEADER.unpack(head[: NgrrdReader.FIXED_HEADER_SIZE])
    magic, version = fields[0], fields[1]
    total_bytes, stored_crc = fields[11], fields[12]
    if magic != NgrrdReader.MAGIC:
        raise NgrrdMigrationError(f"{where}: MAGIC inválido ({magic!r})")
    if version != NgrrdReader.CURRENT_VERSION:
        raise NgrrdMigrationError(f"{where}: versão não suportada ({version})")
    actual_crc = zlib.crc32(head[: NgrrdReader.HEADER_CRC_OFFSET]) & 0xFFFFFFFF
    if actual_crc != (stored_crc & 0xFFFFFFFF):
        raise NgrrdMigrationError(f"{where}: header CRC inválido")
    if total_bytes != full_size:
        raise NgrrdMigrationError(
            f"{where}: file_total_bytes ({total_bytes}) != tamanho ({full_size})"
        )
    return total_bytes


def validate_ngrr_header(path: str | os.PathLike[str]) -> int:
    """Valida o header de um arquivo .ngrr e retorna ``file_total_bytes``."""
    p = Path(path)
    size = os.path.getsize(p)
    with open(p, "rb") as f:
        head = f.read(NgrrdReader.FIXED_HEADER_SIZE)
    return _check_header(head, size, str(p))


def validate_ngrr_bytes(data: bytes, where: str = "<bytes>") -> int:
    """Valida o header de uma imagem .ngrr em memória e retorna ``file_total_bytes``."""
    return _check_header(data, len(data), where)
