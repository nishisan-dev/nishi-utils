"""Helper de teste: constrói uma imagem .ngrr mínima com header válido.

Suficiente para os testes de migração (que validam o header e copiam bytes
verbatim). A fidelidade total com a engine Java é coberta pelos testes
cross-language (Fase 5), que usam arquivos .ngrr gerados pelo próprio Java.
"""

from __future__ import annotations

import struct
import zlib

_FIXED = struct.Struct(">4sHHi32siiQQQQQi")  # = NgrrdReader.FIXED_HEADER


def make_ngrr(total_bytes: int, *, version: int = 1, columns: int = 1, archives: int = 1,
              fill: int = 0) -> bytes:
    """Imagem .ngrr de exatamente ``total_bytes`` bytes com header fixo válido."""
    if total_bytes < _FIXED.size:
        raise ValueError(f"total_bytes deve ser >= {_FIXED.size}")
    buf = bytearray(total_bytes)
    if fill:
        for i in range(_FIXED.size, total_bytes):
            buf[i] = (fill + i) & 0xFF
    _FIXED.pack_into(buf, 0, b"NGRR", version, 0, 300, b"\x00" * 32,
                     columns, archives, 96, 96, 0, 96, total_bytes, 0)
    crc = zlib.crc32(bytes(buf[:92])) & 0xFFFFFFFF
    struct.pack_into(">I", buf, 92, crc)
    return bytes(buf)
