"""ngrrd sharded blob volume tooling (migration + verification).

Mirrors the Java ``dev.nishisan.utils.oss.storage.blob`` on-disk format. See
``doc/oss/ngrrd-blob-volume.md`` for the normative byte layout.
"""

from .spec import CatalogEntry, shard_for, align_page
from .migrate import migrate, MigrationReport
from .verify import verify, VerifyReport

__all__ = [
    "CatalogEntry",
    "shard_for",
    "align_page",
    "migrate",
    "MigrationReport",
    "verify",
    "VerifyReport",
]
