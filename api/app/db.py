import psycopg
from psycopg.rows import dict_row

from . import config


def primary(row_factory=None):
    """Connection to the always-on source database."""
    return psycopg.connect(
        config.PRIMARY_DSN, autocommit=True, connect_timeout=5, row_factory=row_factory
    )


def replica():
    """Read-only connection to the currently recovered replica."""
    return psycopg.connect(
        config.REPLICA_DSN, autocommit=True, connect_timeout=3, row_factory=dict_row
    )
