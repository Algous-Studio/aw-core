from typing import Callable, Dict

from . import storages
from .datastore import Datastore
from .migration import check_for_migration


def get_storage_methods() -> Dict[str, Callable[..., storages.AbstractStorage]]:
    from .storages import MemoryStorage, PeeweeStorage, SqliteStorage, PostgresqlStorage

    methods: Dict[str, Callable[..., storages.AbstractStorage]] = {
        PeeweeStorage.sid: PeeweeStorage,
        MemoryStorage.sid: MemoryStorage,
        SqliteStorage.sid: SqliteStorage,
        PostgresqlStorage.sid: PostgresqlStorage,
    }
    return methods


__all__ = ["Datastore", "get_storage_methods", "check_for_migration"]
