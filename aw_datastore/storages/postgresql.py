# aw_datastore/storages/postgresql.py
import os
import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone
from typing import Optional, List

from aw_core.models import Event
from .abstract import AbstractStorage

CREATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS buckets (
    rowid    BIGSERIAL PRIMARY KEY,
    id       TEXT UNIQUE NOT NULL,
    name     TEXT,
    type     TEXT NOT NULL,
    client   TEXT NOT NULL,
    hostname TEXT NOT NULL,
    created  TEXT NOT NULL,
    datastr  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
    id        BIGSERIAL PRIMARY KEY,
    bucketrow BIGINT NOT NULL REFERENCES buckets(rowid) ON DELETE CASCADE,
    starttime BIGINT NOT NULL,
    endtime   BIGINT NOT NULL,
    datastr   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS event_index_starttime ON events(bucketrow, starttime);
CREATE INDEX IF NOT EXISTS event_index_endtime   ON events(bucketrow, endtime);
"""

MAX_TIMESTAMP = 2**63 - 1  # как в sqlite-бэкенде

class PostgresqlStorage(AbstractStorage):
    sid = "postgresql"

    def __init__(self, testing: bool, dsn: Optional[str] = None) -> None:
        self.testing = testing
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            raise ValueError("POSTGRES_DSN must be provided or pass dsn=...")

        self.conn = psycopg2.connect(self.dsn)
        # В gunicorn sync-воркерах соединение процесс-локальное, ок
        self.conn.autocommit = True

        with self.conn.cursor() as cur:
            cur.execute(CREATE_SCHEMA)

    # совместимость с интерфейсом
    def commit(self):
        try:
            self.conn.commit()
        except Exception:
            pass

    # --- helpers ---
    def _bucket_rowid(self, bucket_id: str) -> int:
        with self.conn.cursor() as cur:
            cur.execute("SELECT rowid FROM buckets WHERE id=%s", (bucket_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Bucket '{bucket_id}' does not exist")
            return int(row[0])

    # --- buckets metadata ---
    def buckets(self):
        with self.conn.cursor() as cur:
            cur.execute("SELECT id,name,type,client,hostname,created,datastr FROM buckets")
            rows = cur.fetchall()
        return {
            r[0]: {
                "id": r[0],
                "name": r[1],
                "type": r[2],
                "client": r[3],
                "hostname": r[4],
                "created": r[5],
                "data": json.loads(r[6] or "{}"),
            }
            for r in rows
        }

    def create_bucket(
        self,
        bucket_id: str,
        type_id: str,
        client: str,
        hostname: str,
        created: str,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        with self.conn.cursor() as cur:
            # защита от гонок: если уже есть — просто ничего не делаем
            cur.execute(
                """INSERT INTO buckets(id,name,type,client,hostname,created,datastr)
                   VALUES (%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (id) DO NOTHING""",
                (bucket_id, name, type_id, client, hostname, created, json.dumps(data or {})),
            )
        return self.get_metadata(bucket_id)

    def update_bucket(
        self,
        bucket_id: str,
        type_id: Optional[str] = None,
        client: Optional[str] = None,
        hostname: Optional[str] = None,
        name: Optional[str] = None,
        data: Optional[dict] = None,
    ):
        updates = []
        values = []
        if type_id is not None: updates.append("type=%s"); values.append(type_id)
        if client is not None: updates.append("client=%s"); values.append(client)
        if hostname is not None: updates.append("hostname=%s"); values.append(hostname)
        if name is not None: updates.append("name=%s"); values.append(name)
        if data is not None: updates.append("datastr=%s"); values.append(json.dumps(data))
        if not updates:
            raise ValueError("At least one field must be updated.")
        values.append(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute(f"UPDATE buckets SET {', '.join(updates)} WHERE id=%s", values)
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        with self.conn.cursor() as cur:
            # благодаря ON DELETE CASCADE достаточно удалить запись из buckets
            cur.execute("DELETE FROM buckets WHERE id=%s", (bucket_id,))
            if cur.rowcount != 1:
                raise ValueError("Bucket did not exist, could not delete")

    def get_metadata(self, bucket_id: str):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id,name,type,client,hostname,created,datastr FROM buckets WHERE id=%s",
                (bucket_id,),
            )
            row = cur.fetchone()
            if not row:
                raise ValueError("Bucket did not exist, could not get metadata")
            return {
                "id": row[0],
                "name": row[1],
                "type": row[2],
                "client": row[3],
                "hostname": row[4],
                "created": row[5],
                "data": json.loads(row[6] or "{}"),
            }

    # --- events ---
    def insert_one(self, bucket_id: str, event: Event) -> Event:
        start = int(event.timestamp.timestamp() * 1_000_000)
        end = start + int(event.duration.total_seconds() * 1_000_000)
        rowid = self._bucket_rowid(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO events(bucketrow,starttime,endtime,datastr)
                   VALUES (%s,%s,%s,%s) RETURNING id""",
                (rowid, start, end, json.dumps(event.data)),
            )
            event.id = int(cur.fetchone()[0])
        return event

    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        if not events:
            return
        rowid = self._bucket_rowid(bucket_id)
        rows = []
        for e in events:
            s = int(e.timestamp.timestamp() * 1_000_000)
            en = s + int(e.duration.total_seconds() * 1_000_000)
            rows.append((rowid, s, en, json.dumps(e.data)))
        with self.conn.cursor() as cur:
            execute_values(
                cur,
                "INSERT INTO events (bucketrow,starttime,endtime,datastr) VALUES %s",
                rows,
            )

    def delete(self, bucket_id: str, event_id: int) -> bool:
        rowid = self._bucket_rowid(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM events WHERE id=%s AND bucketrow=%s", (event_id, rowid))
            return cur.rowcount == 1

    def replace(self, bucket_id: str, event_id: int, event: Event) -> bool:
        rowid = self._bucket_rowid(bucket_id)
        s = int(event.timestamp.timestamp() * 1_000_000)
        en = s + int(event.duration.total_seconds() * 1_000_000)
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE events
                   SET bucketrow=%s, starttime=%s, endtime=%s, datastr=%s
                   WHERE id=%s""",
                (rowid, s, en, json.dumps(event.data), event_id),
            )
            return True

    def replace_last(self, bucket_id: str, event: Event) -> None:
        rowid = self._bucket_rowid(bucket_id)
        s = int(event.timestamp.timestamp() * 1_000_000)
        en = s + int(event.duration.total_seconds() * 1_000_000)
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE events
                   SET starttime=%s, endtime=%s, datastr=%s
                   WHERE id = (
                     SELECT id FROM events WHERE bucketrow=%s
                     ORDER BY endtime DESC LIMIT 1
                   )""",
                (s, en, json.dumps(event.data), rowid),
            )

    def get_event(self, bucket_id: str, event_id: int) -> Optional[Event]:
        rowid = self._bucket_rowid(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT id,starttime,endtime,datastr
                   FROM events
                   WHERE bucketrow=%s AND id=%s""",
                (rowid, event_id),
            )
            row = cur.fetchone()
        if not row:
            return None
        eid, s_i, e_i, ds = row
        start = datetime.fromtimestamp(s_i / 1_000_000, tz=timezone.utc)
        end   = datetime.fromtimestamp(e_i / 1_000_000, tz=timezone.utc)
        return Event(id=eid, timestamp=start, duration=end - start, data=json.loads(ds))

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:
        rowid = self._bucket_rowid(bucket_id)
        s_i = int(starttime.timestamp() * 1_000_000) if starttime else 0
        e_i = int(endtime.timestamp() * 1_000_000) if endtime else MAX_TIMESTAMP
        base_sql = """
            SELECT id,starttime,endtime,datastr
            FROM events
            WHERE bucketrow=%s AND endtime >= %s AND starttime <= %s
            ORDER BY endtime DESC
        """
        params = [rowid, s_i, e_i]
        if limit == 0:
            return []
        if limit > 0:
            sql = base_sql + " LIMIT %s"
            params.append(limit)
        else:
            sql = base_sql
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
        out: List[Event] = []
        for eid, s_i, e_i, ds in rows:
            start = datetime.fromtimestamp(s_i / 1_000_000, tz=timezone.utc)
            end   = datetime.fromtimestamp(e_i / 1_000_000, tz=timezone.utc)
            out.append(Event(id=eid, timestamp=start, duration=end - start, data=json.loads(ds)))
        return out

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        rowid = self._bucket_rowid(bucket_id)
        s_i = int(starttime.timestamp() * 1_000_000) if starttime else 0
        e_i = int(endtime.timestamp() * 1_000_000) if endtime else MAX_TIMESTAMP
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(1) FROM events
                   WHERE bucketrow=%s AND endtime >= %s AND starttime <= %s""",
                (rowid, s_i, e_i),
            )
            return int(cur.fetchone()[0])
