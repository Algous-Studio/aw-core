# aw_datastore/storages/postgresql.py (optimized, peewee-compatible semantics)
"""
PostgreSQL storage backend for ActivityWatch, aligned with peewee/sqlite logic
and tuned for better query/insert performance.

Key differences vs previous version:
- insert_many() now mirrors peewee: updates events with preset IDs, bulk-inserts the rest
- get_events() trims edge events to [start,end] exactly (like peewee)
- replace() and replace_last() now return the updated Event and set event.id (peewee semantics)
- delete() returns number of removed rows (peewee semantics)
- Caches bucket id -> rowid lookups (major speedup on hot paths)
- Adds an index matching ORDER BY endtime DESC for faster pagination
- Uses execute_values with page_size for efficient bulk inserts

Optional DB-level optimizations you can run manually (not required):
  CREATE INDEX IF NOT EXISTS event_index_bucket_endtime_desc
    ON events (bucketrow, endtime DESC);
  -- For heavy range queries you may also consider range indexing (see README/notes)
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Dict, List, Optional

import psycopg2
from psycopg2.extras import execute_values

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

-- Existing helpful indexes
CREATE INDEX IF NOT EXISTS event_index_starttime ON events(bucketrow, starttime);
CREATE INDEX IF NOT EXISTS event_index_endtime   ON events(bucketrow, endtime);
-- Extra index aiding ORDER BY endtime DESC
CREATE INDEX IF NOT EXISTS event_index_bucket_endtime_desc ON events(bucketrow, endtime DESC);
"""

MAX_TIMESTAMP = 2**63 - 1  # keep parity with sqlite backend


class PostgresqlStorage(AbstractStorage):
    sid = "postgresql"

    def __init__(self, testing: bool, dsn: Optional[str] = None) -> None:
        self.testing = testing
        self.dsn = dsn or os.environ.get("POSTGRES_DSN")
        if not self.dsn:
            raise ValueError("POSTGRES_DSN must be provided or pass dsn=...")

        # One connection per process; callers should manage concurrency at a higher level
        self.conn = psycopg2.connect(self.dsn)
        self.conn.autocommit = True

        with self.conn.cursor() as cur:
            cur.execute(CREATE_SCHEMA)

        # cache: bucket_id -> rowid
        self._bucket_cache: Dict[str, int] = {}
        self._refresh_bucket_cache()

    # --- helpers -------------------------------------------------------------
    def commit(self):  # compatibility shim
        try:
            self.conn.commit()
        except Exception:
            pass

    def _refresh_bucket_cache(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute("SELECT id, rowid FROM buckets")
            self._bucket_cache = {bid: int(rid) for bid, rid in cur.fetchall()}

    def _bucket_rowid(self, bucket_id: str) -> int:
        rid = self._bucket_cache.get(bucket_id)
        if rid is not None:
            return rid
        with self.conn.cursor() as cur:
            cur.execute("SELECT rowid FROM buckets WHERE id=%s", (bucket_id,))
            row = cur.fetchone()
            if not row:
                raise ValueError(f"Bucket '{bucket_id}' does not exist")
            rid = int(row[0])
            self._bucket_cache[bucket_id] = rid
            return rid

    # --- buckets metadata ----------------------------------------------------
    def buckets(self):
        with self.conn.cursor() as cur:
            cur.execute(
                "SELECT id,name,type,client,hostname,created,datastr FROM buckets"
            )
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
            cur.execute(
                """INSERT INTO buckets(id,name,type,client,hostname,created,datastr)
                       VALUES (%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (id) DO NOTHING""",
                (bucket_id, name, type_id, client, hostname, created, json.dumps(data or {})),
            )
        # refresh cache lazily; only when newly created
        self._bucket_cache.pop(bucket_id, None)
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
        if type_id is not None:
            updates.append("type=%s"); values.append(type_id)
        if client is not None:
            updates.append("client=%s"); values.append(client)
        if hostname is not None:
            updates.append("hostname=%s"); values.append(hostname)
        if name is not None:
            updates.append("name=%s"); values.append(name)
        if data is not None:
            updates.append("datastr=%s"); values.append(json.dumps(data))
        if not updates:
            raise ValueError("At least one field must be updated.")
        values.append(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute(f"UPDATE buckets SET {', '.join(updates)} WHERE id=%s", values)
        return self.get_metadata(bucket_id)

    def delete_bucket(self, bucket_id: str):
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM buckets WHERE id=%s", (bucket_id,))
            if cur.rowcount != 1:
                raise ValueError("Bucket did not exist, could not delete")
        # ensure cache consistency
        self._bucket_cache.pop(bucket_id, None)

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

    # --- events --------------------------------------------------------------
    @staticmethod
    def _to_us(micros_dt: datetime) -> int:
        return int(micros_dt.timestamp() * 1_000_000)

    def insert_one(self, bucket_id: str, event: Event) -> Event:
        s = self._to_us(event.timestamp)
        e = s + int(event.duration.total_seconds() * 1_000_000)
        rid = self._bucket_rowid(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO events(bucketrow,starttime,endtime,datastr)
                       VALUES (%s,%s,%s,%s) RETURNING id""",
                (rid, s, e, json.dumps(event.data)),
            )
            event.id = int(cur.fetchone()[0])
        return event

    def insert_many(self, bucket_id: str, events: List[Event]) -> None:
        if not events:
            return
        rid = self._bucket_rowid(bucket_id)

        # 1) Updates (events that have explicit IDs)
        updates = [e for e in events if e.id is not None]
        if updates:
            with self.conn.cursor() as cur:
                # Use pipeline of UPDATEs; could be optimized further via temp table if needed
                for ev in updates:
                    s = self._to_us(ev.timestamp)
                    e = s + int(ev.duration.total_seconds() * 1_000_000)
                    cur.execute(
                        """UPDATE events
                               SET bucketrow=%s, starttime=%s, endtime=%s, datastr=%s
                               WHERE id=%s""",
                        (rid, s, e, json.dumps(ev.data), ev.id),
                    )

        # 2) Inserts (events without IDs) — bulk insert with execute_values
        to_insert = [e for e in events if e.id is None]
        if to_insert:
            rows = []
            for ev in to_insert:
                s = self._to_us(ev.timestamp)
                e = s + int(ev.duration.total_seconds() * 1_000_000)
                rows.append((rid, s, e, json.dumps(ev.data)))
            with self.conn.cursor() as cur:
                execute_values(
                    cur,
                    "INSERT INTO events (bucketrow,starttime,endtime,datastr) VALUES %s",
                    rows,
                    page_size=1000,
                )

    def delete(self, bucket_id: str, event_id: int) -> int:
        rid = self._bucket_rowid(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute("DELETE FROM events WHERE id=%s AND bucketrow=%s", (event_id, rid))
            return int(cur.rowcount)

    def replace(self, bucket_id: str, event_id: int, event: Event) -> Event:
        rid = self._bucket_rowid(bucket_id)
        s = self._to_us(event.timestamp)
        e = s + int(event.duration.total_seconds() * 1_000_000)
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE events
                       SET bucketrow=%s, starttime=%s, endtime=%s, datastr=%s
                       WHERE id=%s""",
                (rid, s, e, json.dumps(event.data), event_id),
            )
        event.id = event_id
        return event

    def replace_last(self, bucket_id: str, event: Event) -> Event:
        rid = self._bucket_rowid(bucket_id)
        s = self._to_us(event.timestamp)
        e = s + int(event.duration.total_seconds() * 1_000_000)
        with self.conn.cursor() as cur:
            cur.execute(
                """UPDATE events
                       SET starttime=%s, endtime=%s, datastr=%s
                       WHERE id = (
                         SELECT id FROM events WHERE bucketrow=%s
                         ORDER BY endtime DESC LIMIT 1
                       ) RETURNING id""",
                (s, e, json.dumps(event.data), rid),
            )
            row = cur.fetchone()
        if row:
            event.id = int(row[0])
        return event

    def get_event(self, bucket_id: str, event_id: int) -> Optional[Event]:
        rid = self._bucket_rowid(bucket_id)
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT id,starttime,endtime,datastr
                       FROM events
                      WHERE bucketrow=%s AND id=%s""",
                (rid, event_id),
            )
            row = cur.fetchone()
        if not row:
            return None
        eid, s_i, e_i, ds = row
        start = datetime.fromtimestamp(s_i / 1_000_000, tz=timezone.utc)
        end = datetime.fromtimestamp(e_i / 1_000_000, tz=timezone.utc)
        return Event(id=eid, timestamp=start, duration=end - start, data=json.loads(ds))

    def get_events(
        self,
        bucket_id: str,
        limit: int,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> List[Event]:
        rid = self._bucket_rowid(bucket_id)
        s_i = int(starttime.timestamp() * 1_000_000) if starttime else 0
        e_i = int(endtime.timestamp() * 1_000_000) if endtime else MAX_TIMESTAMP

        if limit == 0:
            return []

        base_sql = (
            "SELECT id,starttime,endtime,datastr FROM events "
            "WHERE bucketrow=%s AND endtime >= %s AND starttime <= %s "
            "ORDER BY endtime DESC"
        )
        params: List[object] = [rid, s_i, e_i]
        if limit > 0:
            sql = base_sql + " LIMIT %s"
            params.append(limit)
        else:
            sql = base_sql

        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        events: List[Event] = []
        for eid, s_us, e_us, ds in rows:
            start = datetime.fromtimestamp(s_us / 1_000_000, tz=timezone.utc)
            end = datetime.fromtimestamp(e_us / 1_000_000, tz=timezone.utc)
            ev = Event(id=eid, timestamp=start, duration=end - start, data=json.loads(ds))
            events.append(ev)

        # Trim to [starttime, endtime] like peewee does
        if starttime or endtime:
            for ev in events:
                if starttime and ev.timestamp < starttime:
                    ev_end = ev.timestamp + ev.duration
                    ev.timestamp = starttime
                    ev.duration = ev_end - ev.timestamp
                if endtime and (ev.timestamp + ev.duration) > endtime:
                    ev.duration = endtime - ev.timestamp

        return events

    def get_eventcount(
        self,
        bucket_id: str,
        starttime: Optional[datetime] = None,
        endtime: Optional[datetime] = None,
    ) -> int:
        rid = self._bucket_rowid(bucket_id)
        s_i = int(starttime.timestamp() * 1_000_000) if starttime else 0
        e_i = int(endtime.timestamp() * 1_000_000) if endtime else MAX_TIMESTAMP
        with self.conn.cursor() as cur:
            cur.execute(
                """SELECT COUNT(1) FROM events
                       WHERE bucketrow=%s AND endtime >= %s AND starttime <= %s""",
                (rid, s_i, e_i),
            )
            return int(cur.fetchone()[0])
