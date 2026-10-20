"""SQLite 存储。对应 TS: src/storage/sqlite/（storage.ts / schema.ts / node.ts）。

一个数据库文件，WAL 模式 + synchronous=NORMAL（与 TS 版一致）：
进程崩溃不丢已提交数据；断电时最新的一次提交可能丢失。

每条记录以 JSON 文本存储，另外抽出几列（conversation_id / status / request_id）建索引。
TS 版的 SqliteStorage 跑在一个异步的 `SqliteDatabase` 门面上，所以同一份代码能跑在 Node、Bun、
Cloudflare Durable Object 里；这里直接用 Python 标准库 sqlite3（同步调用）。
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Dict, List, Optional

from ..records import LIVE_STATUSES, ROOT_CONVERSATION_ID
from .base import Storage, check_writes

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS conversations (id INTEGER PRIMARY KEY, json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS entries (
    id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS entries_by_conversation ON entries (conversation_id, id);
CREATE TABLE IF NOT EXISTS tasks (id INTEGER PRIMARY KEY, status TEXT NOT NULL, json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS tasks_by_status ON tasks (status, id);
CREATE TABLE IF NOT EXISTS submissions (
    id INTEGER PRIMARY KEY, conversation_id INTEGER NOT NULL, request_id TEXT, json TEXT NOT NULL);
CREATE UNIQUE INDEX IF NOT EXISTS submissions_by_request ON submissions (conversation_id, request_id);
CREATE TABLE IF NOT EXISTS documents (kind TEXT NOT NULL, key TEXT NOT NULL, json TEXT NOT NULL,
    PRIMARY KEY (kind, key));
CREATE INDEX IF NOT EXISTS documents_by_key ON documents (key);
"""


def _load(row: Optional[tuple]) -> Optional[dict]:
    return None if row is None else json.loads(row[0])


class SqliteStorage(Storage):
    def __init__(self, path: str) -> None:
        # isolation_level=None：自己控制 BEGIN / COMMIT
        self._db = sqlite3.connect(path, isolation_level=None)
        self._db.execute("PRAGMA journal_mode = WAL")
        self._db.execute("PRAGMA synchronous = NORMAL")
        self._db.executescript(SCHEMA)
        self._db.execute("INSERT OR IGNORE INTO meta VALUES ('next_id', ?)", (ROOT_CONVERSATION_ID + 1,))
        self._db.execute("INSERT OR IGNORE INTO meta VALUES ('seq', 0)")

    @classmethod
    async def open(cls, path: str) -> "SqliteStorage":
        return cls(path)

    async def commit(self, writes: List[Dict[str, Any]]) -> int:
        check_writes(writes)
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        try:
            for write in writes:
                kind = write["type"]
                if kind == "conversation":
                    value = write["value"]
                    db.execute("INSERT INTO conversations VALUES (?, ?)", (value["id"], json.dumps(value)))
                elif kind == "entry":
                    value = write["value"]
                    db.execute(
                        "INSERT INTO entries VALUES (?, ?, ?)",
                        (value["id"], value["conversation_id"], json.dumps(value)),
                    )
                elif kind == "task":
                    value = write["value"]
                    db.execute(
                        "INSERT OR REPLACE INTO tasks VALUES (?, ?, ?)",
                        (value["id"], value["state"]["status"], json.dumps(value)),
                    )
                elif kind == "submission":
                    value = write["value"]
                    db.execute(
                        "INSERT OR REPLACE INTO submissions VALUES (?, ?, ?, ?)",
                        (value["id"], value["conversation_id"], value.get("request_id"), json.dumps(value)),
                    )
                elif kind == "doc.put":
                    db.execute(
                        "INSERT OR REPLACE INTO documents VALUES (?, ?, ?)",
                        (write["kind"], write["key"], json.dumps(write["value"])),
                    )
                elif kind == "doc.retire":
                    db.execute("DELETE FROM documents WHERE key = ?", (write["key"],))
            db.execute("UPDATE meta SET value = value + 1 WHERE key = 'seq'")
            seq = db.execute("SELECT value FROM meta WHERE key = 'seq'").fetchone()[0]
            db.execute("COMMIT")
            return seq
        except BaseException:
            db.execute("ROLLBACK")  # 任何一条失败 → 整批回滚
            raise

    async def mint_id(self) -> int:
        # 立刻持久化计数器：即使这个 ID 最终没被用上，重启后也不会再分配出去
        db = self._db
        db.execute("BEGIN IMMEDIATE")
        id = db.execute("SELECT value FROM meta WHERE key = 'next_id'").fetchone()[0]
        db.execute("UPDATE meta SET value = ? WHERE key = 'next_id'", (id + 1,))
        db.execute("COMMIT")
        return id

    async def conversation(self, id: int) -> Optional[dict]:
        return _load(self._db.execute("SELECT json FROM conversations WHERE id = ?", (id,)).fetchone())

    async def scan_conversations(self) -> List[dict]:
        return [json.loads(r[0]) for r in self._db.execute("SELECT json FROM conversations ORDER BY id")]

    async def entry(self, id: int) -> Optional[dict]:
        return _load(self._db.execute("SELECT json FROM entries WHERE id = ?", (id,)).fetchone())

    async def scan_entries(self, conversation_id: int, max_id: Optional[int] = None) -> List[dict]:
        rows = self._db.execute(
            "SELECT json FROM entries WHERE conversation_id = ? AND id <= ? ORDER BY id",
            (conversation_id, max_id if max_id is not None else 2**62),
        )
        return [json.loads(r[0]) for r in rows]

    async def task(self, id: int) -> Optional[dict]:
        return _load(self._db.execute("SELECT json FROM tasks WHERE id = ?", (id,)).fetchone())

    async def scan_tasks(self, live_only: bool = False) -> List[dict]:
        if live_only:
            marks = ",".join("?" for _ in LIVE_STATUSES)
            rows = self._db.execute(f"SELECT json FROM tasks WHERE status IN ({marks}) ORDER BY id", LIVE_STATUSES)
        else:
            rows = self._db.execute("SELECT json FROM tasks ORDER BY id")
        return [json.loads(r[0]) for r in rows]

    async def submission(self, id: int) -> Optional[dict]:
        return _load(self._db.execute("SELECT json FROM submissions WHERE id = ?", (id,)).fetchone())

    async def submission_by_request(self, conversation_id: int, request_id: str) -> Optional[dict]:
        row = self._db.execute(
            "SELECT json FROM submissions WHERE conversation_id = ? AND request_id = ?",
            (conversation_id, request_id),
        ).fetchone()
        return _load(row)

    async def document(self, kind: str, key: str) -> Optional[dict]:
        return _load(self._db.execute("SELECT json FROM documents WHERE kind = ? AND key = ?", (kind, key)).fetchone())

    async def close(self) -> None:
        self._db.close()
