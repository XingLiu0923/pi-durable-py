"""内存存储。对应 TS: src/storage/memory.ts。

什么都不落盘，但「同一个 MemoryStorage 对象」可以先后交给两个 Harness，
用来在一个进程里模拟「关掉再打开」或「崩溃后重开」。
"""

from __future__ import annotations

import copy
from typing import Any, Dict, List, Optional

from ..records import LIVE_STATUSES, ROOT_CONVERSATION_ID
from .base import Storage, check_writes


class MemoryStorage(Storage):
    def __init__(self) -> None:
        self._next_id = ROOT_CONVERSATION_ID + 1
        self._seq = 0
        self._conversations: Dict[int, dict] = {}
        self._entries: Dict[int, dict] = {}
        self._tasks: Dict[int, dict] = {}
        self._submissions: Dict[int, dict] = {}
        self._docs: Dict[tuple, dict] = {}

    async def commit(self, writes: List[Dict[str, Any]]) -> int:
        check_writes(writes)
        # 先校验、先拷贝（「借用」的参数在提交结束前不能被保留，spec §4 Storage ownership），
        # 全部通过之后再一次性应用 —— 这就是原子性。
        staged = copy.deepcopy(writes)
        for write in staged:
            if write["type"] == "conversation" and write["value"]["id"] in self._conversations:
                raise ValueError("conversations are immutable")
            if write["type"] == "entry" and write["value"]["id"] in self._entries:
                raise ValueError("entries are immutable")
        for write in staged:
            kind = write["type"]
            if kind == "conversation":
                self._conversations[write["value"]["id"]] = write["value"]
            elif kind == "entry":
                self._entries[write["value"]["id"]] = write["value"]
            elif kind == "task":
                self._tasks[write["value"]["id"]] = write["value"]
            elif kind == "submission":
                self._submissions[write["value"]["id"]] = write["value"]
            elif kind == "doc.put":
                self._docs[(write["kind"], write["key"])] = write["value"]
            elif kind == "doc.retire":
                for address in [a for a in self._docs if a[1] == write["key"]]:
                    del self._docs[address]
        self._seq += 1
        return self._seq

    async def mint_id(self) -> int:
        id = self._next_id
        self._next_id += 1
        return id

    async def conversation(self, id: int) -> Optional[dict]:
        return copy.deepcopy(self._conversations.get(id))

    async def scan_conversations(self) -> List[dict]:
        return [copy.deepcopy(c) for _, c in sorted(self._conversations.items())]

    async def entry(self, id: int) -> Optional[dict]:
        return copy.deepcopy(self._entries.get(id))

    async def scan_entries(self, conversation_id: int, max_id: Optional[int] = None) -> List[dict]:
        return [
            copy.deepcopy(e)
            for id, e in sorted(self._entries.items())
            if e["conversation_id"] == conversation_id and (max_id is None or id <= max_id)
        ]

    async def task(self, id: int) -> Optional[dict]:
        return copy.deepcopy(self._tasks.get(id))

    async def scan_tasks(self, live_only: bool = False) -> List[dict]:
        return [
            copy.deepcopy(t)
            for _, t in sorted(self._tasks.items())
            if not live_only or t["state"]["status"] in LIVE_STATUSES
        ]

    async def submission(self, id: int) -> Optional[dict]:
        return copy.deepcopy(self._submissions.get(id))

    async def submission_by_request(self, conversation_id: int, request_id: str) -> Optional[dict]:
        for s in self._submissions.values():
            if s["conversation_id"] == conversation_id and s.get("request_id") == request_id:
                return copy.deepcopy(s)
        return None

    async def document(self, kind: str, key: str) -> Optional[dict]:
        return copy.deepcopy(self._docs.get((kind, key)))
