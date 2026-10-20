"""Storage 抽象。对应 TS: `interface Storage`（src/types.ts，spec §10「Storage contract」）。

Storage 是整个系统里最「笨」的一层：它信任上层 Session 给的记录都是合法的，只负责三件事：

1. **原子提交**：commit(writes) 要么全部写入，要么一个都不写；
2. **全局 ID**：mint_id() 从一个全局递增的数字空间分配 ID，重启后也只会更大
   （所以「按 ID 排序」= 「按创建顺序排序」，上层依赖这个性质）；
3. **分离的值**：读出来的永远是副本，调用方随便改都不会污染存储。

一次提交（commit）里可以混合下面这些写入：

    {"type": "conversation", "value": ConversationRecord}   # 只能创建，不可改
    {"type": "entry",        "value": EntryRecord}          # 只能创建，不可改
    {"type": "task",         "value": TaskRecord}           # 整条替换（每个 checkpoint 都是完整替换）
    {"type": "submission",   "value": SubmissionRecord}     # 整条替换
    {"type": "doc.put",      "kind": str, "key": str, "value": dict}
    {"type": "doc.retire",   "key": str}                    # 删除这个 key 下的所有文档（任务结束时退休任务文档）

TS 版的文档写入是 Chord 的增量操作（delta ops）+ 偶尔的完整基线（base），
这里简化为每次写完整值。
"""

from __future__ import annotations

import abc
from typing import Any, Dict, List, Optional


class Storage(abc.ABC):
    # ---- 写 ----
    @abc.abstractmethod
    async def commit(self, writes: List[Dict[str, Any]]) -> int:
        """原子地应用一批写入，返回严格递增的提交序号 seq。"""

    @abc.abstractmethod
    async def mint_id(self) -> int:
        """分配一个全局唯一、单调递增的 ID（跨重启）。"""

    # ---- 读（全部返回副本） ----
    @abc.abstractmethod
    async def conversation(self, id: int) -> Optional[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def scan_conversations(self) -> List[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def entry(self, id: int) -> Optional[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def scan_entries(self, conversation_id: int, max_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """按 ID 升序返回会话的条目（TS 版是基于游标的分页扫描）。"""

    @abc.abstractmethod
    async def task(self, id: int) -> Optional[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def scan_tasks(self, live_only: bool = False) -> List[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def submission(self, id: int) -> Optional[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def submission_by_request(self, conversation_id: int, request_id: str) -> Optional[Dict[str, Any]]: ...

    @abc.abstractmethod
    async def document(self, kind: str, key: str) -> Optional[Dict[str, Any]]: ...

    async def close(self) -> None:
        pass


def check_writes(writes: List[Dict[str, Any]]) -> None:
    for write in writes:
        if write["type"] not in ("conversation", "entry", "task", "submission", "doc.put", "doc.retire"):
            raise ValueError(f"unknown write type {write['type']}")
