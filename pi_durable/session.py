"""Session：一个打开的存储 + 唯一一条「变更线」（mutation line）。

对应 TS: src/session/session.ts、src/session/transaction.ts；规范 spec §1、§4。

核心规则（spec 开头那句话）：
    > Session 原子地提交「不可变条目 + 完整任务记录 + 文档」。只有已提交的状态是可观察的。

具体落地成几条不变量（spec §1 Required invariants，挑最重要的）：
    1. 一次提交跨所有记录和文档都是原子的；
    2. 文档的更新只在存储提交成功之后才发布；
    3. 所有可见的进度都是持久化的 —— 没有「易失」的发布路径；
    4. 外部副作用（调模型、跑工具、网络、人）不在提交事务里执行；
    5. 条目和 ID 不可变，永不复用；
    8. 存储错误对 Session 是致命的：什么都不发布，必须重新打开。

「变更线」就是一把 asyncio.Lock：所有提交排成一队，一个一个地执行
「回调 → 准备 → 存储提交 → 采纳 → 发布」。
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import json
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from .documents import DocDefinition
from .errors import ReadAfterWrite, SessionClosed, SessionFailed
from .storage.base import Storage


def wall_clock_ms() -> float:
    return time.time() * 1000


class Publication:
    """一次成功提交的「发布」：只有提交落盘以后才会产生，观察者看到的都是它。"""

    def __init__(self, seq: int, writes: List[Dict[str, Any]]) -> None:
        self.seq = seq
        self.writes = writes
        self.tasks = [w["value"] for w in writes if w["type"] == "task"]
        self.entries = [w["value"] for w in writes if w["type"] == "entry"]
        self.submissions = [w["value"] for w in writes if w["type"] == "submission"]
        self.conversations = [w["value"] for w in writes if w["type"] == "conversation"]
        self.docs = [(w["kind"], w["key"], w["value"]) for w in writes if w["type"] == "doc.put"]

    def touches(self, conversation_id: int) -> bool:
        key = f"c:{conversation_id}"
        return (
            any(e["conversation_id"] == conversation_id for e in self.entries)
            or any(t["conversation_id"] == conversation_id for t in self.tasks)
            or any(s["conversation_id"] == conversation_id for s in self.submissions)
            or any(k == key for _, k, _ in self.docs)
        )


class Session:
    def __init__(self, storage: Storage, now: Callable[[], float] = wall_clock_ms) -> None:
        self.storage = storage
        self.now = now
        self._line = asyncio.Lock()  # 唯一的变更线
        self._docs: Dict[Tuple[str, str], Dict[str, Any]] = {}  # 已提交文档的内存缓存
        self._listeners: List[Callable[[Publication], None]] = []
        self.closing = False
        self.failure: Optional[BaseException] = None

    # ------------------------------------------------------------------ 提交

    def subscribe_commits(self, listener: Callable[[Publication], None]) -> Callable[[], None]:
        """在变更线上同步地观察每次发布（TS: subscribeCommits）。监听器不能阻塞、不能调用 Session API。"""
        self._listeners.append(listener)
        return lambda: self._listeners.remove(listener) if listener in self._listeners else None

    def _check_open(self) -> None:
        if self.failure is not None:
            raise SessionFailed("session failed") from self.failure
        if self.closing:
            raise SessionClosed("session is closing")

    async def commit(self, change: Callable[["Transaction"], Any], *, task_id: Optional[int] = None,
                     conversation_id: Optional[int] = None) -> Any:
        """原子提交。change(tx) 可以是同步或异步函数；它抛错 → 什么都不写（回滚）。"""
        self._check_open()
        await self._line.acquire()
        tx = Transaction(self, task_id=task_id, conversation_id=conversation_id)
        try:
            self._check_open()  # 「准入」在变更线上判断：关闭开始后不再接纳新提交
            result = change(tx)
            if inspect.isawaitable(result):
                result = await result
            writes = tx._prepare()
        except BaseException:
            tx._revoke()
            self._line.release()
            raise
        tx._revoke()  # 回调结束，草稿失效（invariant 6）
        # 一旦准入，调用方的取消不会打断存储提交，也不会撤销它（spec §4）
        settle = asyncio.ensure_future(self._settle(writes))
        await asyncio.shield(settle)
        return result

    async def _settle(self, writes: List[Dict[str, Any]]) -> None:
        try:
            if not writes:
                return
            try:
                seq = await self.storage.commit(writes)
            except Exception as error:  # 规则 1：存储错误终结 Session
                self.failure = error
                raise SessionFailed("storage commit failed") from error
            self._adopt(writes)
            publication = Publication(seq, writes)
            for listener in list(self._listeners):
                try:
                    listener(publication)
                except Exception as error:  # 一个监听器出错不影响其他监听器
                    print(f"[session] commit listener failed: {error!r}")
        finally:
            self._line.release()

    def _adopt(self, writes: List[Dict[str, Any]]) -> None:
        """存储提交成功之后，才把新值「采纳」进内存缓存。"""
        for write in writes:
            if write["type"] == "doc.put":
                self._docs[(write["kind"], write["key"])] = copy.deepcopy(write["value"])
            elif write["type"] == "doc.retire":
                for address in [a for a in self._docs if a[1] == write["key"]]:
                    del self._docs[address]

    # ------------------------------------------------------------------ 读已提交的文档

    async def _committed_doc(self, kind: str, key: str) -> Optional[Dict[str, Any]]:
        address = (kind, key)
        if address not in self._docs:
            value = await self.storage.document(kind, key)
            if value is None:
                return None
            self._docs[address] = value
        return self._docs[address]

    async def snapshot(self, definition: DocDefinition, owner: Optional[int] = None) -> Dict[str, Any]:
        """读已提交的文档（副本）；不存在时返回 initial()。"""
        value = await self._committed_doc(definition.kind, definition.key(owner))
        return definition.initial() if value is None else copy.deepcopy(value)

    async def close(self) -> None:
        self.closing = True
        async with self._line:  # 等已准入的提交落定
            pass
        await self.storage.close()


class Transaction:
    """一次提交回调里拿到的 tx。

    规则（spec §4 Table rules）：
      - 表 = conversations / entries / tasks / submissions；
      - 第一次表写入之前可以随便读表；之后再读表 → ReadAfterWrite；
      - 文档随时可读写，并且能读到自己本事务里写的值（read-your-writes）；
      - 创建方法直接返回创建出的记录 / ID，不需要回读。
    """

    def __init__(self, session: Session, task_id: Optional[int] = None,
                 conversation_id: Optional[int] = None) -> None:
        self._session = session
        self.task_id = task_id  # 由任务发起的提交：追加的条目会记录 by_task_id
        self.conversation_id = conversation_id  # 绑定的会话：create_task 的默认会话
        self._writes: List[Dict[str, Any]] = []
        self._drafts: Dict[Tuple[str, str], Tuple[DocDefinition, Dict[str, Any], Optional[str]]] = {}
        self._retired: List[str] = []
        self._table_written = False
        self._revoked = False
        # 本事务写入的「候选」任务记录；调度器据此判断「拥有的活跃工作」（例如同一提交里刚创建的子任务）
        self.tasks_written: Dict[int, Dict[str, Any]] = {}

    # ---------------------------------------------------------------- 守卫
    def _alive(self) -> None:
        if self._revoked:
            raise RuntimeError("transaction used after its commit callback settled")

    def _read(self) -> None:
        self._alive()
        if self._table_written:
            raise ReadAfterWrite("table read after the first table write in this transaction")

    def _write(self) -> None:
        self._alive()
        self._table_written = True

    @property
    def now(self) -> float:
        return self._session.now()

    # ---------------------------------------------------------------- 表读
    async def conversation(self, id: int):
        self._read()
        return await self._session.storage.conversation(id)

    async def entry(self, id: int):
        self._read()
        return await self._session.storage.entry(id)

    async def entries(self, conversation_id: int, max_id: Optional[int] = None):
        self._read()
        return await self._session.storage.scan_entries(conversation_id, max_id)

    async def task(self, id: int):
        self._read()
        return await self._session.storage.task(id)

    async def submission(self, id: int):
        self._read()
        return await self._session.storage.submission(id)

    async def submission_by_request(self, conversation_id: int, request_id: str):
        self._read()
        return await self._session.storage.submission_by_request(conversation_id, request_id)

    # ---------------------------------------------------------------- 文档
    async def doc(self, definition: DocDefinition, owner: Optional[int] = None) -> Dict[str, Any]:
        """取文档草稿（get-or-create）。直接修改返回的 dict，提交成功后才生效。"""
        self._alive()
        key = definition.key(owner)
        address = (definition.kind, key)
        if address not in self._drafts:
            committed = await self._session._committed_doc(definition.kind, key)
            if committed is None:
                self._drafts[address] = (definition, definition.initial(), None)
            else:
                original = json.dumps(committed, sort_keys=True)
                self._drafts[address] = (definition, copy.deepcopy(committed), original)
        return self._drafts[address][1]

    def retire_docs(self, key: str) -> None:
        self._alive()
        self._retired.append(key)

    # ---------------------------------------------------------------- 表写
    async def mint_id(self) -> int:
        self._alive()
        return await self._session.storage.mint_id()

    async def create_conversation(self, owner_task_id: Optional[int] = None,
                                  owner_conversation_id: Optional[int] = None,
                                  id: Optional[int] = None) -> Dict[str, Any]:
        self._write()
        record: Dict[str, Any] = {"id": id if id is not None else await self.mint_id()}
        if owner_task_id is not None:
            record["owner"] = {"conversation_id": owner_conversation_id, "task_id": owner_task_id}
        self._writes.append({"type": "conversation", "value": record})
        return record

    async def append_entry(self, conversation_id: int, entry: Dict[str, Any]) -> Dict[str, Any]:
        self._write()
        id = await self.mint_id()
        record = dict(entry, id=id, conversation_id=conversation_id)
        if record.get("head") == "self":  # reset：上下文从自己开始
            record["head"] = id
        if self.task_id is not None:
            record["by_task_id"] = self.task_id
        self._writes.append({"type": "entry", "value": record})
        return record

    async def create_task(self, definition: Any, input: Any = None, *, conversation_id: Optional[int] = None,
                          owner: Optional[int] = None, background: bool = False) -> int:
        """创建任务。owner=None 表示「会话拥有」（顶层任务），否则是 owner 任务的子任务。"""
        task_def = getattr(definition, "definition", definition)
        conversation = conversation_id if conversation_id is not None else self.conversation_id
        if conversation is None:
            raise ValueError("create_task needs a conversation_id")
        if owner is not None and background:
            raise ValueError("a child task cannot be background (spec §5.5)")
        id = await self.mint_id()
        record = {
            "id": id,
            "conversation_id": conversation,
            "kind": task_def.name,
            "version": task_def.version,
            "input": input,
            "owner": owner,
            "background": background,
            "abort_requested": False,
            "restarts": 0,
            "state": {"status": "pending", "checkpoint": task_def.initial(input)},
            "memos": {},
        }
        self._set_task(record)
        return id

    def _set_task(self, record: Dict[str, Any]) -> None:
        """整条替换任务记录。用户代码不该直接调用：任务只通过自己的 runtime 改自己的状态（spec §5.1）。"""
        self._write()
        self._writes.append({"type": "task", "value": record})
        self.tasks_written[record["id"]] = record

    def put_submission(self, record: Dict[str, Any]) -> None:
        self._write()
        self._writes.append({"type": "submission", "value": record})

    # ---------------------------------------------------------------- 准备
    def _prepare(self) -> List[Dict[str, Any]]:
        writes = list(self._writes)
        for (kind, key), (_, draft, original) in self._drafts.items():
            if key in self._retired:
                continue
            # 值必须是严格 JSON（invariant 6）；序列化一次顺便校验
            encoded = json.dumps(draft, sort_keys=True)
            if original is None or encoded != original:
                writes.append({"type": "doc.put", "kind": kind, "key": key, "value": json.loads(encoded)})
        for key in self._retired:
            writes.append({"type": "doc.retire", "key": key})
        return writes

    def _revoke(self) -> None:
        self._revoked = True
