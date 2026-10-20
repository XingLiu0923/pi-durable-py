"""Harness：对外的入口。对应 TS: src/harness/harness.ts（以及 spec §2.2 Public Harness surface）。

    Harness = 一个打开的存储（Session）+ 在它上面跑 agent 的机器（调度器、内置任务、注册表、模型）。

    harness = await Harness.open(storage, models=..., registry=...)
    root = await harness.root(agent={"model": "fake"})        # 根会话，第一次用时创建
    submission = await root.submit({"type": "input", "content": "..."})
    settled = await submission.wait()                         # done / unanswered
    await harness.close()

所有异步 API 在 TS 里都多带一个 Chord `Context`（取消信号 + 遥测 span）；
「取消一个等待只取消这个等待，从不取消工作本身」。Python 版省略了 Context。
"""

from __future__ import annotations

import asyncio
import copy
import sys
from typing import Any, Callable, Dict, List, Optional

from ..context import active_range, model_messages
from ..documents import AgentDoc, InboxDoc, LiveDoc, UsageDoc
from ..errors import SessionClosed
from ..records import ROOT_CONVERSATION_ID
from ..registry import Registry
from ..scheduler import TaskScheduler
from ..session import Publication, Session, Transaction, wall_clock_ms
from .generation import GenerationTask, convert_partial
from .submissions import admit, settle_inputs
from .tool import ToolTask

BUILTIN_TASKS = {GenerationTask.name: GenerationTask, ToolTask.name: ToolTask}

DEFAULT_SETTINGS: Dict[str, Any] = {
    "retry": {"max_retries": 2, "base_delay_ms": 200},
    "progress": {"partial_interval_ms": 100, "output_interval_ms": 100},
    "restart_limit": 5,
    "steering_mode": "one-at-a-time",
    "follow_up_mode": "one-at-a-time",
}


def _merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = copy.deepcopy(base)
    for key, value in (override or {}).items():
        merged[key] = _merge(merged[key], value) if isinstance(value, dict) and isinstance(merged.get(key), dict) else value
    return merged


def _names(values: Any) -> Any:
    """扩展 / 工具以对象传入，以名字存储：名字比代码活得久。"""
    if values is None:
        return None
    return [v if isinstance(v, str) else v.name for v in values]


async def settle_scheduler_outcome(tx: Transaction, record: Dict[str, Any], outcome: Dict[str, Any]) -> None:
    """调度器自己写下 faulted / orphaned 时，替任务做它的代码没机会做的清理（spec §5.4）。"""
    conversation_id = record["conversation_id"]
    if record["kind"] == GenerationTask.name:
        live = await tx.doc(LiveDoc, conversation_id)
        run = live["run"]
        if run is not None and run["task_id"] == record["id"]:
            await convert_partial(tx, conversation_id)
            reason = "faulted" if outcome["status"] == "faulted" else outcome.get("reason", "orphaned")
            settle_inputs(tx, conversation_id, run["inputs"], "unanswered", reason=reason,
                          detail=outcome.get("error", {}).get("message"))
            live["run"] = None
            live["generation"] = None
            live["tools"] = {}
    elif record["kind"] == ToolTask.name:
        live = await tx.doc(LiveDoc, conversation_id)
        for slot in live["tools"].values():
            if slot["task_id"] == record["id"]:
                slot["status"] = "done"  # 没有结果条目；上下文推导会合成一个错误结果


class Harness:
    def __init__(self, storage: Any, *, models: Any, registry: Registry, settings: Optional[Dict[str, Any]] = None,
                 now: Callable[[], float] = wall_clock_ms,
                 on_report: Optional[Callable[[BaseException, Dict[str, Any]], None]] = None) -> None:
        self.session = Session(storage, now)
        self.models = models
        self.registry = registry
        self._settings = settings or {}
        self._on_report = on_report
        self._submission_waiters: Dict[int, List["asyncio.Future[Any]"]] = {}
        self._watchers: List[Callable[[Publication], None]] = []
        self.scheduler = TaskScheduler(self.session, snapshot=self.snapshot, host=self,
                                       on_outcome=settle_scheduler_outcome, report=self.report)
        self.session.subscribe_commits(self._observe)
        registry.subscribe(self.scheduler.registry_changed)

    @classmethod
    async def open(cls, storage: Any, **options: Any) -> "Harness":
        harness = cls(storage, **options)
        await harness.scheduler.open()  # 恢复：上次崩溃时 running 的任务 → pending
        return harness

    # ------------------------------------------------------------------ 宿主能力
    @property
    def settings(self) -> Dict[str, Any]:
        """每次使用时读取、从不存储：可以是「活的」配置。"""
        return _merge(DEFAULT_SETTINGS, self._settings)

    def snapshot(self) -> Any:
        return self.registry.snapshot(BUILTIN_TASKS)

    def now(self) -> float:
        return self.session.now()

    def report(self, error: BaseException, info: Optional[Dict[str, Any]] = None) -> None:
        info = info or {}
        if self._on_report is not None:
            self._on_report(error, info)
        else:
            print(f"  [report] {info.get('type')}: {error}", file=sys.stderr)

    async def resolve_agent(self, conversation_id: int, snapshot: Any = None) -> Any:
        agent_doc = await self.session.snapshot(AgentDoc, conversation_id)
        return (snapshot or self.snapshot()).resolve(agent_doc)

    # ------------------------------------------------------------------ 会话
    async def root(self, agent: Optional[Dict[str, Any]] = None) -> "Conversation":
        if await self.session.storage.conversation(ROOT_CONVERSATION_ID) is None:
            async def create(tx: Transaction) -> None:
                await tx.create_conversation(id=ROOT_CONVERSATION_ID)
                _configure(await tx.doc(AgentDoc, ROOT_CONVERSATION_ID), agent or {})
            await self.session.commit(create)
        return Conversation(self, ROOT_CONVERSATION_ID)

    async def create_conversation(self, agent: Optional[Dict[str, Any]] = None) -> "Conversation":
        async def create(tx: Transaction) -> int:
            record = await tx.create_conversation()
            _configure(await tx.doc(AgentDoc, record["id"]), agent or {})
            return record["id"]
        return Conversation(self, await self.session.commit(create))

    def conversation(self, id: int) -> "Conversation":
        return Conversation(self, id)

    # ------------------------------------------------------------------ 任务
    def resume(self) -> None:
        self.scheduler.resume()

    async def get_task(self, id: int) -> Optional[Dict[str, Any]]:
        return await self.session.storage.task(id)

    async def wait_for_task(self, id: int) -> Dict[str, Any]:
        self.resume()
        return await self.scheduler.wait_for_task(id)

    async def abort_task(self, id: int, wait: bool = True) -> Dict[str, Any]:
        self.resume()
        await self.scheduler.abort(id)
        return await self.scheduler.wait_for_task(id) if wait else await self.get_task(id)

    async def retry_task(self, id: int) -> None:
        await self.scheduler.retry(id)
        self.resume()

    async def inspect(self) -> List[Dict[str, Any]]:
        """活跃任务及其阻塞原因（missing_task / restart_limit / ...）。"""
        snapshot = self.snapshot()
        return [{"task": copy.deepcopy(r), "blocked": self.scheduler.blocked_reason(r, snapshot)}
                for r in sorted(self.scheduler._live.values(), key=lambda r: r["id"])]

    def wake(self) -> Dict[str, Any]:
        return self.scheduler.wake()

    async def submission(self, id: int) -> Optional["Submission"]:
        record = await self.session.storage.submission(id)
        return None if record is None else Submission(self, id)

    # ------------------------------------------------------------------ 关闭 / 崩溃
    async def close(self) -> None:
        await self.scheduler.close()
        self._fail_waiters(SessionClosed("harness closed"))
        await self.session.close()

    def simulate_crash(self) -> None:
        """进程内模拟崩溃：立即停止一切，不写任何东西（存储里会留下 running 的任务）。"""
        self.scheduler.crash()
        self._fail_waiters(SessionClosed("process crashed"))

    def _fail_waiters(self, error: BaseException) -> None:
        for waiters in self._submission_waiters.values():
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(error)
        self._submission_waiters.clear()

    # ------------------------------------------------------------------ 发布观察
    def _observe(self, publication: Publication) -> None:
        for record in publication.submissions:
            if record["status"] in ("done", "unanswered"):
                for waiter in self._submission_waiters.pop(record["id"], []):
                    if not waiter.done():
                        waiter.set_result(copy.deepcopy(record))
        for watcher in list(self._watchers):
            # 观察者回调在变更线之外、稍后执行（spec §1 invariant 7）
            asyncio.get_event_loop().call_soon(watcher, publication)


def _configure(agent_doc: Dict[str, Any], changes: Dict[str, Any]) -> None:
    for key, value in changes.items():
        if key in ("tools", "extensions"):
            value = _names(value)
        if value is None:
            agent_doc.pop(key, None)  # None 清回宿主默认
        else:
            agent_doc[key] = value


class Conversation:
    """会话句柄：不持有任何状态，只有 id。"""

    def __init__(self, harness: Harness, id: int) -> None:
        self.harness = harness
        self.id = id

    async def submit(self, request: Dict[str, Any]) -> "Submission":
        """持久化地接收输入（或写入），返回可等待的 Submission。"""
        self.harness.resume()
        id = await self.harness.session.commit(lambda tx: admit(tx, self.harness, self.id, request),
                                               conversation_id=self.id)
        return Submission(self.harness, id)

    async def commit(self, change: Callable[[Transaction], Any]) -> Any:
        """原子提交：可以一起追加条目、改文档、创建任务。"""
        return await self.harness.session.commit(change, conversation_id=self.id)

    async def configure(self, **changes: Any) -> None:
        async def change(tx: Transaction) -> None:
            _configure(await tx.doc(AgentDoc, self.id), changes)
        await self.commit(change)

    async def agent(self) -> Any:
        return await self.harness.resolve_agent(self.id)

    async def entries(self) -> List[Dict[str, Any]]:
        return await self.harness.session.storage.scan_entries(self.id)

    async def context(self) -> List[Dict[str, Any]]:
        return model_messages(active_range(await self.entries()))

    async def view(self) -> Dict[str, Any]:
        """UI 需要的一切都是已提交状态：活跃条目 + 内置文档。"""
        session = self.harness.session
        return {
            "entries": active_range(await self.entries()),
            "docs": {d.kind: await session.snapshot(d, self.id) for d in (LiveDoc, InboxDoc, AgentDoc, UsageDoc)},
        }

    def watch(self, callback: Callable[[Dict[str, Any]], None]) -> Callable[[], None]:
        """每次提交触及本会话时回调 {"seq", "entries": 新条目, "docs": {kind: 新值}}。"""
        key = f"c:{self.id}"

        def on_publication(publication: Publication) -> None:
            if publication.touches(self.id):
                callback({
                    "seq": publication.seq,
                    "entries": [e for e in publication.entries if e["conversation_id"] == self.id],
                    "docs": {kind: value for kind, k, value in publication.docs if k == key},
                })

        self.harness._watchers.append(on_publication)
        return lambda: self.harness._watchers.remove(on_publication)

    async def is_busy(self) -> bool:
        return (await self.harness.session.snapshot(LiveDoc, self.id))["run"] is not None

    async def wait_for_idle(self) -> None:
        self.harness.resume()
        await self.harness.scheduler.wait_for_idle(self.id)

    async def reset(self, note: Optional[str] = None) -> "Submission":
        """开始新上下文：旧条目还在存储里，但模型从这里开始看。忙的时候像 write 一样排队。"""
        entry: Dict[str, Any] = {"kind": "pi.reset", "head": "self"}
        entry["model"] = [{"role": "user", "content": note}] if note else []
        return await self.submit({"type": "write", "entry": entry})

    async def abort(self) -> None:
        """停下会话：撤回排队的输入（排队的 write 保留），中止当前工作的所有任务，等到空闲。"""
        scheduler = self.harness.scheduler

        async def change(tx: Transaction) -> None:
            inbox = await tx.doc(InboxDoc, self.id)
            withdrawn = [i for i in inbox["items"] if i["mode"] != "write"]
            inbox["items"] = [i for i in inbox["items"] if i["mode"] == "write"]
            for item in withdrawn:
                tx.put_submission({"id": item["id"], "conversation_id": self.id, "type": "input",
                                   "status": "unanswered", "reason": "aborted",
                                   **({"request_id": item["request_id"]} if item.get("request_id") else {})})
            for record in list(scheduler._live.values()):
                if record["conversation_id"] == self.id and record.get("owner") is None \
                        and not record["background"] and not record["abort_requested"]:
                    tx._set_task(dict(record, abort_requested=True, restarts=0))  # 级联会处理它们的子任务

        await self.commit(change)
        await self.wait_for_idle()


class Submission:
    def __init__(self, harness: Harness, id: int) -> None:
        self.harness = harness
        self.id = id

    async def record(self) -> Dict[str, Any]:
        return await self.harness.session.storage.submission(self.id)

    async def wait(self) -> Dict[str, Any]:
        """等到 done（回答了）或 unanswered（带原因）。重启后用 harness.submission(id) 拿回来接着等。"""
        self.harness.resume()
        waiter: "asyncio.Future[Any]" = asyncio.get_event_loop().create_future()
        self.harness._submission_waiters.setdefault(self.id, []).append(waiter)
        record = await self.record()
        if record["status"] in ("done", "unanswered") and not waiter.done():
            waiter.set_result(record)
        return await waiter

    async def abort(self) -> str:
        """只能撤回还在排队的 submission。"""
        async def change(tx: Transaction) -> str:
            record = await tx.submission(self.id)
            if record["status"] in ("done", "unanswered"):
                return "settled"
            if record["status"] == "placed":
                return "already_placed"
            inbox = await tx.doc(InboxDoc, record["conversation_id"])
            inbox["items"] = [i for i in inbox["items"] if i["id"] != self.id]
            tx.put_submission(dict(record, status="unanswered", reason="aborted"))
            return "withdrawn"
        return await self.harness.session.commit(change)
