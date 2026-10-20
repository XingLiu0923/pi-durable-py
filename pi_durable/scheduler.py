"""任务调度器 —— 整个项目的心脏。对应 TS: src/harness/scheduler.ts（约 2500 行），spec §5.1、§5.4–§5.6。

调度器对「run / 工具 / 模型」一无所知，它只认识「任务记录」这个状态机：

    pending ──预留──▶ running ──phase 提交──▶ running(新 checkpoint) ─▶ ... ─▶ terminal
                         │                       │
                         ├─▶ waiting(on=[...]) ──┤ 等的任务都终结了 → 再预留
                         ├─▶ sleeping(until)  ───┤ 时间到了       → 再预留
                         └─▶ completing(outcome) ─ 自己拥有的工作排空后 → terminal

主要职责：
    1. open()：恢复（reconciliation）—— 上个进程死时还在 running 的任务改回 pending，restarts+1；
    2. 预留（reserve）：在变更线上把可运行的任务标成 running，然后在线外跑 phase handler；
    3. step：每个 phase 之前/之后在变更线上按 6 条规则决定下一步；
    4. 中止协议：打中止标记 → 取消正在跑的 run 调用 → 等拥有的工作排空 → 另起 abort 调用；
    5. 结构化并发：completing 挂起、级联中止、failFast；
    6. 睡眠与计时器、wait_for_task / 空闲等待、close()。

内存里维护一份「活跃任务索引」(self._live)，每次提交发布时同步更新（TS: #observe / #track）。
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import math
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from .documents import task_doc_key
from .errors import InvocationEnded, SessionClosed, SessionFailed
from .records import task_parent, terminal
from .session import Publication, Session, Transaction
from .tasks import TaskDefinition, TaskRuntime


class Invocation:
    """一个任务在内存里的「一次调用」：模式（run / abort）、所用的定义和注册表快照、asyncio 任务。

    调用模式是易失的：重开后由持久化的中止标记重新推导。
    """

    def __init__(self, record: Dict[str, Any], mode: str, definition: TaskDefinition, snapshot: Any) -> None:
        self.task_id: int = record["id"]
        self.conversation_id: int = record["conversation_id"]
        self.mode = mode
        self.definition = definition
        self.snapshot = snapshot
        self.ended = False
        self.aio: Optional["asyncio.Future[Any]"] = None
        self.timers: List[asyncio.TimerHandle] = []


def _fault(error: Any) -> Dict[str, Any]:
    message = error if isinstance(error, str) else f"{type(error).__name__}: {error}"
    return {"status": "faulted", "error": {"message": message}}


def _final(record: Dict[str, Any], outcome: Dict[str, Any], now: float) -> Dict[str, Any]:
    final = dict(record, state=terminal(outcome), ended_at=now)
    final.pop("memos", None)  # 终结记录里 checkpoint 和 memos 都消失，只留结果（「回执」）
    return final


class TaskScheduler:
    def __init__(self, session: Session, *, snapshot: Callable[[], Any], host: Any,
                 on_outcome: Callable[[Transaction, Dict[str, Any], Dict[str, Any]], Awaitable[None]],
                 report: Callable[[BaseException, Dict[str, Any]], None]) -> None:
        self.session = session
        self.host = host
        self._snapshot = snapshot
        # Harness 提供的唯一一个钩子：调度器自己写下 faulted / orphaned 结局时，让 Harness 清理 run / 工具槽位
        self._on_outcome = on_outcome
        self._report = report
        self._live: Dict[int, Dict[str, Any]] = {}  # 活跃任务（非 terminal）
        self._meta: Dict[int, Dict[str, Any]] = {}  # 见过的所有任务的所有权信息（含已终结的）和结局
        self._conversation_owner: Dict[int, int] = {}  # 会话 → 拥有它的任务
        self._invocations: Dict[int, Invocation] = {}
        self._task_waiters: Dict[int, List["asyncio.Future[Any]"]] = {}
        self._idle_waiters: List[Tuple[Optional[int], "asyncio.Future[None]"]] = []
        self._enabled = False
        self._closing = False
        self._drain: Optional["asyncio.Future[None]"] = None
        self._again = False
        self._reconcile_needed = True
        self._timer: Optional[asyncio.TimerHandle] = None
        self._parked_reported: set = set()
        session.subscribe_commits(self._observe)

    # ================================================================== 生命周期

    async def open(self) -> None:
        """打开时的恢复（spec §5.1 末尾、§5.4 Restarts）。"""
        storage = self.session.storage
        for conversation in await storage.scan_conversations():
            if conversation.get("owner"):
                self._conversation_owner[conversation["id"]] = conversation["owner"]["task_id"]
        for record in await storage.scan_tasks():
            self._remember(record)
            if record["state"]["status"] != "terminal":
                self._live[record["id"]] = record
        # 上个进程死的时候还在 running 的任务：改回 pending，保留 checkpoint 和中止标记，restarts + 1。
        # （正常 close() 会把它们写回 pending 且不计数，所以留下 running 的只可能是崩溃 / 被驱逐。）
        cut_off = [r for r in self._live.values() if r["state"]["status"] == "running"]
        if cut_off:
            def reconcile(tx: Transaction) -> None:
                for record in cut_off:
                    tx._set_task(dict(record, restarts=record.get("restarts", 0) + 1,
                                      state={"status": "pending", "checkpoint": record["state"]["checkpoint"]}))
            await self.session.commit(reconcile)
        limit = self._restart_limit()
        for record in self._live.values():
            if not record["abort_requested"] and record.get("restarts", 0) >= limit:
                self._parked_reported.add(record["id"])
                self._report(RuntimeError(f"task {record['id']} parked after {record['restarts']} restarts"),
                             {"type": "task_parked", "task": record["id"]})

    def resume(self) -> None:
        """开始调度（submit / wait 也会隐式调用）。只「打开」不 resume 的 Harness 不会跑任何东西。"""
        self._enabled = True
        self._kick()

    async def close(self) -> None:
        """正常关闭：停掉所有调用，但**不写结局**，把它们写回 pending（不计崩溃次数），重开后继续。"""
        self._closing = True
        self._cancel_timer()
        invocations = list(self._invocations.values())
        for invocation in invocations:
            if invocation.aio is not None:
                invocation.aio.cancel()
        await asyncio.gather(*(i.aio for i in invocations if i.aio is not None), return_exceptions=True)
        if self._drain is not None:
            await asyncio.gather(self._drain, return_exceptions=True)
        async with self.session._line:  # 等在途的提交落定
            stopped = [r for r in self._live.values() if r["state"]["status"] == "running"]
            if stopped and self.session.failure is None:
                # 直接写存储、不发布（spec §5.1：close 不改变任何可观察状态）
                await self.session.storage.commit([
                    {"type": "task", "value": dict(r, state={"status": "pending", "checkpoint": r["state"]["checkpoint"]})}
                    for r in stopped
                ])
        self._fail_waiters(SessionClosed("harness closed"))

    def crash(self) -> None:
        """模拟进程崩溃：立刻停止一切，什么都不写。存储里留下 running 的任务。"""
        self.session.failure = RuntimeError("simulated crash")
        self._closing = True
        self._cancel_timer()
        for invocation in list(self._invocations.values()):
            if invocation.aio is not None:
                invocation.aio.cancel()
        if self._drain is not None:
            self._drain.cancel()
        self._fail_waiters(SessionClosed("process crashed"))

    def registry_changed(self) -> None:
        """注册表变了：被「缺定义」阻塞的任务可能可以跑了。"""
        self._kick()

    # ================================================================== 发布观察（在变更线上同步执行）

    def _remember(self, record: Dict[str, Any]) -> None:
        meta = {k: record.get(k) for k in ("id", "conversation_id", "owner", "background")}
        if record["state"]["status"] == "terminal":
            meta["outcome"] = record["state"]["outcome"]
        self._meta[record["id"]] = meta

    def _observe(self, publication: Publication) -> None:
        for conversation in publication.conversations:
            if conversation.get("owner"):
                self._conversation_owner[conversation["id"]] = conversation["owner"]["task_id"]
        if not publication.tasks:
            return
        for record in publication.tasks:
            previous = self._live.get(record["id"])
            self._remember(record)
            if record["state"]["status"] == "terminal":
                self._live.pop(record["id"], None)
                for waiter in self._task_waiters.pop(record["id"], []):
                    if not waiter.done():
                        waiter.set_result(copy.deepcopy(record))
            else:
                self._live[record["id"]] = record
            # 新出现的中止标记：给正在跑的 run 调用发信号（这里用 asyncio 的 cancel 来表达 AbortSignal）
            if record.get("abort_requested") and not (previous and previous.get("abort_requested")):
                invocation = self._invocations.get(record["id"])
                if invocation is not None and invocation.mode == "run" and invocation.aio is not None:
                    invocation.aio.cancel()
        self._reconcile_needed = True
        self._check_idle()
        self._kick()

    # ================================================================== 驱动循环

    def _kick(self) -> None:
        if not self._enabled or self._closing or self.session.failure is not None:
            return
        if self._drain is not None and not self._drain.done():
            self._again = True
            return
        self._drain = asyncio.ensure_future(self._drain_loop())

    async def _drain_loop(self) -> None:
        try:
            while not self._closing:
                self._again = False
                if self._reconcile_needed:
                    self._reconcile_needed = False
                    await self._reconcile()
                started = await self._reserve()
                for invocation in started:
                    self._start(invocation)
                if not started and not self._again and not self._reconcile_needed:
                    break
        except (SessionFailed, SessionClosed):
            pass
        except Exception as error:  # 预留 / 恢复提交失败 → Session 失败（spec §5.4 末尾）
            self._report(error, {"type": "session_failed"})
        finally:
            self._arm_timer()

    # ================================================================== 恢复型提交：completing 收尾 / 级联中止 / failFast

    async def _reconcile(self) -> None:
        async def body(tx: Transaction) -> None:
            marked: set = set()
            now = self.session.now()
            for record in sorted(self._live.values(), key=lambda r: r["id"]):
                status = record["state"]["status"]
                # (a) completing：结局已定，拥有的工作排空后写最终的 terminal
                if status == "completing":
                    if not self._has_owned_live(record["id"]):
                        outcome = record["state"]["outcome"]
                        if record["state"].get("by") == "scheduler":
                            await self._on_outcome(tx, record, outcome)  # 调度器写的结局：清理推迟到这里
                        tx._set_task(_final(record, outcome, now))
                        tx.retire_docs(task_doc_key(record["id"]))
                    continue
                # (b) 级联中止：祖先（沿所有权链向上）有「取消意图」→ 打中止标记（abort flows down）
                if not record["abort_requested"] and not record["background"] and record["id"] not in marked:
                    if self._cancelling_owner(record) is not None:
                        tx._set_task(dict(record, abort_requested=True, restarts=0))
                        marked.add(record["id"])
                # (c) failFast：等待的任务里有一个失败了 → 其余还活着的全部打中止标记
                if status == "waiting" and record["state"].get("policy") == "failFast" and not record["abort_requested"]:
                    on = record["state"]["on"]
                    if any(self._failed(t) for t in on):
                        for t in on:
                            other = self._live.get(t)
                            if other and not other["abort_requested"] and other["state"]["status"] != "completing" \
                                    and t not in marked:
                                tx._set_task(dict(other, abort_requested=True, restarts=0))
                                marked.add(t)
        await self.session.commit(body)

    # ================================================================== 预留（reserve）

    def blocked_reason(self, record: Dict[str, Any], snapshot: Any = None) -> Optional[str]:
        """阻塞原因是「运行时推导」出来的，不是持久化状态（spec §5.4）。"""
        if not record["abort_requested"] and record.get("restarts", 0) >= self._restart_limit():
            return "restart_limit"  # 崩溃循环保护：被「停放」(parked)
        snapshot = snapshot or self._snapshot()
        return self._fit(record, snapshot.task(record["kind"]))

    @staticmethod
    def _fit(record: Dict[str, Any], definition: Optional[TaskDefinition]) -> Optional[str]:
        if definition is None:
            return "missing_task"  # 扩展没装：任务保持 pending，装上后自动继续
        if definition.version < record["version"]:
            return "task_too_old"
        if definition.version > record["version"] and definition.migrate is None:
            return "migration_failed"
        return None

    def _candidates(self, snapshot: Any, now: float) -> List[Tuple[Dict[str, Any], str]]:
        candidates: List[Tuple[Dict[str, Any], str]] = []
        for record in sorted(self._live.values(), key=lambda r: r["id"]):
            if record["id"] in self._invocations:
                continue
            status = record["state"]["status"]
            if status == "completing":
                continue  # 结局已定，不再跑任何代码
            if record["abort_requested"]:
                # 中止是自下而上的：拥有的工作还活着，就先不跑自己的 abort 处理器
                if not self._has_owned_live(record["id"]):
                    candidates.append((record, "abort"))
                continue
            if not record["background"] and self._cancelling_owner(record) is not None:
                self._reconcile_needed = True  # 祖先要取消了：不再开新的 run 阶段，等级联打标记
                continue
            if status == "waiting" and not all(self._is_terminal(t) for t in record["state"]["on"]):
                continue
            if status == "sleeping" and record["state"]["until"] > now:
                continue
            if self.blocked_reason(record, snapshot) is not None:
                continue
            candidates.append((record, "run"))
        return candidates

    async def _reserve(self) -> List[Invocation]:
        snapshot = self._snapshot()

        # 挑选和写入都在变更线上做：self._live 在线上才是最新的（否则可能用旧记录覆盖掉刚打的中止标记）
        async def body(tx: Transaction) -> List[Invocation]:
            now = self.session.now()
            started: List[Invocation] = []
            for record, mode in self._candidates(snapshot, now):
                definition = snapshot.task(record["kind"])
                misfit = self._fit(record, definition)
                if mode == "abort" and misfit is not None:
                    # 想中止，但代码缺失 / 不兼容：没人能跑 abort 处理器 → orphaned（副作用可能没被清理）
                    await self._terminate(tx, record, {"status": "orphaned", "reason": misfit})
                    continue
                input, checkpoint = record["input"], record["state"]["checkpoint"]
                if mode == "run" and definition.version > record["version"]:
                    try:  # 迁移在预留时进行，与 → running 原子提交
                        input, checkpoint = definition.migrate(input, checkpoint, record["version"])
                    except Exception as error:
                        self._report(error, {"type": "migration_failed", "task": record["id"]})
                        continue
                running = dict(record, input=input, version=definition.version,
                               state={"status": "running", "checkpoint": checkpoint})
                running.setdefault("started_at", now)
                tx._set_task(running)
                started.append(Invocation(running, mode, definition, snapshot))
            return started

        return await self.session.commit(body)

    def _start(self, invocation: Invocation) -> None:
        self._invocations[invocation.task_id] = invocation
        runner = self._run if invocation.mode == "run" else self._run_abort
        invocation.aio = asyncio.ensure_future(runner(invocation))

        def done(future: "asyncio.Future[Any]") -> None:
            invocation.ended = True  # 之后 runtime 的任何操作都会被拒绝
            for timer in invocation.timers:
                timer.cancel()
            if self._invocations.get(invocation.task_id) is invocation:
                del self._invocations[invocation.task_id]
            if not future.cancelled():
                error = future.exception()
                if error is not None and not isinstance(error, (SessionFailed, SessionClosed, InvocationEnded)):
                    self._report(error, {"type": "task_error", "task": invocation.task_id})
            self._check_idle()
            self._kick()

        invocation.aio.add_done_callback(done)

    # ================================================================== run 调用：phase 循环 + step 规则

    async def _run(self, invocation: Invocation) -> None:
        runtime = TaskRuntime(self, invocation)
        current = await self._step(invocation, None, None)
        while current is not None and not self._closing:
            checkpoint = current["state"]["checkpoint"]
            failure: Optional[BaseException] = None
            try:
                handler = invocation.definition.phases.get(checkpoint["phase"])
                if handler is None:
                    raise KeyError(f"task {current['kind']} has no phase {checkpoint['phase']!r}")
                await handler(copy.deepcopy(current), runtime)
            except Exception as error:  # 注意：CancelledError 不是 Exception，会直接冒出去结束这次调用
                failure = error
            current = await self._step(invocation, checkpoint, failure)

    async def _step(self, invocation: Invocation, previous: Optional[Dict[str, Any]],
                    failure: Optional[BaseException]) -> Optional[Dict[str, Any]]:
        """每个 phase 之前（含第一个）在变更线上跑一次，按顺序套用第一条匹配的规则（spec §5.1）。

        返回下一次要跑的任务记录，或 None 表示本次调用结束。
        """

        async def decide(tx: Transaction) -> Optional[Dict[str, Any]]:
            record = await tx.task(invocation.task_id)
            status = record["state"]["status"]
            # 规则 1：已终结 / completing / waiting / sleeping → 停（等待和睡眠不占用调用）
            if status != "running":
                return None
            # 规则 2：正在关闭 → 停，保留 checkpoint 和中止标记，等重开
            if self._closing:
                return None
            # 规则 3：有中止标记 → 结束 run 调用；拥有的工作排空后会另起 abort 调用
            if record["abort_requested"]:
                return None
            if not record["background"] and self._cancelling_owner(record) is not None:
                self._reconcile_needed = True
                return None
            if previous is None:
                return record
            # 规则 4：未捕获的异常 → faulted
            if failure is not None:
                await self._terminate(tx, record, _fault(failure))
                return None
            # 规则 6：checkpoint 没变 → 没有持久化进展 → faulted（防止「看起来跑完了其实啥也没存」）
            if record["state"]["checkpoint"] == previous:
                await self._terminate(tx, record, _fault(
                    f"task {record['kind']} phase {previous['phase']!r} returned without durable progress"))
                return None
            # 规则 5：有进展 → 刷新注册表快照；如果定义被热替换且新定义能接手，就「交接」(handover)
            snapshot = self._snapshot()
            definition = snapshot.task(record["kind"])
            if definition is not invocation.definition and definition is not None and self._fit(record, definition) is None:
                tx._set_task(dict(record, state={"status": "pending", "checkpoint": record["state"]["checkpoint"]}))
                return None
            invocation.snapshot = snapshot
            return record

        return await self.session.commit(decide)

    async def _run_abort(self, invocation: Invocation) -> None:
        """abort 调用：只跑一次 abort 处理器。它必须提交一个终结结局，否则 faulted。"""
        runtime = TaskRuntime(self, invocation)
        record = await self.session.storage.task(invocation.task_id)
        failure: Optional[BaseException] = None
        try:
            await invocation.definition.abort(record, runtime)
        except Exception as error:
            failure = error

        async def decide(tx: Transaction) -> None:
            current = await tx.task(invocation.task_id)
            if current["state"]["status"] != "running" or self._closing:
                return
            await self._terminate(tx, current, _fault(failure or "abort handler returned without an outcome"))

        await self.session.commit(decide)

    async def _terminate(self, tx: Transaction, record: Dict[str, Any], outcome: Dict[str, Any]) -> None:
        """调度器自己写的结局（faulted / orphaned）。拥有的工作还活着就先挂成 completing。"""
        if self._has_owned_live(record["id"], tx):
            held = dict(record, state={"status": "completing", "outcome": outcome, "by": "scheduler"})
            held.pop("memos", None)
            tx._set_task(held)
            return
        await self._on_outcome(tx, record, outcome)
        tx._set_task(_final(record, outcome, self.session.now()))
        tx.retire_docs(task_doc_key(record["id"]))
        self._report(RuntimeError(outcome.get("error", {}).get("message") or outcome.get("reason")),
                     {"type": f"task_{outcome['status']}", "task": record["id"], "kind": record["kind"]})

    # ================================================================== TaskRuntime 背后的提交

    async def _runtime_commit(self, invocation: Invocation, change: Callable[..., Any]) -> None:
        async def body(tx: Transaction) -> None:
            current = await tx.task(invocation.task_id)  # 重新读取并把关
            if current is None or current["state"]["status"] != "running":
                raise InvocationEnded(f"task {invocation.task_id} is not running")
            if invocation.mode == "run" and current["abort_requested"]:
                raise InvocationEnded(f"task {invocation.task_id} has an abort mark")
            if self._closing or invocation.ended:
                raise InvocationEnded("invocation ended")
            state = change(tx, current)
            if inspect.isawaitable(state):
                state = await state
            if state is None:
                return  # 只写了条目 / 文档 / 子任务，任务状态不变
            state = dict(state)
            status = state["status"]
            if status not in ("running", "waiting", "sleeping", "terminal"):
                raise ValueError(f"a task cannot commit status {status!r}")
            if invocation.mode == "abort" and status in ("waiting", "sleeping"):
                raise ValueError("an abort handler cannot wait or sleep")
            if status == "waiting":
                for t in state["on"]:
                    if t == invocation.task_id:
                        raise ValueError("a task cannot wait on itself")
                    if t not in self._meta and t not in tx.tasks_written:
                        raise ValueError(f"cannot wait on missing task {t}")
            if status == "sleeping" and not (isinstance(state["until"], (int, float)) and math.isfinite(state["until"])):
                raise ValueError("sleep until must be a finite time")
            record = dict(current)
            if status == "terminal" and self._has_owned_live(invocation.task_id, tx):
                state = {"status": "completing", "outcome": state["outcome"]}  # 结局已定，等子工作排空
            if state != current["state"]:
                record["restarts"] = 0  # 新状态 = 进展，崩溃计数清零
            record["state"] = state
            if state["status"] == "terminal":
                record = _final(record, state["outcome"], self.session.now())
                tx.retire_docs(task_doc_key(invocation.task_id))
            elif state["status"] == "completing":
                record.pop("memos", None)
            tx._set_task(record)

        await self.session.commit(body, task_id=invocation.task_id, conversation_id=invocation.conversation_id)

    async def _memo(self, invocation: Invocation, name: str, candidate: Any) -> Any:
        async def body(tx: Transaction) -> Any:
            current = await tx.task(invocation.task_id)
            if current is None or current["state"]["status"] != "running" or invocation.ended:
                raise InvocationEnded(f"task {invocation.task_id} is not running")
            memos = dict(current.get("memos") or {})
            if name in memos:
                return memos[name]  # 先写者胜
            memos[name] = candidate
            tx._set_task(dict(current, memos=memos))  # memo 不是「进展」，不清零 restarts
            return candidate

        return await self.session.commit(body)

    # ================================================================== 中止 / 等待 / 空闲

    async def abort(self, id: int) -> str:
        """持久化地打中止标记（spec §5.4 Abort protocol）。返回 "marked" 或 "terminal"。"""
        snapshot = self._snapshot()

        async def body(tx: Transaction) -> str:
            record = await tx.task(id)
            if record is None:
                raise KeyError(f"no task {id}")
            if record["state"]["status"] == "terminal":
                return "terminal"
            marked = dict(record, abort_requested=True, restarts=0)
            # 代码缺失、没有在跑、也没有活跃的子工作 → 当场 orphaned
            if record["state"]["status"] != "completing" and id not in self._invocations \
                    and not self._has_owned_live(id) and self._fit(record, snapshot.task(record["kind"])) is not None:
                await self._terminate(tx, marked, {"status": "orphaned",
                                                   "reason": self._fit(record, snapshot.task(record["kind"]))})
                return "terminal"
            tx._set_task(marked)
            return "marked"

        return await self.session.commit(body)

    async def retry(self, id: int) -> None:
        """让一个被停放（parked）的任务重新跑。"""
        def body(tx: Transaction) -> None:
            record = self._live.get(id)
            if record is not None and record.get("restarts", 0) >= self._restart_limit():
                tx._set_task(dict(record, restarts=0))
        await self.session.commit(body)
        self._parked_reported.discard(id)

    async def wait_for_task(self, id: int) -> Dict[str, Any]:
        loop = asyncio.get_event_loop()
        waiter: "asyncio.Future[Any]" = loop.create_future()
        self._task_waiters.setdefault(id, []).append(waiter)  # 先登记，再读，避免错过
        record = await self.session.storage.task(id)
        if record is None:
            self._task_waiters[id].remove(waiter)
            raise KeyError(f"no task {id}")
        if record["state"]["status"] == "terminal" and not waiter.done():
            waiter.set_result(record)
        return await waiter

    def is_idle(self, conversation_id: Optional[int]) -> bool:
        """会话里是否还有活跃的、非后台的工作（后台任务是边界，不阻止空闲）。"""
        for record in self._live.values():
            if conversation_id is not None and record["conversation_id"] != conversation_id:
                continue
            if not self._behind_background(record):
                return False
        return True

    async def wait_for_idle(self, conversation_id: Optional[int]) -> None:
        waiter: "asyncio.Future[None]" = asyncio.get_event_loop().create_future()
        self._idle_waiters.append((conversation_id, waiter))
        self._check_idle()
        await waiter

    def _check_idle(self) -> None:
        for entry in list(self._idle_waiters):
            conversation_id, waiter = entry
            if waiter.done():
                self._idle_waiters.remove(entry)
            elif self.is_idle(conversation_id):
                waiter.set_result(None)
                self._idle_waiters.remove(entry)

    def _fail_waiters(self, error: BaseException) -> None:
        for waiters in self._task_waiters.values():
            for waiter in waiters:
                if not waiter.done():
                    waiter.set_exception(error)
        self._task_waiters.clear()
        for _, waiter in self._idle_waiters:
            if not waiter.done():
                waiter.set_exception(error)
        self._idle_waiters.clear()

    # ================================================================== 所有权树

    def _node(self, id: int) -> Optional[Dict[str, Any]]:
        return self._live.get(id) or self._meta.get(id)

    def _has_owned_live(self, owner_id: int, tx: Optional[Transaction] = None) -> bool:
        """owner 的「普通拥有工作」里还有活跃任务吗？

        普通拥有工作 = 它拥有的子任务 + 它拥有的会话里的任务，递归下去；后台任务及其子树不算。
        tx 不为空时按「本次提交的候选记录」判断（同一次提交里刚创建的子任务也算）。
        """
        live = self._live
        if tx is not None and tx.tasks_written:
            live = dict(self._live)
            for id, record in tx.tasks_written.items():
                if record["state"]["status"] == "terminal":
                    live.pop(id, None)
                else:
                    live[id] = record
        for record in live.values():
            if record["id"] == owner_id:
                continue
            node: Optional[Dict[str, Any]] = record
            while node is not None and not node.get("background"):
                parent = task_parent(node, self._conversation_owner)
                if parent is None:
                    break
                if parent == owner_id:
                    return True
                node = live.get(parent) or self._meta.get(parent)
        return False

    def _has_intent(self, record: Dict[str, Any]) -> bool:
        """取消意图：中止标记，或者挂起了一个非 completed 的结局。"""
        if record.get("abort_requested"):
            return True
        state = record.get("state", {})
        return state.get("status") == "completing" and state["outcome"]["status"] != "completed"

    def _cancelling_owner(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        node = record
        while True:
            parent_id = task_parent(node, self._conversation_owner)
            if parent_id is None:
                return None
            parent = self._live.get(parent_id)
            if parent is None:  # 已终结的所有者不级联，但继续往上看
                node = self._meta.get(parent_id)
                if node is None:
                    return None
                continue
            if self._has_intent(parent):
                return parent
            if parent["background"]:
                return None  # 后台任务是边界
            node = parent

    def _behind_background(self, record: Dict[str, Any]) -> bool:
        node: Optional[Dict[str, Any]] = record
        while node is not None:
            if node.get("background"):
                return True
            owner = node.get("owner")
            node = self._node(owner) if owner is not None else None
        return False

    def _is_terminal(self, id: int) -> bool:
        return id not in self._live and id in self._meta

    def _failed(self, id: int) -> bool:
        record = self._live.get(id)
        if record is not None:
            state = record["state"]
            return state["status"] == "completing" and state["outcome"]["status"] != "completed"
        outcome = (self._meta.get(id) or {}).get("outcome")
        return outcome is not None and outcome["status"] != "completed"

    # ================================================================== 睡眠、计时器、wake

    def _restart_limit(self) -> float:
        return self.host.settings.get("restart_limit", 5)

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    def _arm_timer(self) -> None:
        """给最早的一个睡眠任务装计时器（spec §5.6）。计时器可能晚触发，触发后重新看时钟。"""
        self._cancel_timer()
        if not self._enabled or self._closing:
            return
        due = [r["state"]["until"] for r in self._live.values()
               if r["state"]["status"] == "sleeping" and not r["abort_requested"] and r["id"] not in self._invocations]
        if due:
            delay = max(0.0, (min(due) - self.session.now()) / 1000)
            self._timer = asyncio.get_event_loop().call_later(delay + 0.001, self._kick)

    def wake(self) -> Dict[str, Any]:
        """宿主（例如 Cloudflare Durable Object）用它决定何时再叫醒进程。"""
        now = self.session.now()
        active = bool(self._invocations)
        at = None
        for record in self._live.values():
            state = record["state"]
            if state["status"] == "sleeping" and not record["abort_requested"]:
                if state["until"] <= now:
                    active = True
                else:
                    at = state["until"] if at is None else min(at, state["until"])
            elif state["status"] in ("pending", "running") and self.blocked_reason(record) is None:
                active = True
            elif state["status"] == "waiting" and all(self._is_terminal(t) for t in state["on"]):
                active = True
        return {"active": active, "at": at}
