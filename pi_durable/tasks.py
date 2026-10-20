"""任务定义与 TaskRuntime。对应 TS: src/harness/define.ts、src/harness/types.ts，spec §5.1。

任务 = 一个「每一步都存 checkpoint 的状态机」。定义它需要：

    name / version      存进任务记录；重启后靠 name 在注册表里找回代码，version 不同就走 migrate
    initial(input)      第一个 checkpoint，形如 {"phase": "xxx", ...}
    phases              {phase 名: async handler(task, runtime)}；checkpoint["phase"] 决定跑哪个
    abort(task, rt)     中止处理器：负责「撤销自己的副作用」并写下最终结局

写 phase handler 的规矩（spec §5.1、§5.2）：
    - 每个 phase 结束前必须通过 runtime.commit() 持久化「进展」：
        新的 checkpoint（running）/ 等待（waiting）/ 睡眠（sleeping）/ 结局（terminal）。
      什么都没提交就返回 → 调度器判定「没有持久化进展」→ 任务 faulted。
    - phase 可能在崩溃后从头重跑，所以副作用要么幂等，要么用「effect sandwich」：
        commit 意图 → 执行外部副作用 → commit 结果/下一阶段
    - 不能在 phase 中间 sleep：要等时间就 sleep_until() 结束这个 phase，后续工作放到下一个 phase。
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Dict, List, Optional

from .errors import InvocationEnded
from .records import sleeping

if TYPE_CHECKING:  # pragma: no cover
    from .scheduler import Invocation, TaskScheduler

PhaseHandler = Callable[[Dict[str, Any], "TaskRuntime"], Awaitable[None]]


@dataclass(eq=False)  # eq=False：按对象身份比较 —— 「定义对象换了」= 代码被热重载了
class TaskDefinition:
    name: str
    version: int
    initial: Callable[[Any], Dict[str, Any]]
    phases: Dict[str, PhaseHandler]
    abort: PhaseHandler
    # migrate(input, checkpoint, from_version) -> (input, checkpoint)
    migrate: Optional[Callable[[Any, Dict[str, Any], int], Any]] = None
    hooks: Dict[str, Any] = field(default_factory=dict)


def define_task(name: str, version: int, initial: Callable[[Any], Dict[str, Any]], phases: Dict[str, PhaseHandler],
                abort: PhaseHandler, migrate: Optional[Callable[..., Any]] = None) -> TaskDefinition:
    return TaskDefinition(name=name, version=version, initial=initial, phases=phases, abort=abort, migrate=migrate)


_MISSING = object()


class TaskRuntime:
    """一个任务「这一次调用（invocation）」能用的全部能力。

    调用结束后（phase 跑完、被中止、Harness 关闭）任何操作都会抛 InvocationEnded：
    残留的协程再也写不进任何东西。
    """

    def __init__(self, scheduler: "TaskScheduler", invocation: "Invocation") -> None:
        self._scheduler = scheduler
        self._invocation = invocation
        self.task_id: int = invocation.task_id
        self.conversation_id: int = invocation.conversation_id
        self.mode: str = invocation.mode  # "run" | "abort"

    # ---------------------------------------------------------------- 宿主提供的东西
    @property
    def host(self) -> Any:
        """Harness 本身（TS 里是 Harness 注入给调度器的 hooks / agent 解析 / models / settings）。"""
        return self._scheduler.host

    @property
    def models(self) -> Any:
        return self.host.models

    @property
    def settings(self) -> Dict[str, Any]:
        return self.host.settings

    @property
    def registry(self) -> Any:
        """本 phase 的注册表快照；每个 phase 边界刷新一次。"""
        return self._invocation.snapshot

    async def agent(self) -> Any:
        """按本 phase 的注册表快照解析会话的 agent（model / tools / sections / hooks）。"""
        self._check()
        return await self.host.resolve_agent(self.conversation_id, self._invocation.snapshot)

    def now(self) -> float:
        return self._scheduler.session.now()

    def report(self, error: BaseException) -> None:
        self.host.report(error, {"type": "task_error", "task": self.task_id})

    # ---------------------------------------------------------------- 写
    def _check(self) -> None:
        if self._invocation.ended:
            raise InvocationEnded("this task invocation has ended")

    async def commit(self, change: Callable[[Any, Dict[str, Any]], Any]) -> None:
        """在变更线上原子提交。change(tx, current) 返回新状态（或 None 表示状态不变）。

        - 提交前会重新读取任务记录并「把关」：调用已结束 / 正在关闭 / 已终结 /
          （run 调用时）任务带中止标记 → 直接拒绝；
        - 返回的状态和 change 里写的条目、文档、子任务在同一次提交里落盘；
        - 返回 terminal 但自己拥有的工作还活着 → 自动变成 completing（spec §5.5）。
        """
        self._check()
        await self._scheduler._runtime_commit(self._invocation, change)

    async def memo(self, name: str, candidate: Any = _MISSING) -> Any:
        """先写者胜的小值。memo(name) 读；memo(name, value) 写（已有值则返回已有值）。

        典型用法：在副作用之前写 memo，崩溃重跑时看到 memo 就知道副作用已经做过。
        """
        self._check()
        if candidate is _MISSING:
            record = await self._scheduler.session.storage.task(self.task_id)
            return (record or {}).get("memos", {}).get(name)
        return await self._scheduler._memo(self._invocation, name, candidate)

    async def sleep_until(self, until: float, checkpoint: Dict[str, Any]) -> None:
        """持久化地睡到 until（Harness 时钟，毫秒）。存下来就返回；phase 应该紧接着返回。"""
        await self.commit(lambda tx, current: sleeping(checkpoint, until))

    # ---------------------------------------------------------------- 读
    async def get_task(self, id: int) -> Optional[Dict[str, Any]]:
        self._check()
        return await self._scheduler.session.storage.task(id)

    async def wait_for_task(self, id: int) -> Dict[str, Any]:
        self._check()
        return await self._scheduler.wait_for_task(id)

    async def outcomes(self, ids: List[int]) -> List[Dict[str, Any]]:
        """已终结任务的结局，按顺序返回；有一个没终结就抛错。用在 waiting 之后。"""
        self._check()
        result = []
        for id in ids:
            record = await self._scheduler.session.storage.task(id)
            if record is None or record["state"]["status"] != "terminal":
                raise RuntimeError(f"task {id} is not terminal")
            result.append(record["state"]["outcome"])
        return result

    async def abort_owned(self, id: int) -> None:
        """中止一个自己拥有的任务，等到它终结。"""
        self._check()
        record = await self._scheduler.session.storage.task(id)
        if record is None or record.get("owner") != self.task_id:
            raise ValueError(f"task {id} is not owned by task {self.task_id}")
        await self._scheduler.abort(id)
        await self._scheduler.wait_for_task(id)

    async def entry(self, id: int) -> Optional[Dict[str, Any]]:
        self._check()
        return await self._scheduler.session.storage.entry(id)

    async def entries(self, conversation_id: Optional[int] = None, max_id: Optional[int] = None) -> List[Dict[str, Any]]:
        self._check()
        conversation = conversation_id if conversation_id is not None else self.conversation_id
        return await self._scheduler.session.storage.scan_entries(conversation, max_id)

    async def snapshot(self, definition: Any, owner: Optional[int] = None) -> Dict[str, Any]:
        """读已提交的文档（副本）。"""
        self._check()
        return await self._scheduler.session.snapshot(definition, owner if owner is not None else self.conversation_id)

    # ---------------------------------------------------------------- 节流
    def set_timer(self, callback: Callable[[], Any], delay_ms: float) -> Callable[[], None]:
        """phase 内部用来「节奏控制」的计时器（例如节流的进度提交）；调用结束时自动解除。"""
        handle = asyncio.get_event_loop().call_later(delay_ms / 1000, callback)
        self._invocation.timers.append(handle)
        return handle.cancel


async def maybe_await(value: Any) -> Any:
    if inspect.isawaitable(value):
        return await value
    return value
