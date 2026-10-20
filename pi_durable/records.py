"""核心记录类型。

对应 TS 源码：src/types.ts，规范 docs/spec.md §2「Core records」与 §5.1「Task」。

所有记录在运行时都是普通 dict（纯 JSON），这样能原封不动地写进 SQLite / JSONL。
下面的 TypedDict 只是给读代码的人看「长什么样」，Python 运行时不做校验。

四张「表」：
    conversations  会话（一个 transcript 作用域）
    entries        条目（transcript 里不可变的一条记录）
    tasks          任务（可持久化的状态机）
    submissions    提交（用户交给会话的输入 / 写入，可以等待它的结果）
另外还有「文档」（documents），见 documents.py。
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

try:  # Python 3.9 也有 TypedDict
    from typing import TypedDict
except ImportError:  # pragma: no cover
    TypedDict = dict  # type: ignore

Json = Any

# 根会话固定为 1（TS: ROOT_CONVERSATION_ID = 1）。其余 ID 都由 Storage.mint_id() 分配。
ROOT_CONVERSATION_ID = 1


class ConversationRecord(TypedDict, total=False):
    id: int
    # 被哪个任务拥有（例如子 agent 的会话由「subagent 工具任务」拥有）。
    # 这条所有权边把「会话」接进了任务的所有权树，用于级联中止和空闲判断。
    owner: Optional[Dict[str, int]]  # {"conversation_id": .., "task_id": ..}


class EntryRecord(TypedDict, total=False):
    id: int
    conversation_id: int
    kind: str  # "pi.user" / "pi.assistant" / "pi.tool-result" / "pi.system" / "pi.reset" / 你自己的 kind
    model: List[Dict[str, Any]]  # 模型能看到的消息（没有 model 的条目模型看不到，只给 UI 用）
    data: Json  # 给程序 / UI 的附加数据
    head: int  # 「上下文起点」标记：reset / compaction 用它让模型从这里开始看
    by_task_id: int  # 哪个任务写的


class TaskRecord(TypedDict, total=False):
    id: int
    conversation_id: int
    kind: str  # 任务定义名，例如 "pi.generation" / "pi.tool" / "app.ticker"
    version: int
    input: Json
    owner: Optional[int]  # 所属的父任务；None 表示由会话直接拥有（顶层任务）
    background: bool  # 后台任务：会话 abort / idle 不会穿过它
    abort_requested: bool  # 持久化的「中止标记」
    restarts: int  # 连续被崩溃打断的次数（达到 restart_limit 就「停放」parked）
    state: Dict[str, Any]  # 见下面的 TaskState
    memos: Dict[str, Json]  # 先写者胜的小值（防止副作用重复执行）
    started_at: float
    ended_at: float


# TaskState 是一个带 status 的联合类型（spec §5.1）：
#   {"status": "pending",    "checkpoint": S}
#   {"status": "running",    "checkpoint": S}
#   {"status": "waiting",    "checkpoint": S, "on": [task ids], "policy": "allSettled" | "failFast"}
#   {"status": "sleeping",   "checkpoint": S, "until": epoch_ms}
#   {"status": "completing", "outcome": O}      # 结果已定，但还在等自己拥有的子工作结束
#   {"status": "terminal",   "outcome": O}
#
# TaskOutcome:
#   {"status": "completed", "result": R}
#   {"status": "failed",    "error": {"message": ..}, "result"?: R}
#   {"status": "aborted",   "reason"?: str, "result"?: R}   # 任务自己的 abort 处理器跑过并决定的
#   {"status": "orphaned",  "reason": str}                 # 想中止它，但它的代码不在了，没人能清理
#   {"status": "faulted",   "error": {"message": ..}}       # 任务代码有 bug：抛异常 / 没有持久化进展

LIVE_STATUSES = ("pending", "running", "waiting", "sleeping", "completing")


class SubmissionRecord(TypedDict, total=False):
    id: int
    conversation_id: int
    type: str  # "input" | "write"
    status: str  # "queued" | "placed" | "done" | "unanswered"
    request_id: str  # 幂等键：同一个 request_id 重复提交只会得到同一个 submission
    entry: int  # 放置后对应的条目
    answer: int  # input 被回答后，回答所在的条目
    reason: str  # unanswered 的原因：aborted / model_error / no_model / faulted / ...
    detail: Json


# ---------------------------------------------------------------------------
# 构造 TaskState / TaskOutcome 的小工具，让任务代码读起来更像 TS 版本
# ---------------------------------------------------------------------------


def running(checkpoint: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": "running", "checkpoint": checkpoint}


def waiting(checkpoint: Dict[str, Any], on: List[int], policy: str = "allSettled") -> Dict[str, Any]:
    return {"status": "waiting", "checkpoint": checkpoint, "on": list(on), "policy": policy}


def sleeping(checkpoint: Dict[str, Any], until: float) -> Dict[str, Any]:
    return {"status": "sleeping", "checkpoint": checkpoint, "until": until}


def terminal(outcome: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": "terminal", "outcome": outcome}


def completed(result: Json = None) -> Dict[str, Any]:
    return terminal({"status": "completed", "result": result})


def failed(message: str, result: Json = None, detail: Json = None) -> Dict[str, Any]:
    outcome: Dict[str, Any] = {"status": "failed", "error": {"message": message}}
    if detail is not None:
        outcome["error"]["detail"] = detail
    if result is not None:
        outcome["result"] = result
    return terminal(outcome)


def aborted(reason: Optional[str] = None, result: Json = None) -> Dict[str, Any]:
    outcome: Dict[str, Any] = {"status": "aborted"}
    if reason is not None:
        outcome["reason"] = reason
    if result is not None:
        outcome["result"] = result
    return terminal(outcome)


def is_live(task: Dict[str, Any]) -> bool:
    return task["state"]["status"] != "terminal"


def task_parent(task: Dict[str, Any], conversation_owner: Dict[int, int]) -> Optional[int]:
    """任务在所有权树里的「父任务」。

    - 子任务：owner 字段直接指向父任务；
    - 会话拥有的顶层任务：如果这个会话本身被某个任务拥有（子 agent），父任务就是那个任务。
    """
    owner = task.get("owner")
    if owner is not None:
        return owner
    return conversation_owner.get(task["conversation_id"])
