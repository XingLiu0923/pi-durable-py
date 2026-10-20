"""文档（Document）：和 transcript 放在一起、在提交里修改的「类型化 JSON 状态」。

对应 TS: src/documents.ts、spec §3。TS 版每个文档是一个 Chord tracker：修改被记录成增量 ops，
提交后 UI 可以拿到「精确的操作流」（watch）。这里简化为整值读写。

文档按作用域寻址：
    conversation 作用域 → key = "c:<会话ID>"
    task 作用域         → key = "t:<任务ID>"   （任务结束时自动「退休」）
    session 作用域      → key = "s"

内置文档（spec §8.2、§6）：
    pi.agent  会话用什么跑：model / instructions / tools / extensions（存的是「名字」，每次使用时去注册表解析）
    pi.live   正在进行的东西：run（谁负责当前这轮 run、它的输入）、generation（流式 partial）、tools（工具槽位与输出）
    pi.inbox  忙碌时排队的 submissions
    pi.usage  token / 花费统计
UI 需要的一切都在这些「已提交」的文档和条目里 —— 没有任何「只在内存里」的进度。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional


@dataclass(frozen=True)
class DocDefinition:
    kind: str
    initial: Callable[[], Dict[str, Any]]
    scope: str = "conversation"  # "conversation" | "task" | "session"
    version: int = 1

    def key(self, owner: Optional[int] = None) -> str:
        if self.scope == "session":
            return "s"
        if owner is None:
            raise ValueError(f"document {self.kind} needs an owner id")
        return f"{self.scope[0]}:{owner}"


def define_doc(kind: str, initial: Callable[[], Dict[str, Any]], scope: str = "conversation", version: int = 1) -> DocDefinition:
    return DocDefinition(kind=kind, initial=initial, scope=scope, version=version)


def task_doc_key(task_id: int) -> str:
    return f"t:{task_id}"


AgentDoc = define_doc("pi.agent", lambda: {})
# run:        {"task_id": 当前负责 run 的 generation 任务, "inputs": [{"id", "entry", "request_id"}]}
#             run 不为 None ⇔ 会话「忙」(busy)
# generation: {"task_id", "partial": 流式输出到一半的文本}
# tools:      {call_id: {"task_id", "name", "status": "pending|running|done", "output": 已提交的输出}}
LiveDoc = define_doc("pi.live", lambda: {"run": None, "generation": None, "tools": {}})
# items: [{"id", "mode": "steer"|"followUp"|"write", "content"?, "entry"?, "request_id"?}]
InboxDoc = define_doc("pi.inbox", lambda: {"items": []})
UsageDoc = define_doc("pi.usage", lambda: {"models": {}, "tools": {}})
