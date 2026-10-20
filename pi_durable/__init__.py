"""pi_durable —— @earendil-works/pi-durable 的 Python 教学版迷你实现。

分层（自底向上）：
    storage/   Storage 抽象 + Memory / SQLite 实现       ← 原子提交、全局 ID
    session    Session（唯一变更线）+ Transaction          ← 只有已提交的状态可见
    documents  类型化 JSON 文档（pi.live / pi.inbox / ...）
    tasks      define_task + TaskRuntime                   ← 持久化状态机
    scheduler  TaskScheduler                               ← 恢复、预留、step 规则、中止、结构化并发
    registry   Extension / Registry / Tool / Hook / Agent  ← 代码（不存储）
    harness/   Harness / Conversation / Submission + 内置任务 pi.generation、pi.tool
"""

from .documents import AgentDoc, InboxDoc, LiveDoc, UsageDoc, define_doc
from .errors import ConversationBusy, InvocationEnded, ReadAfterWrite, SessionClosed, SessionFailed
from .harness import Conversation, GenerationTask, Harness, Submission, ToolTask
from .models import FakeModel, Models
from .records import aborted, completed, failed, running, sleeping, terminal, waiting
from .registry import Registry, define_extension, define_tool, hook, section
from .storage import MemoryStorage, SqliteStorage
from .tasks import TaskRuntime, define_task

__all__ = [
    "Harness", "Conversation", "Submission", "GenerationTask", "ToolTask",
    "MemoryStorage", "SqliteStorage", "FakeModel", "Models",
    "Registry", "define_extension", "define_tool", "hook", "section",
    "define_task", "TaskRuntime", "define_doc", "AgentDoc", "InboxDoc", "LiveDoc", "UsageDoc",
    "running", "waiting", "sleeping", "terminal", "completed", "failed", "aborted",
    "ConversationBusy", "InvocationEnded", "ReadAfterWrite", "SessionClosed", "SessionFailed",
]
