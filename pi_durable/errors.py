"""错误类型。对应 TS: src/errors.ts，以及 spec §10.1 的三条错误规则：

1. 存储错误终结整个 Harness（SessionFailed），必须重新打开；
2. 非法读取只让那一次调用失败；
3. 你的代码只让它自己的单元失败：任务阶段抛错 → 该任务 faulted；工具抛错 → 错误结果；
   提交回调抛错 → 该次提交回滚。
"""


class DurableError(Exception):
    pass


class SessionFailed(DurableError):
    """存储出错后，Session 失败；之后的所有调用都会抛这个。"""


class SessionClosed(DurableError):
    pass


class ReadAfterWrite(DurableError):
    """同一个事务里，第一次「表写入」之后不能再做「表读取」（spec §4）。

    原因：表写入是缓冲在事务里的，读会直接打到 Storage，看不到这些缓冲写入。
    与其返回一个陈旧的结果，不如直接禁止。文档（document）的读写不受此限制。
    """


class ConversationBusy(DurableError):
    """when_busy="reject" 时，会话忙会抛这个。"""


class InvocationEnded(DurableError):
    """任务的一次调用（invocation）已结束，或被中止标记 / 关闭拦截后，再用它的 runtime 就会抛这个。"""
