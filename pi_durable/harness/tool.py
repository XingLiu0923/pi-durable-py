"""内置任务 pi.tool：一次工具调用 = 一个持久化任务。对应 TS: src/harness/tool.ts，spec §7.3、§8.4。

    checkpoint: {"phase": "call"}                                      ← 初始
              → {"phase": "execute", "arguments": .., "replay": ..}   ← 已提交「意图」

这就是 effect sandwich（spec §5.2）：

    call 阶段：解析工具 → 校验参数 → before_tool 钩子 → 【提交意图：进入 execute】
              → 执行工具（外部副作用，不在提交里）→ after_tool 钩子 → 【提交结果条目，任务完成】

    如果进程在「意图」和「结果」之间死了，重开后任务停在 execute 阶段：
        replay="safe"   → 用存下的参数重新执行
        replay="unsafe" → 不重跑，提交一个 interrupted 错误结果（带已提交的部分输出）
    execute 阶段只有恢复时才会走到。
"""

from __future__ import annotations

import asyncio
import inspect
import time
from typing import Any, Dict, Optional

from ..documents import LiveDoc, UsageDoc
from ..records import aborted, completed, failed, running
from ..registry import Tool, validate_arguments
from ..tasks import TaskRuntime, define_task


class ToolApi:
    """工具 execute(args, api) 拿到的 api。"""

    def __init__(self, rt: TaskRuntime, call: Dict[str, Any]) -> None:
        self._rt = rt
        self.call = call
        self.task_id = rt.task_id
        self.conversation_id = rt.conversation_id
        self.text = ""
        self._last = 0.0

    async def output(self, text: str) -> None:
        """流式输出。节流地提交到 pi.live 的工具槽位 —— UI 看到的输出都已落盘，崩溃最多丢一个节流窗口。"""
        self.text += text
        if self._rt.now() - self._last >= self._rt.settings["progress"]["output_interval_ms"]:
            await self.flush()

    async def flush(self) -> None:
        self._last = self._rt.now()
        snapshot = self.text
        call_id = self.call["id"]

        async def change(tx: Any, current: Dict[str, Any]) -> None:
            slot = (await tx.doc(LiveDoc, self.conversation_id))["tools"].get(call_id)
            if slot is not None:
                slot["output"] = snapshot

        await self._rt.commit(change)

    async def memo(self, name: str, *candidate: Any) -> Any:
        return await self._rt.memo(name, *candidate)


async def _find_call(rt: TaskRuntime, task: Dict[str, Any]) -> Dict[str, Any]:
    entry = await rt.entry(task["input"]["assistant"])
    message = entry["model"][0]
    return next(c for c in message["tool_calls"] if c["id"] == task["input"]["call_id"])


def _error(text: str) -> Dict[str, Any]:
    return {"content": text, "is_error": True}


async def _execute(rt: TaskRuntime, tool: Tool, call: Dict[str, Any], arguments: Dict[str, Any]) -> Dict[str, Any]:
    api = ToolApi(rt, call)
    started = time.monotonic()
    try:
        value = tool.execute(arguments, api)
        if inspect.isawaitable(value):
            value = await value
    except asyncio.CancelledError:
        raise  # 被中止 / 关闭：交给 abort 处理器 / 重开后的恢复
    except Exception as error:  # 工具抛错 → 错误结果，任务 failed（但 run 继续）
        return {"content": f"tool_error: {error}", "is_error": True, "failed": True,
                "duration_ms": int((time.monotonic() - started) * 1000)}
    if value is None:
        result: Dict[str, Any] = {"content": api.text}
    elif isinstance(value, str):
        result = {"content": value}
    else:
        result = dict(value)
        result.setdefault("content", api.text)
    result["duration_ms"] = int((time.monotonic() - started) * 1000)
    agent = await rt.agent()
    for handler in agent.handlers("tool", "after_tool"):
        replaced = handler(call, result)
        if replaced is not None:
            result = replaced
    return result


async def _commit_result(rt: TaskRuntime, task: Dict[str, Any], call: Dict[str, Any], result: Dict[str, Any],
                         outcome: str = "completed") -> None:
    conversation_id = task["conversation_id"]

    async def change(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
        message = {"role": "tool_result", "call_id": call["id"], "name": call["name"],
                   "content": result["content"], "is_error": bool(result.get("is_error"))}
        entry = await tx.append_entry(conversation_id, {"kind": "pi.tool-result", "model": [message],
                                                        "data": {"duration_ms": result.get("duration_ms")}})
        live = await tx.doc(LiveDoc, conversation_id)
        slot = live["tools"].get(call["id"])
        if slot is not None:
            slot["status"] = "done"
            slot["output"] = result["content"]
        usage = await tx.doc(UsageDoc, conversation_id)
        usage["tools"][call["name"]] = usage["tools"].get(call["name"], 0) + 1
        value: Dict[str, Any] = {"entry_id": entry["id"]}
        if result.get("control"):
            value["control"] = result["control"]
        if outcome == "aborted":
            return aborted("aborted", result=value)
        if outcome == "failed" or result.get("failed"):
            return failed(result["content"], result=value)
        return completed(value)  # is_error 的结果也算 completed：模型会看到错误并继续

    await rt.commit(change)


async def _set_slot(rt: TaskRuntime, call_id: str, status: str, state: Dict[str, Any]) -> None:
    async def change(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
        slot = (await tx.doc(LiveDoc, rt.conversation_id))["tools"].get(call_id)
        if slot is not None:
            slot["status"] = status
        return state

    await rt.commit(change)


async def call_phase(task: Dict[str, Any], rt: TaskRuntime) -> None:
    call = await _find_call(rt, task)
    agent = await rt.agent()
    tool: Optional[Tool] = agent.tools.get(call["name"])
    if tool is None:
        return await _commit_result(rt, task, call, _error(f"tool_unavailable: no tool named {call['name']!r}"))
    arguments = call["arguments"]
    problem = validate_arguments(tool, arguments)
    if problem is not None:
        return await _commit_result(rt, task, call, _error(f"invalid_arguments: {problem}"))
    for handler in agent.handlers("tool", "before_tool"):
        decision = handler(dict(call, arguments=arguments))
        if decision and "block" in decision:
            return await _commit_result(rt, task, call, _error(f"blocked: {decision['block']}"))
        if decision and "arguments" in decision:
            arguments = decision["arguments"]
    problem = validate_arguments(tool, arguments)  # 钩子改写后再校验一次
    if problem is not None:
        return await _commit_result(rt, task, call, _error(f"invalid_arguments: {problem}"))
    # ① 提交意图（之后崩溃 → 恢复时走 execute 阶段）
    await _set_slot(rt, call["id"], "running",
                    running({"phase": "execute", "arguments": arguments, "replay": tool.replay}))
    # ② 执行外部副作用
    result = await _execute(rt, tool, call, arguments)
    # ③ 提交结果
    await _commit_result(rt, task, call, result)


async def execute_phase(task: Dict[str, Any], rt: TaskRuntime) -> None:
    """只有恢复才会走到：上一个进程提交了意图，但没来得及提交结果。"""
    checkpoint = task["state"]["checkpoint"]
    call = await _find_call(rt, task)
    tool = (await rt.agent()).tools.get(call["name"])
    if checkpoint["replay"] == "safe" and tool is not None and tool.replay == "safe":
        result = await _execute(rt, tool, call, checkpoint["arguments"])
        return await _commit_result(rt, task, call, result)
    live = await rt.snapshot(LiveDoc)
    partial = (live["tools"].get(call["id"]) or {}).get("output", "")
    await _commit_result(rt, task, call, _error(
        "interrupted: the process stopped while this call was running; it was not retried "
        f"because the tool is not replay-safe. Output so far: {partial!r}"), outcome="failed")


async def abort_tool(task: Dict[str, Any], rt: TaskRuntime) -> None:
    call = await _find_call(rt, task)
    live = await rt.snapshot(LiveDoc)
    partial = (live["tools"].get(call["id"]) or {}).get("output", "")
    await _commit_result(rt, task, call, _error(f"aborted. Output so far: {partial!r}"), outcome="aborted")


ToolTask = define_task(
    name="pi.tool",
    version=1,
    initial=lambda input: {"phase": "call"},
    phases={"call": call_phase, "execute": execute_phase},
    abort=abort_tool,
)
