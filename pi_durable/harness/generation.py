"""内置任务 pi.generation：调用模型，处理回答 / 工具轮 / 重试。对应 TS: src/harness/generation.ts，spec §8.3、§8.5。

    prepare ──▶ request ──┬─ stop       → 追加回答，settle 输入 done，final 边界 → terminal(completed)
                          ├─ tool_use   → 追加 assistant，为每个调用创建 pi.tool 子任务，waiting(on=工具任务)
                          │                └─▶ tools：放置 steer，创建「下一个」generation 并把 run 交给它 → completed
                          ├─ 可重试错误  → 追加错误条目，sleeping(until=退避) → retry → prepare(attempt+1)
                          └─ 其他错误    → settle 输入 unanswered(model_error) → failed

一次问答，在条目和任务层面长这样（README「Concepts」那张图）：

    submit(input) → pi.user
      pi.generation → pi.system（只有提示词/工具变了才写）, pi.assistant（工具调用）
        pi.tool × n → pi.tool-result × n   （被 generation 拥有，generation 等它们）
      pi.generation → pi.assistant（回答）→ submission done
"""

from __future__ import annotations

from typing import Any, Dict

from ..context import active_range, effective_sections, model_messages, section_patch
from ..documents import LiveDoc, UsageDoc
from ..records import aborted, completed, failed, running, sleeping, waiting
from ..tasks import TaskRuntime, define_task
from .submissions import fail_run, final_boundary, place_items, settle_inputs, start_run
from .tool import ToolTask


def tool_spec(tool: Any) -> Dict[str, Any]:
    return {"name": tool.name, "description": tool.description,
            "parameters": {k: v.__name__ for k, v in tool.parameters.items()}}


async def convert_partial(tx: Any, conversation_id: int) -> None:
    """被打断的流式输出不会丢：转成一条 stop_reason=aborted 的 assistant 条目（模型以后看不到它，UI 能看到）。"""
    live = await tx.doc(LiveDoc, conversation_id)
    generation = live.get("generation")
    if generation and generation.get("partial"):
        await tx.append_entry(conversation_id, {"kind": "pi.assistant", "model": [{
            "role": "assistant", "content": generation["partial"], "tool_calls": [], "stop_reason": "aborted"}]})
        generation["partial"] = None


async def _usage(tx: Any, conversation_id: int, model: str, message: Dict[str, Any]) -> None:
    usage = await tx.doc(UsageDoc, conversation_id)
    totals = usage["models"].setdefault(model, {"input": 0, "output": 0, "requests": 0})
    totals["input"] += message.get("usage", {}).get("input", 0)
    totals["output"] += message.get("usage", {}).get("output", 0)
    totals["requests"] += 1


async def _no_model(task: Dict[str, Any], rt: TaskRuntime, model: Any) -> None:
    async def change(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
        await fail_run(tx, task, "no_model", f"unknown model {model!r}")
        return failed("no_model")

    await rt.commit(change)


async def prepare(task: Dict[str, Any], rt: TaskRuntime) -> None:
    checkpoint = task["state"]["checkpoint"]
    conversation_id = task["conversation_id"]
    agent = await rt.agent()
    if rt.models.get(agent.model) is None:
        return await _no_model(task, rt, agent.model)
    entries = await rt.entries()
    current = effective_sections(active_range(entries))
    wanted = agent.render_sections({"conversation_id": conversation_id, "agent": agent})
    patch = section_patch(current, wanted)  # 只写变了的 section
    tail = entries[-1]["id"] if entries else 0

    async def change(tx: Any, record: Dict[str, Any]) -> Dict[str, Any]:
        cutoff = tail
        if patch:
            system = await tx.append_entry(conversation_id, {"kind": "pi.system",
                                                             "model": [{"role": "system", "sections": patch}]})
            cutoff = system["id"]
        # 模型、工具、cutoff 固定在 checkpoint 里：恢复时重发完全相同的请求
        return running({"phase": "request", "attempt": checkpoint["attempt"], "model": agent.model,
                        "tools": sorted(agent.tools), "cutoff": cutoff})

    await rt.commit(change)


async def request(task: Dict[str, Any], rt: TaskRuntime) -> None:
    checkpoint = task["state"]["checkpoint"]
    conversation_id = task["conversation_id"]
    model = rt.models.get(checkpoint["model"])
    if model is None:
        return await _no_model(task, rt, checkpoint["model"])
    # 上一次尝试被打断时留下的 partial：先转成 aborted 条目
    if ((await rt.snapshot(LiveDoc)).get("generation") or {}).get("partial"):
        await rt.commit(lambda tx, current: convert_partial(tx, conversation_id))
    entries = await rt.entries(max_id=checkpoint["cutoff"])
    messages = model_messages(active_range(entries))
    agent = await rt.agent()
    tools = [tool_spec(agent.tools[n]) for n in checkpoint["tools"] if n in agent.tools]
    for handler in agent.handlers("generation", "before_request"):
        replaced = handler(messages)
        if replaced is not None:
            messages = replaced

    async def save_partial(text: str) -> None:
        async def change(tx: Any, current: Dict[str, Any]) -> None:
            live = await tx.doc(LiveDoc, conversation_id)
            live["generation"] = {"task_id": task["id"], "partial": text}
        await rt.commit(change)

    # 流式请求是外部副作用：不在提交里。partial 按 partial_interval_ms 节流提交
    interval = rt.settings["progress"]["partial_interval_ms"]
    text, last, message = "", 0.0, None
    async for event in model.stream({"messages": messages, "tools": tools}):
        if event["type"] == "text_delta":
            text += event["delta"]
            if rt.now() - last >= interval:
                last = rt.now()
                await save_partial(text)
        elif event["type"] == "done":
            message = event["message"]
    for handler in agent.handlers("generation", "after_response"):
        handler(message)
    await _classify(task, rt, checkpoint, message)


async def _classify(task: Dict[str, Any], rt: TaskRuntime, checkpoint: Dict[str, Any], message: Dict[str, Any]) -> None:
    """对终结消息分类，一次提交（同时清掉 partial）。"""
    conversation_id = task["conversation_id"]
    model = checkpoint["model"]
    stop = message.get("stop_reason")

    if stop == "tool_use" and message.get("tool_calls"):
        async def tool_round(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
            await _usage(tx, conversation_id, model, message)
            assistant = await tx.append_entry(conversation_id, {"kind": "pi.assistant", "model": [message]})
            live = await tx.doc(LiveDoc, conversation_id)
            ids, slots = [], {}
            for call in message["tool_calls"]:
                # 每个调用一个 pi.tool 任务，由本 generation 拥有（并行执行）
                id = await tx.create_task(ToolTask, {"assistant": assistant["id"], "call_id": call["id"]},
                                          owner=task["id"])
                ids.append(id)
                slots[call["id"]] = {"task_id": id, "name": call["name"], "status": "pending", "output": ""}
            live["tools"] = slots
            live["generation"] = None
            return waiting({"phase": "tools", "assistant": assistant["id"], "tools": ids}, ids, "allSettled")

        return await rt.commit(tool_round)

    if stop in ("stop", "length", "tool_use"):
        async def answer(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
            await _usage(tx, conversation_id, model, message)
            entry = await tx.append_entry(conversation_id, {"kind": "pi.assistant", "model": [message]})
            live = await tx.doc(LiveDoc, conversation_id)
            run = live["run"]
            if run is not None and run["task_id"] == task["id"]:
                settle_inputs(tx, conversation_id, run["inputs"], "done", answer=entry["id"])
                await final_boundary(tx, rt.host, conversation_id)  # 有 followUp 就开下一轮 run
            return completed({"entry_id": entry["id"]})

        return await rt.commit(answer)

    retry = rt.settings["retry"]
    if stop == "error" and message.get("retryable") and checkpoint["attempt"] <= retry["max_retries"]:
        until = rt.now() + retry["base_delay_ms"] * 2 ** (checkpoint["attempt"] - 1)

        async def backoff(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
            await _usage(tx, conversation_id, model, message)
            await tx.append_entry(conversation_id, {"kind": "pi.assistant", "model": [message]})
            live = await tx.doc(LiveDoc, conversation_id)
            live["generation"] = {"task_id": task["id"], "partial": None}
            # 退避用的是「持久化睡眠」，不是 asyncio.sleep：重启后照样按时醒来
            return sleeping({"phase": "retry", "attempt": checkpoint["attempt"], "until": until}, until)

        return await rt.commit(backoff)

    async def give_up(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
        await tx.append_entry(conversation_id, {"kind": "pi.assistant", "model": [message]})
        await fail_run(tx, task, "model_error", message.get("error"))
        return failed(f"model_error: {message.get('error')}")

    await rt.commit(give_up)


async def tools_phase(task: Dict[str, Any], rt: TaskRuntime) -> None:
    """所有工具任务都终结后才会到这里（waiting 结束）。"""
    checkpoint = task["state"]["checkpoint"]
    conversation_id = task["conversation_id"]
    outcomes = await rt.outcomes(checkpoint["tools"])
    agent = await rt.agent()
    for handler in agent.handlers("generation", "after_tools"):
        handler(outcomes)
    controls = [(o.get("result") or {}).get("control") for o in outcomes]
    terminate = bool(controls) and all(c and c.get("terminate") for c in controls)
    result = {"entry_id": checkpoint["assistant"]}

    async def change(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
        live = await tx.doc(LiveDoc, conversation_id)
        run = live["run"]
        live["tools"] = {}
        if run is None or run["task_id"] != task["id"]:
            return completed(result)
        if terminate:  # 每个结果都要求 terminate：不再请求模型，run 结束
            settle_inputs(tx, conversation_id, run["inputs"], "done", answer=checkpoint["assistant"])
            await final_boundary(tx, rt.host, conversation_id)
            return completed(result)
        inputs, reset = await place_items(tx, rt.host, conversation_id, "postTools")
        if reset:  # 工具轮期间排了一个 reset：当前 run 没回答就被切断
            settle_inputs(tx, conversation_id, run["inputs"], "unanswered", reason="reset")
            live["run"] = None
            live["generation"] = None
            if inputs:
                await start_run(tx, conversation_id, inputs)
            return completed(result)
        # 普通情况：steer 加入当前 run；创建下一个 generation（会话拥有）并把 run 交给它
        successor = await tx.create_task(GenerationTask, {}, conversation_id=conversation_id)
        live["run"] = {"task_id": successor, "inputs": run["inputs"] + inputs}
        live["generation"] = {"task_id": successor, "partial": None}
        return completed(result)

    await rt.commit(change)


async def retry_phase(task: Dict[str, Any], rt: TaskRuntime) -> None:
    checkpoint = task["state"]["checkpoint"]
    if rt.now() < checkpoint["until"]:  # 被提前叫醒：用同一个 checkpoint 再睡
        return await rt.sleep_until(checkpoint["until"], checkpoint)
    # 回到 prepare：退避期间的 agent / settings 变化在下一次尝试中生效
    await rt.commit(lambda tx, current: running({"phase": "prepare", "attempt": checkpoint["attempt"] + 1}))


async def abort_generation(task: Dict[str, Any], rt: TaskRuntime) -> None:
    """只在它拥有的工具任务都终结后才会跑（中止自下而上）。"""
    conversation_id = task["conversation_id"]

    async def change(tx: Any, current: Dict[str, Any]) -> Dict[str, Any]:
        await convert_partial(tx, conversation_id)
        live = await tx.doc(LiveDoc, conversation_id)
        run = live["run"]
        if run is not None and run["task_id"] == task["id"]:
            settle_inputs(tx, conversation_id, run["inputs"], "unanswered", reason="aborted")
            live["run"] = None
            live["generation"] = None
            live["tools"] = {}
        return aborted("aborted")

    await rt.commit(change)


GenerationTask = define_task(
    name="pi.generation",
    version=1,
    initial=lambda input: {"phase": "prepare", "attempt": 1},
    phases={"prepare": prepare, "request": request, "tools": tools_phase, "retry": retry_phase},
    abort=abort_generation,
)
