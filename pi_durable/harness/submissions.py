"""Submission 准入、inbox 与 turn 边界。对应 TS: src/harness/submissions.ts、src/harness/inbox.ts，spec §6。

会话是否「忙」只看一件事：pi.live.run 是否存在。

准入（admission）表（spec §6）：
    | 动作                              | submission 状态         | 其他写入
    | input，空闲且 inbox 为空           | placed（带 user 条目）   | 创建 run 和 generation 任务
    | 纯展示的 write（只有 kind/data）    | done（带条目）           | 立即追加条目，不排队，忙也一样
    | write，空闲且 inbox 为空           | done（带条目）           | 追加条目
    | input 或其他 write，忙或 inbox 非空 | queued                  | 追加 inbox 项
    | 边界放置 user 项                   | placed                  | 加入当前 run 或开启后继 run
    | 边界放置 write                    | done                    | 追加条目
    | run 回答了                        | done（带 answer）        | 移除 run，或交给后继
    | run 失败 / 被中止                  | unanswered（带 reason）  | 移除 run

turn 边界：
    postTools（一轮工具结束后）: 放置 所有 write + steer（默认一次一个）
    final    （run 回答后）    : 放置 所有 write + steer + followUp（默认一次一个）
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from ..documents import InboxDoc, LiveDoc
from ..errors import ConversationBusy
from ..session import Transaction


def user_entry(content: str, entry: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    model = [{"role": "user", "content": content}]
    if entry is not None:  # 用自己的 kind 展示，但模型看到的仍是 content
        return {"kind": entry["kind"], "data": entry.get("data"), "model": model}
    return {"kind": "pi.user", "model": model}


def _submission(id: int, conversation_id: int, type: str, request_id: Optional[str], **fields: Any) -> Dict[str, Any]:
    record = {"id": id, "conversation_id": conversation_id, "type": type, **fields}
    if request_id is not None:
        record["request_id"] = request_id
    return record


async def admit(tx: Transaction, harness: Any, conversation_id: int, request: Dict[str, Any]) -> int:
    kind = request.get("type", "input")
    request_id = request.get("request_id")
    # —— 先读（表读必须在第一次表写之前）——
    if request_id is not None:
        existing = await tx.submission_by_request(conversation_id, request_id)
        if existing is not None:  # 幂等：同一个 request_id 只会有一个 submission
            if existing["type"] != kind:
                raise ValueError("request_id reused for another submission type")
            return existing["id"]
    live = await tx.doc(LiveDoc, conversation_id)
    inbox = await tx.doc(InboxDoc, conversation_id)
    busy = live["run"] is not None
    id = await tx.mint_id()

    if kind == "write":
        entry = dict(request["entry"])
        display_only = "model" not in entry and "head" not in entry
        queued_reset = any(i["mode"] == "write" and i["entry"].get("head") is not None for i in inbox["items"])
        if (display_only and not queued_reset) or (not busy and not inbox["items"]):
            placed = await tx.append_entry(conversation_id, entry)
            tx.put_submission(_submission(id, conversation_id, "write", request_id, status="done", entry=placed["id"]))
            return id
        inbox["items"].append({"id": id, "mode": "write", "entry": entry, "request_id": request_id})
        tx.put_submission(_submission(id, conversation_id, "write", request_id, status="queued"))
        if not busy:  # 空闲但 inbox 非空（例如上次 run 失败留下的）：立刻跑一次 final 边界
            await final_boundary(tx, harness, conversation_id)
        return id

    content = request["content"]
    when_busy = request.get("when_busy", "followUp")
    if busy and when_busy == "reject":
        raise ConversationBusy(f"conversation {conversation_id} is busy")
    if busy or inbox["items"]:
        mode = "steer" if when_busy == "steer" else "followUp"
        inbox["items"].append({"id": id, "mode": mode, "content": content, "entry": request.get("entry"),
                               "request_id": request_id})
        tx.put_submission(_submission(id, conversation_id, "input", request_id, status="queued"))
        if not busy:
            await final_boundary(tx, harness, conversation_id)
        return id

    placed = await tx.append_entry(conversation_id, user_entry(content, request.get("entry")))
    tx.put_submission(_submission(id, conversation_id, "input", request_id, status="placed", entry=placed["id"]))
    await start_run(tx, conversation_id, [{"id": id, "entry": placed["id"], "request_id": request_id}])
    return id


async def start_run(tx: Transaction, conversation_id: int, inputs: List[Dict[str, Any]]) -> int:
    """开启一轮 run：创建会话拥有的 generation 任务，并把 run 交给它。"""
    from .generation import GenerationTask

    generation = await tx.create_task(GenerationTask, {}, conversation_id=conversation_id)
    live = await tx.doc(LiveDoc, conversation_id)
    live["run"] = {"task_id": generation, "inputs": inputs}
    live["generation"] = {"task_id": generation, "partial": None}
    return generation


async def place_items(tx: Transaction, harness: Any, conversation_id: int,
                      boundary: str) -> Tuple[List[Dict[str, Any]], bool]:
    """在边界放置排队项。先放 write，再放 user 项（都按 ID 顺序）。返回 (放置的 user 输入, 是否放置了 reset)。"""
    inbox = await tx.doc(InboxDoc, conversation_id)
    items = inbox["items"]
    settings = harness.settings

    def pick(mode: str, setting: str) -> List[Dict[str, Any]]:
        chosen = [i for i in items if i["mode"] == mode]
        return chosen if settings.get(setting) == "all" else chosen[:1]

    writes = [i for i in items if i["mode"] == "write"]
    reset = any(w["entry"].get("head") is not None for w in writes)
    steers = pick("steer", "steering_mode")
    # 在 postTools 边界放置 reset 时，它的行为等同 final：也会选出 followUp
    follow_ups = pick("followUp", "follow_up_mode") if boundary == "final" or reset else []
    for write in writes:
        placed = await tx.append_entry(conversation_id, write["entry"])
        tx.put_submission(_submission(write["id"], conversation_id, "write", write.get("request_id"),
                                      status="done", entry=placed["id"]))
    inputs: List[Dict[str, Any]] = []
    for item in sorted(steers + follow_ups, key=lambda i: i["id"]):
        placed = await tx.append_entry(conversation_id, user_entry(item["content"], item.get("entry")))
        tx.put_submission(_submission(item["id"], conversation_id, "input", item.get("request_id"),
                                      status="placed", entry=placed["id"]))
        inputs.append({"id": item["id"], "entry": placed["id"], "request_id": item.get("request_id")})
    selected = {i["id"] for i in writes + steers + follow_ups}
    inbox["items"] = [i for i in items if i["id"] not in selected]
    return inputs, reset


def settle_inputs(tx: Transaction, conversation_id: int, inputs: List[Dict[str, Any]], status: str,
                  answer: Optional[int] = None, reason: Optional[str] = None, detail: Any = None) -> None:
    for item in inputs:
        fields: Dict[str, Any] = {"status": status, "entry": item["entry"]}
        if status == "done":
            fields["answer"] = answer
        else:
            fields["reason"] = reason
            if detail is not None:
                fields["detail"] = detail
        tx.put_submission(_submission(item["id"], conversation_id, "input", item.get("request_id"), **fields))


async def final_boundary(tx: Transaction, harness: Any, conversation_id: int) -> None:
    """run 成功结束（输入已经 settle）：放置排队项；有 user 项就开启后继 run，否则会话变空闲。"""
    live = await tx.doc(LiveDoc, conversation_id)
    live["run"] = None
    live["generation"] = None
    live["tools"] = {}
    inputs, _ = await place_items(tx, harness, conversation_id, "final")
    if inputs:
        await start_run(tx, conversation_id, inputs)


async def fail_run(tx: Transaction, task: Dict[str, Any], reason: str, detail: Any = None) -> None:
    """run 失败：输入 settle 为 unanswered；inbox 保持原样（spec §6 afterFailedRun 默认 keep）。"""
    conversation_id = task["conversation_id"]
    live = await tx.doc(LiveDoc, conversation_id)
    run = live["run"]
    if run is not None and run["task_id"] == task["id"]:
        settle_inputs(tx, conversation_id, run["inputs"], "unanswered", reason=reason, detail=detail)
        live["run"] = None
        live["generation"] = None
        live["tools"] = {}
