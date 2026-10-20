"""上下文推导：把 transcript 条目变成发给模型的消息。对应 TS: src/harness/context.ts，spec §2.1。

条目永不删除。reset / compaction 只是追加一个带 `head` 的条目，模型从那里开始看：

    1. 找到最新的带 head 的条目 H；
    2. 起点 = H.head（没有 H 就从头开始）；
    3. 上下文 = [H] + 起点之后所有不带 head 的条目；
    7. 每个 assistant 的工具结果紧跟在它后面（按调用顺序）—— provider 协议要求；
    8. 没有结果的调用（比如崩溃打断、fork 截断）合成一个错误结果；没有调用的结果丢掉；
    9. 没有 model 的条目、stop_reason 为 error / aborted 的 assistant 消息不发给模型；
   10. 第一条 system 消息挪到最前面（前面只有 user 消息时），保持 provider 的 prompt cache 稳定。
（第 4–6 步的 context edit 在本迷你版里省略。）
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


def active_range(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    marker: Optional[Dict[str, Any]] = None
    for entry in reversed(entries):
        if entry.get("head") is not None:
            marker = entry
            break
    if marker is None:
        return list(entries)
    start = marker["head"]
    return [marker] + [e for e in entries if e["id"] >= start and e.get("head") is None]


def model_messages(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    messages: List[Dict[str, Any]] = []
    for entry in entries:
        for message in entry.get("model") or []:
            if message["role"] == "assistant" and message.get("stop_reason") in ("error", "aborted"):
                continue
            messages.append(message)
    results = {m["call_id"]: m for m in messages if m["role"] == "tool_result"}
    ordered: List[Dict[str, Any]] = []
    for message in messages:
        if message["role"] == "tool_result":
            continue
        ordered.append(message)
        for call in message.get("tool_calls") or []:
            ordered.append(results.pop(call["id"], None) or {
                "role": "tool_result", "call_id": call["id"], "name": call["name"],
                "content": "No result: the call was interrupted.", "is_error": True,
            })
    first_system = next((i for i, m in enumerate(ordered) if m["role"] == "system"), None)
    if first_system and all(m["role"] == "user" for m in ordered[:first_system]):
        ordered.insert(0, ordered.pop(first_system))
    return ordered


def effective_sections(entries: List[Dict[str, Any]]) -> Dict[str, str]:
    """按顺序重放 system 消息里的 section 补丁，得到当前生效的系统提示。"""
    sections: Dict[str, str] = {}
    for entry in entries:
        for message in entry.get("model") or []:
            if message["role"] == "system":
                for name, text in message.get("sections", {}).items():
                    if text is None:
                        sections.pop(name, None)
                    else:
                        sections[name] = text
    return sections


def section_patch(current: Dict[str, str], wanted: Dict[str, str]) -> Dict[str, Optional[str]]:
    """只发「变了的部分」：新增 / 修改给字符串，删除给 None。这样 provider 的 prompt cache 保持温热。"""
    patch: Dict[str, Optional[str]] = {name: text for name, text in wanted.items() if current.get(name) != text}
    for name in current:
        if name not in wanted:
            patch[name] = None
    return patch
