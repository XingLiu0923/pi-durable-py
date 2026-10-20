"""模型接口与假模型。TS 版用 @earendil-works/pi-ai（统一多 provider 的 LLM API）；
示例里大多用 pi-ai 的 faux provider。这里只定义最小的「流式接口」：

    async for event in model.stream({"messages": [...], "tools": [...]}):
        {"type": "text_delta", "delta": "..."}     # 流式文本
        {"type": "done", "message": AssistantMessage}

AssistantMessage（简化版）：
    {"role": "assistant", "content": str, "tool_calls": [{"id", "name", "arguments"}],
     "stop_reason": "stop" | "tool_use" | "error", "error"?: str, "retryable"?: bool, "usage": {...}}

要接真模型，只要写一个有同样 `name` 和 `stream()` 的类，把消息格式转换成对应 provider 的格式即可。
"""

from __future__ import annotations

import asyncio
import inspect
import uuid
from typing import Any, AsyncIterator, Callable, Dict, List, Optional


def reply(text: str) -> Dict[str, Any]:
    return {"role": "assistant", "content": text, "tool_calls": [], "stop_reason": "stop"}


def call(name: str, **arguments: Any) -> Dict[str, Any]:
    return {"id": f"call_{uuid.uuid4().hex[:8]}", "name": name, "arguments": arguments}


def tool_calls(*calls: Dict[str, Any], text: str = "") -> Dict[str, Any]:
    return {"role": "assistant", "content": text, "tool_calls": list(calls), "stop_reason": "tool_use"}


def error(message: str, retryable: bool = True) -> Dict[str, Any]:
    return {"role": "assistant", "content": "", "tool_calls": [], "stop_reason": "error",
            "error": message, "retryable": retryable}


class FakeModel:
    """假模型：用一个「应答函数」根据上下文决定回复。

    为什么是函数而不是「按顺序弹出的脚本」？崩溃重启后，新进程会拿**同样的上下文**重新请求 ——
    真实模型本来就是无状态的。用上下文决定回复，恢复后的行为才和真实情况一致。
    """

    def __init__(self, name: str, respond: Callable[[List[Dict[str, Any]], List[Dict[str, Any]]], Any],
                 chunk_delay: float = 0.0, chunk_size: int = 6) -> None:
        self.name = name
        self._respond = respond
        self._chunk_delay = chunk_delay
        self._chunk_size = chunk_size
        self.requests = 0

    async def stream(self, request: Dict[str, Any]) -> AsyncIterator[Dict[str, Any]]:
        self.requests += 1
        message = self._respond(request["messages"], request["tools"])
        if inspect.isawaitable(message):
            message = await message
        message = dict(message)
        text = message.get("content") or ""
        if text and self._chunk_delay > 0:
            for i in range(0, len(text), self._chunk_size):
                await asyncio.sleep(self._chunk_delay)
                yield {"type": "text_delta", "delta": text[i:i + self._chunk_size]}
        prompt = sum(len(str(m.get("content", ""))) for m in request["messages"])
        message.setdefault("usage", {"input": prompt // 4 + 1, "output": len(text) // 4 + 1})
        yield {"type": "done", "message": message}


class Models:
    def __init__(self, *models: Any) -> None:
        self._models: Dict[str, Any] = {m.name: m for m in models}

    def add(self, model: Any) -> None:
        self._models[model.name] = model

    def get(self, name: Optional[str]) -> Optional[Any]:
        return self._models.get(name) if name else None


def last_user_text(messages: List[Dict[str, Any]]) -> str:
    for message in reversed(messages):
        if message["role"] == "user":
            return message["content"]
    return ""


def tool_results_since_last_user(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    for message in reversed(messages):
        if message["role"] == "user":
            break
        if message["role"] == "tool_result":
            results.insert(0, message)
    return results
