"""示例 1：一问一答（对应 TS README 的 Quick Start 和 test/examples/14-chat.ts）。

看点：
    - submit() 持久化地接收输入，返回 Submission；wait() 等它 done / unanswered；
    - 一次回答背后是一个 pi.generation 任务（prepare → request → completed）；
    - watch() 看到的每一帧都是「已提交」的状态：流式 partial 也是先落盘、再可见。
"""

import asyncio

import _common  # noqa: F401
from _common import print_tasks, print_transcript

from pi_durable import FakeModel, Harness, MemoryStorage, Models, Registry, define_extension, section
from pi_durable.models import last_user_text, reply


def respond(messages, tools):
    question = last_user_text(messages)
    if "France" in question:
        return reply("The capital of France is Paris.")
    return reply(f"You said: {question}")


async def main():
    models = Models(FakeModel("fake", respond, chunk_delay=0.03))
    registry = Registry()
    registry.install(define_extension("persona", sections=[section("preamble", lambda _: "You are concise.")]))

    harness = await Harness.open(MemoryStorage(), models=models, registry=registry,
                                 settings={"progress": {"partial_interval_ms": 0}})
    root = await harness.root(agent={"model": "fake"})

    frames = []
    stop = root.watch(lambda frame: frames.append(frame))

    submission = await root.submit({"type": "input", "content": "What is the capital of France?"})
    settled = await submission.wait()
    stop()
    print("submission:", settled)
    assert settled["status"] == "done"

    answer = next(e for e in await root.entries() if e["id"] == settled["answer"])
    print("answer:", answer["model"][0]["content"])

    print("--- 流式 partial（每一帧都来自一次提交）---")
    for frame in frames:
        live = frame["docs"].get("pi.live")
        if live and live.get("generation") and live["generation"].get("partial"):
            print(f"  seq {frame['seq']:<3} partial = {live['generation']['partial']!r}")

    await print_transcript(root)
    generation = next(e["by_task_id"] for e in await root.entries() if e["kind"] == "pi.assistant")
    print("--- tasks ---")
    await print_tasks(harness, [generation])

    # 幂等：同一个 request_id 重复提交 → 同一个 submission，不会问两次
    first = await root.submit({"type": "input", "content": "Hello", "request_id": "greeting-1"})
    again = await root.submit({"type": "input", "content": "Hello", "request_id": "greeting-1"})
    assert first.id == again.id
    await first.wait()
    print(f"request_id dedupe: {first.id} == {again.id}; model requests = {models.get('fake').requests}")
    await harness.close()


asyncio.run(main())
