"""核心规则的小测试。不依赖 pytest：python3 tests/test_core.py"""

import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from pi_durable import (FakeModel, Harness, MemoryStorage, Models, ReadAfterWrite, Registry, aborted, completed,
                        define_extension, define_task, define_tool, running)
from pi_durable.models import call, error, last_user_text, reply, tool_calls, tool_results_since_last_user

TESTS = []


def test(fn):
    TESTS.append(fn)
    return fn


def quiet(error, info):
    pass


async def open_harness(registry=None, respond=None, settings=None, storage=None):
    models = Models(FakeModel("fake", respond or (lambda m, t: reply("ok"))))
    return await Harness.open(storage or MemoryStorage(), models=models, registry=registry or Registry(),
                              settings=settings, on_report=quiet)


def simple_task(name, phase):
    return define_task(name, 1, initial=lambda input: {"phase": "go"}, phases={"go": phase},
                       abort=lambda task, rt: rt.commit(lambda tx, current: aborted()))


@test
async def phase_exception_faults_the_task():
    async def boom(task, rt):
        raise ValueError("bug")
    Boom = simple_task("t.boom", boom)
    registry = Registry()
    registry.install(define_extension("x", tasks=[Boom]))
    harness = await open_harness(registry)
    id = await (await harness.root()).commit(lambda tx: tx.create_task(Boom))
    outcome = (await harness.wait_for_task(id))["state"]["outcome"]
    assert outcome["status"] == "faulted" and "bug" in outcome["error"]["message"], outcome
    await harness.close()


@test
async def phase_without_durable_progress_faults():
    async def lazy(task, rt):
        pass  # 什么都没提交
    Lazy = simple_task("t.lazy", lazy)
    registry = Registry()
    registry.install(define_extension("x", tasks=[Lazy]))
    harness = await open_harness(registry)
    id = await (await harness.root()).commit(lambda tx: tx.create_task(Lazy))
    outcome = (await harness.wait_for_task(id))["state"]["outcome"]
    assert outcome["status"] == "faulted" and "without durable progress" in outcome["error"]["message"], outcome
    await harness.close()


@test
async def multi_phase_checkpoints():
    async def one(task, rt):
        await rt.commit(lambda tx, current: running({"phase": "two", "value": 41}))

    async def two(task, rt):
        value = task["state"]["checkpoint"]["value"] + 1
        await rt.commit(lambda tx, current: completed(value))
    Steps = define_task("t.steps", 1, initial=lambda input: {"phase": "one"}, phases={"one": one, "two": two},
                        abort=lambda task, rt: rt.commit(lambda tx, current: aborted()))
    registry = Registry()
    registry.install(define_extension("x", tasks=[Steps]))
    harness = await open_harness(registry)
    id = await (await harness.root()).commit(lambda tx: tx.create_task(Steps))
    assert (await harness.wait_for_task(id))["state"]["outcome"] == {"status": "completed", "result": 42}
    await harness.close()


@test
async def read_after_write_is_rejected_and_rolls_back():
    harness = await open_harness()
    root = await harness.root()

    async def change(tx):
        await tx.append_entry(root.id, {"kind": "app.note", "data": 1})
        await tx.entries(root.id)  # 表写之后再读表
    try:
        await root.commit(change)
        raise AssertionError("expected ReadAfterWrite")
    except ReadAfterWrite:
        pass
    assert await root.entries() == []  # 回调抛错 → 整个提交回滚
    await harness.close()


@test
async def retryable_errors_back_off_then_succeed():
    attempts = []

    def respond(messages, tools):
        attempts.append(1)
        return error("overloaded") if len(attempts) < 3 else reply("finally")
    harness = await open_harness(respond=respond, settings={"retry": {"max_retries": 3, "base_delay_ms": 10}})
    root = await harness.root(agent={"model": "fake"})
    settled = await (await root.submit({"type": "input", "content": "hi"})).wait()
    assert settled["status"] == "done" and len(attempts) == 3
    context = await root.context()
    assert [m["role"] for m in context] == ["user", "assistant"], context  # 错误消息不进上下文
    await harness.close()


@test
async def non_retryable_error_settles_unanswered():
    harness = await open_harness(respond=lambda m, t: error("bad request", retryable=False))
    root = await harness.root(agent={"model": "fake"})
    settled = await (await root.submit({"type": "input", "content": "hi"})).wait()
    assert settled["status"] == "unanswered" and settled["reason"] == "model_error", settled
    assert not await root.is_busy()
    await harness.close()


@test
async def unknown_model_settles_no_model():
    harness = await open_harness()
    root = await harness.root(agent={"model": "missing"})
    settled = await (await root.submit({"type": "input", "content": "hi"})).wait()
    assert settled["reason"] == "no_model", settled
    await harness.close()


@test
async def reset_starts_a_new_context():
    harness = await open_harness(respond=lambda m, t: reply(f"seen {len(m)} messages"))
    root = await harness.root(agent={"model": "fake"})
    await (await root.submit({"type": "input", "content": "one"})).wait()
    await (await root.reset("Handoff: we said one.")).wait()
    await (await root.submit({"type": "input", "content": "two"})).wait()
    context = await root.context()
    assert [m.get("content") for m in context if m["role"] == "user"] == ["Handoff: we said one.", "two"], context
    assert len(await root.entries()) == 5  # 旧条目还在存储里
    await harness.close()


@test
async def conversation_abort_stops_the_run_and_its_tools():
    async def slow(args, api):
        await api.output("working")
        await api.flush()
        await asyncio.sleep(5)

    def respond(messages, tools):
        if tool_results_since_last_user(messages):
            return reply("unreachable")
        return tool_calls(call("slow"))
    registry = Registry()
    registry.install(define_extension("x", tools=[define_tool("slow", "slow", {}, slow)]))
    harness = await open_harness(registry, respond)
    root = await harness.root(agent={"model": "fake"})
    submission = await root.submit({"type": "input", "content": "go"})
    queued = await root.submit({"type": "input", "content": "later"})
    await asyncio.sleep(0.1)
    await root.abort()
    first, second = await submission.wait(), await queued.wait()
    assert (first["status"], first["reason"]) == ("unanswered", "aborted"), first
    assert (second["status"], second["reason"]) == ("unanswered", "aborted"), second
    result = [e for e in await root.entries() if e["kind"] == "pi.tool-result"][0]["model"][0]
    assert result["is_error"] and "working" in result["content"], result
    assert not await root.is_busy() and harness.scheduler.is_idle(root.id)
    await harness.close()


@test
async def follow_up_after_answer_starts_next_run():
    def respond(messages, tools):
        return reply(f"answer to {last_user_text(messages)}")
    harness = await open_harness(respond=respond)
    root = await harness.root(agent={"model": "fake"})
    a = await root.submit({"type": "input", "content": "a"})
    b = await root.submit({"type": "input", "content": "b"})
    ra, rb = await a.wait(), await b.wait()
    assert ra["status"] == rb["status"] == "done" and ra["answer"] < rb["answer"]
    await harness.close()


async def main():
    failures = 0
    for fn in TESTS:
        try:
            await asyncio.wait_for(fn(), timeout=10)
            print(f"ok    {fn.__name__}")
        except Exception as error:
            failures += 1
            print(f"FAIL  {fn.__name__}: {type(error).__name__}: {error}")
    print(f"{len(TESTS) - failures}/{len(TESTS)} passed")
    sys.exit(1 if failures else 0)


asyncio.run(main())
