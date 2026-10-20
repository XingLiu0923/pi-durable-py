"""示例 2：一次带工具调用的 run（对应 TS test/examples/17-coding-tools.ts、README「Tools」「Hooks」）。

看点：
    - 模型一次返回两个工具调用 → 两个 pi.tool 任务（被 generation 拥有，并行执行）；
    - generation 提交 waiting(on=[两个工具任务]) 后不占用任何调用，工具都结束了才恢复；
    - 工具轮结束后，generation 把 run 「交接」给一个新的 generation，由它拿着工具结果再问模型；
    - before_tool 钩子可以拦截调用（这里拦住了危险的 delete_files）；
    - api.output() 的流式输出被节流提交到 pi.live.tools[call_id].output。
"""

import asyncio

import _common  # noqa: F401
from _common import print_tasks, print_transcript

from pi_durable import FakeModel, Harness, MemoryStorage, Models, Registry, define_extension, define_tool, hook
from pi_durable.models import call, last_user_text, reply, tool_calls, tool_results_since_last_user


async def get_weather(args, api):
    for step in ("connecting... ", "fetching... "):
        await api.output(step)
        await asyncio.sleep(0.05)
    return f"Sunny, 21°C in {args['city']}"


async def delete_files(args, api):
    raise AssertionError("never runs: the guard blocks it")


def respond(messages, tools):
    results = tool_results_since_last_user(messages)
    if not results:  # 第一次：调用两个工具
        return tool_calls(call("get_weather", city="Paris"), call("delete_files", path="/"),
                          text="Let me check.")
    summary = "; ".join(f"{r['name']}: {r['content']}" for r in results)
    return reply(f"Done. ({summary})")


async def main():
    weather = define_tool("get_weather", "Weather for a city", {"city": str}, get_weather, replay="safe")
    danger = define_tool("delete_files", "Delete files", {"path": str}, delete_files)
    guard = hook("tool", before_tool=lambda c: {"block": "needs approval"} if c["name"] == "delete_files" else None)

    registry = Registry()
    registry.install(define_extension("tools", tools=[weather, danger]))
    registry.install(define_extension("guard", hooks=[guard]))

    harness = await Harness.open(MemoryStorage(), models=Models(FakeModel("fake", respond)), registry=registry,
                                 settings={"progress": {"output_interval_ms": 0}})
    root = await harness.root(agent={"model": "fake"})

    outputs = []
    root.watch(lambda f: [outputs.append((slot["name"], slot["status"], slot["output"]))
                          for slot in (f["docs"].get("pi.live") or {}).get("tools", {}).values()])

    settled = await (await root.submit({"type": "input", "content": "Weather in Paris? Then clean up /"})).wait()
    print("submission:", settled["status"])
    assert settled["status"] == "done"

    await print_transcript(root)
    print("--- pi.live.tools 槽位的变化（每行来自一次提交）---")
    seen = set()
    for row in outputs:
        if row not in seen:
            seen.add(row)
            print("  ", row)

    entries = await root.entries()
    task_ids = sorted({e["by_task_id"] for e in entries if e.get("by_task_id")})
    print("--- tasks（注意 owner：工具任务属于第一个 generation）---")
    await print_tasks(harness, task_ids)
    for id in task_ids:
        record = await harness.get_task(id)
        print(f"  task {id} owner={record.get('owner')}")
    usage = (await root.view())["docs"]["pi.usage"]
    print("usage:", usage)
    await harness.close()


asyncio.run(main())
