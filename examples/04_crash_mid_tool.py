"""示例 4：工具执行到一半，进程真的死了（os._exit），换一个新进程恢复。

对应 README「Tools」里这句话：
    「每个调用都是自己的持久化任务。execute() 之前先提交意图。进程在调用中途死掉的话，
     只有声明了 replay="safe" 的工具会在重开时重跑；否则模型得到一个 interrupted 错误结果，
     带着到那时为止已提交的输出。」

模型一次调用两个工具：
    fetch_page   replay="safe"    —— 只读，重跑无害
    charge_card  replay="unsafe"  —— 有外部副作用，不能盲目重跑

子进程：两个工具都提交了意图、各输出了一点东西之后，os._exit(1) —— 没有 close，什么都不收尾。
父进程：打开同一个 SQLite 文件 → 恢复把 running 的任务改回 pending 且 restarts=1 → 继续 run。
"""

import asyncio
import os
import subprocess
import sys
import tempfile

import _common  # noqa: F401
from _common import print_transcript

from pi_durable import FakeModel, Harness, Models, Registry, SqliteStorage, define_extension, define_tool
from pi_durable.models import call, reply, tool_calls, tool_results_since_last_user

started = set()


async def fetch_page(args, api):
    await api.output(f"GET {args['url']} ... ")
    await api.flush()
    started.add("fetch_page")
    await asyncio.sleep(0.2)
    return f"<html>{args['url']} ok</html>"


async def charge_card(args, api):
    await api.output(f"charging {args['amount']} ... ")
    await api.flush()
    started.add("charge_card")
    await asyncio.sleep(0.2)
    return f"charged {args['amount']}"


def respond(messages, tools):
    results = tool_results_since_last_user(messages)
    if not results:
        return tool_calls(call("fetch_page", url="https://example.com"), call("charge_card", amount=42))
    return reply(" | ".join(f"{r['name']}: {r['content'][:70]}" for r in results))


def build_registry():
    registry = Registry()
    registry.install(define_extension("shop", tools=[
        define_tool("fetch_page", "Fetch a web page", {"url": str}, fetch_page, replay="safe"),
        define_tool("charge_card", "Charge the card", {"amount": int}, charge_card, replay="unsafe"),
    ]))
    return registry


async def open_harness(path):
    return await Harness.open(await SqliteStorage.open(path), models=Models(FakeModel("fake", respond)),
                              registry=build_registry())


async def child(path):
    harness = await open_harness(path)
    root = await harness.root(agent={"model": "fake"})
    await root.submit({"type": "input", "content": "Buy it", "request_id": "buy-1"})
    while started != {"fetch_page", "charge_card"}:
        await asyncio.sleep(0.01)
    print("[child] both tools committed intent and are running -> os._exit(1)", flush=True)
    os._exit(1)  # 进程「暴毙」：不 close，不收尾


async def parent(path):
    harness = await open_harness(path)  # open() 里的恢复已经跑过了
    print("--- reopened; live tasks（running → pending，restarts+1）---")
    for item in await harness.inspect():
        task = item["task"]
        print(f"  task {task['id']} {task['kind']:<14} {task['state']['status']:<8} "
              f"checkpoint={task['state'].get('checkpoint')} restarts={task.get('restarts')}")
    root = await harness.root()
    # 重启后，同一个 request_id 找回同一个 submission，不会重复提交
    submission = await root.submit({"type": "input", "content": "Buy it", "request_id": "buy-1"})
    settled = await submission.wait()
    print("submission:", settled["status"])
    await print_transcript(root)
    await harness.close()


async def main():
    if len(sys.argv) > 2 and sys.argv[1] == "child":
        return await child(sys.argv[2])
    path = os.path.join(tempfile.mkdtemp(prefix="pi-durable-py-"), "session.sqlite")
    code = subprocess.call([sys.executable, os.path.abspath(__file__), "child", path])
    print(f"[parent] child exited with code {code}")
    assert code == 1
    await parent(path)


asyncio.run(main())
