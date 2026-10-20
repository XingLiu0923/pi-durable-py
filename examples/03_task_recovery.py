"""示例 3：自定义持久化任务 —— memo、持久化睡眠、关闭后重开继续（逐行对应 TS test/examples/13-recovery.ts）。

Ticker 从 1 数到 5，每数一次「打印」（外部副作用），然后睡 50ms 再数下一个。
第一个进程在打印完 tick 2、还没来得及存下一个 checkpoint 时关闭 —— 等价于「副作用做了、结果没存」的崩溃窗口。
第二个进程打开同一个 SQLite 文件，什么都不用手动恢复，等这个任务就会接着跑。
tick 2 的 phase 会重跑（因为它的结果没存），但 memo 记得「已经打印过」，所以不会重复打印。
"""

import asyncio
import os
import tempfile

import _common  # noqa: F401

from pi_durable import Harness, Models, Registry, SqliteStorage, completed, define_extension, define_task, aborted

reached = None  # asyncio.Event，在 main() 里创建（Python 3.9 的 Event 绑定创建时的事件循环）


async def tick(task, rt):
    n = task["state"]["checkpoint"]["n"]
    # 副作用之前先存 memo（先写者胜）：重跑时看到 memo，就知道这个 tick 已经打印过了
    if await rt.memo(f"printed-{n}") is None:
        await rt.memo(f"printed-{n}", True)
        print(f"tick {n}")
    if n == 2:
        reached.set()
        await asyncio.sleep(0.2)  # 第一个进程会在这里被关闭
    if n == task["input"]["to"]:
        return await rt.commit(lambda tx, current: completed(f"counted to {n}"))
    # 持久化睡眠：睡眠本身存进任务记录，不占用调用；之后打开的 Harness 到点了会继续
    await rt.sleep_until(rt.now() + 50, {"phase": "tick", "n": n + 1})


Ticker = define_task(
    name="example.ticker",
    version=1,
    initial=lambda input: {"phase": "tick", "n": 1},
    phases={"tick": tick},
    abort=lambda task, rt: rt.commit(lambda tx, current: aborted()),
)


async def main():
    global reached
    reached = asyncio.Event()
    path = os.path.join(tempfile.mkdtemp(prefix="pi-durable-py-"), "session.sqlite")
    registry = Registry()
    registry.install(define_extension("ticker", tasks=[Ticker]))

    async def open_harness():
        return await Harness.open(await SqliteStorage.open(path), models=Models(), registry=registry)

    # 第一次运行：开始数到 5；tick 2 打印之后、下一个 checkpoint 存下之前关闭
    first = await open_harness()
    root = await first.root()
    ticker = await root.commit(lambda tx: tx.create_task(Ticker, {"to": 5}))
    first.resume()
    await reached.wait()
    await first.close()

    # 用一个从不 resume 的 Harness 读存下来的记录：什么都不会跑
    reader = await open_harness()
    saved = await reader.get_task(ticker)
    await reader.close()
    print("closed; saved state:", saved["state"], "memos:", saved["memos"], "restarts:", saved["restarts"])

    # 第二次运行：不用手动恢复，等待这个任务就会开启调度并继续
    second = await open_harness()
    done = await second.wait_for_task(ticker)
    print("after reopen:", done["state"]["outcome"])
    assert done["state"]["outcome"] == {"status": "completed", "result": "counted to 5"}
    await second.close()


asyncio.run(main())
