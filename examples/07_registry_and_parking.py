"""示例 7：代码不存储 —— 缺定义的任务被阻塞；崩溃循环被「停放」；热修复后重试。

对应 README「Extensions」「Reload」「Failures and Recovery」，spec §5.4。

A. 存储里只有任务的「名字」(kind)。重开时如果没装对应扩展，任务保持 pending、标记为 blocked: missing_task，
   既不会失败也不会被丢弃；一旦 install 了扩展，调度器自动接着跑。
B. 一个 phase 每次都在存下 checkpoint 前把进程弄死（比如 OOM）。每次重开都会再跑、再死……
   restarts 计数到 restart_limit 后，任务被停放（blocked: restart_limit），不再自动运行，并报告 task_parked。
   修好代码（同名扩展原地替换）后，harness.retry_task(id) 让它再跑一次。
"""

import asyncio

import _common  # noqa: F401

from pi_durable import Harness, MemoryStorage, Models, Registry, aborted, completed, define_extension, define_task

entered = None


async def write_report(task, rt):
    await rt.commit(lambda tx, current: completed("report written"))


Report = define_task("app.report", 1, initial=lambda input: {"phase": "write"}, phases={"write": write_report},
                     abort=lambda task, rt: rt.commit(lambda tx, current: aborted()))


async def hog(task, rt):
    entered.set()
    await asyncio.sleep(10)  # 模拟：在下一个 checkpoint 之前耗尽内存，进程被杀


async def fixed(task, rt):
    await rt.commit(lambda tx, current: completed("fixed and done"))


def hog_task(phase):
    return define_task("app.hog", 1, initial=lambda input: {"phase": "work"}, phases={"work": phase},
                       abort=lambda task, rt: rt.commit(lambda tx, current: aborted()))


async def main():
    global entered
    print("== A. 缺定义 → blocked，安装扩展后自动继续 ==")
    storage = MemoryStorage()
    with_ext = Registry()
    with_ext.install(define_extension("reports", tasks=[Report]))
    first = await Harness.open(storage, models=Models(), registry=with_ext)
    report = await (await first.root()).commit(lambda tx: tx.create_task(Report))
    await first.close()  # 还没 resume，任务没跑过

    registry = Registry()  # 新进程，忘了装 reports 扩展
    second = await Harness.open(storage, models=Models(), registry=registry)
    second.resume()
    await asyncio.sleep(0.05)
    print("  inspect:", [(i["task"]["kind"], i["task"]["state"]["status"], i["blocked"]) for i in await second.inspect()])
    print("  wake:", second.wake())
    registry.install(define_extension("reports", tasks=[Report]))  # 装上 → 自动继续
    print("  after install:", (await second.wait_for_task(report))["state"]["outcome"])
    await second.close()

    print("== B. 崩溃循环 → parked → 热修复 → retry ==")
    storage = MemoryStorage()
    registry = Registry()
    registry.install(define_extension("hog", tasks=[hog_task(hog)]))
    settings = {"restart_limit": 3}
    reports = []
    harness = await Harness.open(storage, models=Models(), registry=registry, settings=settings)
    task_id = await (await harness.root()).commit(lambda tx: tx.create_task(hog_task(hog)))
    for attempt in range(1, 4):
        entered = asyncio.Event()
        harness.resume()
        await entered.wait()
        harness.simulate_crash()
        print(f"  crash #{attempt}")
        harness = await Harness.open(storage, models=Models(), registry=registry, settings=settings,
                                     on_report=lambda error, info: reports.append(info["type"]))
    harness.resume()
    await asyncio.sleep(0.05)
    item = (await harness.inspect())[0]
    print(f"  parked: status={item['task']['state']['status']} restarts={item['task']['restarts']} "
          f"blocked={item['blocked']} reports={reports} wake={harness.wake()}")
    assert item["blocked"] == "restart_limit"

    registry.install(define_extension("hog", tasks=[hog_task(fixed)]))  # 同名扩展：原地替换 = 热修复
    await harness.retry_task(task_id)
    print("  after fix + retry:", (await harness.wait_for_task(task_id))["state"]["outcome"])
    await harness.close()


asyncio.run(main())
