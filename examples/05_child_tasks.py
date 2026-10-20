"""示例 5：子任务与结构化并发（对应 TS test/examples/24-child-tasks.ts、spec §5.5）。

Checkout（结账）任务创建 4 个 Payment（支付）子任务，并 waiting(on=payments, policy="failFast")。
每个 Payment：authorize 阶段持久化睡一会儿（模拟等待银行）→ capture 阶段完成；abort 处理器负责「退款」。

三个场景：
    A. 一张卡过期 → failFast：其余还活着的支付被打上中止标记 → 各自先退款（abort 处理器）
       → 全部终结后 Checkout 才恢复，读到 [aborted, failed, aborted, aborted]，决定自己失败。
    B. 直接 abort Checkout → 中止标记沿所有权树「向下流」→ 支付的 abort 处理器先跑，
       Checkout 自己的 abort 处理器最后跑，并且能看到孩子们的最终结局（中止是自下而上的）。
    C. 支付睡眠期间进程崩溃 → 换一个 Harness 打开同一个存储 → 睡眠的截止时间还在，到点继续，最终成功。
"""

import asyncio

import _common  # noqa: F401

from pi_durable import (Harness, MemoryStorage, Models, Registry, aborted, completed, define_extension, define_task,
                        failed, running, waiting)

log = []


def note(text):
    log.append(text)
    print("   ", text)


# ---------------------------------------------------------------- Payment
async def authorize(task, rt):
    card = task["input"]["card"]
    if card.startswith("expired"):
        note(f"payment {card}: declined")
        return await rt.commit(lambda tx, current: failed(f"card {card} expired"))
    note(f"payment {card}: authorized, waiting for bank")
    await rt.sleep_until(rt.now() + task["input"]["delay"], {"phase": "capture"})


async def capture(task, rt):
    note(f"payment {task['input']['card']}: captured")
    await rt.commit(lambda tx, current: completed({"card": task["input"]["card"]}))


async def refund(task, rt):
    note(f"payment {task['input']['card']}: abort handler -> refund")
    await rt.commit(lambda tx, current: aborted("refunded"))


Payment = define_task("shop.payment", 1, initial=lambda input: {"phase": "authorize"},
                      phases={"authorize": authorize, "capture": capture}, abort=refund)


# ---------------------------------------------------------------- Checkout
async def pay(task, rt):
    async def change(tx, current):
        payments = [await tx.create_task(Payment, {"card": card, "delay": 300}, owner=task["id"])
                    for card in task["input"]["cards"]]
        note(f"checkout: created payments {payments}, waiting failFast")
        # 等待期间不占用任何调用；全部终结后在 decide 阶段恢复
        return waiting({"phase": "decide", "payments": payments}, payments, "failFast")
    await rt.commit(change)


async def decide(task, rt):
    outcomes = await rt.outcomes(task["state"]["checkpoint"]["payments"])
    statuses = [o["status"] for o in outcomes]
    note(f"checkout: resumed, payment outcomes = {statuses}")
    if all(s == "completed" for s in statuses):
        return await rt.commit(lambda tx, current: completed({"paid": len(statuses)}))
    await rt.commit(lambda tx, current: failed("a payment failed", detail=statuses))


async def cancel_checkout(task, rt):
    payments = task["state"]["checkpoint"].get("payments", [])
    outcomes = await rt.outcomes(payments)  # 孩子们此时一定已经终结
    note(f"checkout: abort handler runs last, sees {[o['status'] for o in outcomes]}")
    await rt.commit(lambda tx, current: aborted("cancelled by user"))


Checkout = define_task("shop.checkout", 1, initial=lambda input: {"phase": "pay"},
                       phases={"pay": pay, "decide": decide}, abort=cancel_checkout)


def build_registry():
    registry = Registry()
    registry.install(define_extension("shop", tasks=[Payment, Checkout]))
    return registry


async def start_checkout(harness, cards):
    root = await harness.root()
    return await root.commit(lambda tx: tx.create_task(Checkout, {"cards": cards}))


async def main():
    print("== A. failFast：一张卡过期 ==")
    harness = await Harness.open(MemoryStorage(), models=Models(), registry=build_registry())
    checkout = await start_checkout(harness, ["visa-1", "expired-2", "visa-3", "visa-4"])
    result = await harness.wait_for_task(checkout)
    print("  checkout outcome:", result["state"]["outcome"])
    assert result["state"]["outcome"]["status"] == "failed"
    assert log.index("checkout: resumed, payment outcomes = ['aborted', 'failed', 'aborted', 'aborted']") > \
        max(i for i, line in enumerate(log) if "refund" in line)
    await harness.close()

    print("== B. 直接中止 Checkout：中止向下流动，自下而上执行 ==")
    log.clear()
    harness = await Harness.open(MemoryStorage(), models=Models(), registry=build_registry())
    checkout = await start_checkout(harness, ["visa-1", "visa-2", "visa-3"])
    harness.resume()
    await asyncio.sleep(0.1)  # 让支付进入睡眠
    result = await harness.abort_task(checkout)
    print("  checkout outcome:", result["state"]["outcome"])
    assert "abort handler runs last" in log[-1]
    await harness.close()

    print("== C. 支付睡眠期间进程崩溃，换个 Harness 继续 ==")
    log.clear()
    storage = MemoryStorage()  # 同一个存储对象 = 同一份「磁盘」
    first = await Harness.open(storage, models=Models(), registry=build_registry())
    checkout = await start_checkout(first, ["visa-1", "visa-2"])
    first.resume()
    await asyncio.sleep(0.1)
    first.simulate_crash()
    print("  -- crashed while payments sleep --")
    second = await Harness.open(storage, models=Models(), registry=build_registry())
    for item in await second.inspect():
        task = item["task"]
        print(f"  live: task {task['id']} {task['kind']:<14} {task['state']['status']:<9} restarts={task['restarts']}")
    result = await second.wait_for_task(checkout)
    print("  checkout outcome:", result["state"]["outcome"])
    assert result["state"]["outcome"] == {"status": "completed", "result": {"paid": 2}}
    await second.close()


asyncio.run(main())
