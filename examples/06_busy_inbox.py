"""示例 6：会话忙的时候再提交（对应 TS test/examples/20-inbox.ts、README「Busy Conversations」、spec §6）。

run 进行中（正在跑 build 工具），陆续提交：
    steer    「Use pnpm, not npm」  → 排队；在当前工具轮结束（postTools 边界）时放进 transcript，加入当前 run
    followUp「Also run the tests」  → 排队；当前 run 回答之后（final 边界）放置，开启下一轮 run
    followUp「Deploy too」          → 排队后被 submission.abort() 撤回 → unanswered(aborted)
    write   （纯展示的聊天消息）     → 不排队，立刻追加（即使在 run 中间）
    when_busy="reject"             → 直接抛 ConversationBusy，什么都不写
"""

import asyncio

import _common  # noqa: F401
from _common import print_transcript

from pi_durable import ConversationBusy, FakeModel, Harness, MemoryStorage, Models, Registry, define_extension, define_tool
from pi_durable.models import call, reply, tool_calls


async def build(args, api):
    await asyncio.sleep(0.3)
    return "build ok"


def respond(messages, tools):
    last = messages[-1]
    if last["role"] == "user" and last["content"].startswith("Run the build"):
        return tool_calls(call("build", tool="npm"))
    if last["role"] == "user" and "pnpm" in last["content"]:
        return reply("Build finished. Noted: pnpm from now on.")
    if last["role"] == "user" and "tests" in last["content"]:
        return reply("Tests passed.")
    return reply("OK.")


async def main():
    registry = Registry()
    registry.install(define_extension("ci", tools=[define_tool("build", "Run the build", {"tool": str}, build)]))
    harness = await Harness.open(MemoryStorage(), models=Models(FakeModel("fake", respond)), registry=registry)
    root = await harness.root(agent={"model": "fake"})

    first = await root.submit({"type": "input", "content": "Run the build"})
    await asyncio.sleep(0.1)  # build 工具正在跑
    assert await root.is_busy()

    steer = await root.submit({"type": "input", "content": "Use pnpm, not npm", "when_busy": "steer"})
    follow = await root.submit({"type": "input", "content": "Also run the tests"})
    withdrawn = await root.submit({"type": "input", "content": "Deploy too"})
    chat = await root.submit({"type": "write", "entry": {"kind": "chat.note", "data": "ann: I'm watching this"}})
    try:
        await root.submit({"type": "input", "content": "Only if idle", "when_busy": "reject"})
    except ConversationBusy as error:
        print("reject ->", type(error).__name__)

    inbox = (await root.view())["docs"]["pi.inbox"]["items"]
    print("inbox while busy:", [(i["mode"], i.get("content")) for i in inbox])
    print("withdraw 'Deploy too' ->", await withdrawn.abort())

    for name, submission in [("first", first), ("steer", steer), ("follow", follow), ("withdrawn", withdrawn),
                             ("chat", chat)]:
        record = await submission.wait()
        print(f"  {name:<9} {record['status']:<10} {record.get('reason', '')}")
    await root.wait_for_idle()
    await print_transcript(root)

    statuses = [(await s.record())["status"] for s in (first, steer, follow, withdrawn, chat)]
    assert statuses == ["done", "done", "done", "unanswered", "done"]
    await harness.close()


asyncio.run(main())
