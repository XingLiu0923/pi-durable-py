# pi-durable 学习笔记 + Python 教学版

> 上游：[`earendil-works/pi`](https://github.com/earendil-works/pi) 的 `packages/durable`（npm 包 `@earendil-works/pi-durable`，本文基于 1.3.0，2026-10-19 的 main）。
> 官方标注 **Experimental**：API 会在版本间无预告变化。
>
> 本目录用约 3100 行零依赖 Python（3.9+，含大量中文注释）复刻了它的核心机制，另有 7 个示例和 10 个测试，可以直接运行：`python3 run_all.py`

---

## 0. 一句话

**pi-durable 是给 AI agent 用的「持久化执行」（durable execution）运行时。**
对话、模型轮次、工具调用、你自己的状态，都**先原子地提交到存储，再展示出来**。进程在一轮回答中途死掉，重新打开存储，就从停下的地方继续。

可以把它理解成「专门为 agent loop 设计的 Temporal / Restate」：不是通用工作流引擎，而是把「对话 → 模型 → 工具 → 子 agent」这条链路本身做成了持久化状态机。

## 1. 它要解决什么问题

普通的 agent loop 是内存里的一个 `while` 循环：

```python
while True:
    msg = await model(messages)          # 进程这时死了？流到一半的回答没了
    if not msg.tool_calls: break
    for c in msg.tool_calls:
        result = await run_tool(c)       # 这时死了？扣款到底扣没扣？重启后要不要重跑？
        messages.append(result)
```

问题：
- **崩溃 / 部署 / 被驱逐**（比如 Cloudflare Durable Object 被回收）→ 跑到一半的 run、工具调用、子 agent 全丢；
- **UI 断线重连**看不到中间状态，或者看到的状态和「真实」不一致；
- **重试**可能让有副作用的工具执行两次；
- 用户在 agent 忙的时候又发消息怎么办？想中途打断、想「转向」怎么办？

pi-durable 的答案：
1. 把 agent loop 拆成**持久化任务**（`pi.generation` 调模型、`pi.tool` 跑工具），每一步都存 checkpoint；
2. **所有可见状态都是已提交状态**——包括流式输出到一半的文本（节流提交，默认 100ms），没有「只在内存里」的进度；
3. 外部副作用用「意图 → 执行 → 结果」夹起来，并为每个工具声明能否安全重放；
4. 用一棵**所有权树**管理任务和子 agent（结构化并发）：中止向下传播、自下而上执行。

## 2. 在 pi monorepo 里的位置

Pi 是 Earendil 做的「极简、可扩展的 agent harness」（主产品是 `pi` 编码 agent CLI）。monorepo 里的包（行数为 `src/` 下 TS 代码）：

```text
@earendil-works/pi-telemetry   1.5k   与厂商无关的遥测契约
@earendil-works/pi-ai          27k    统一多 provider 的 LLM API（OpenAI / Anthropic / Google ...）
@earendil-works/chord          14k    类型化宿主运行时：可热重载插件、ReplicatedState、JSON 链路上的 RPC
@earendil-works/pi-codemode     3k    QuickJS 沙箱（让模型写 JS 调工具）
@earendil-works/pi-durable     26k    ← 本项目：持久化的 会话 / 任务 / 文档 运行时
@earendil-works/pi-env          2k    远程 / 本地执行环境（依赖 durable）
@earendil-works/pi-coding-agent 76k   pi CLI 本体（目前没有依赖 durable）

pi-durable 依赖：chord（文档状态 + Context）、pi-ai（模型）、pi-codemode、pi-telemetry
```

也就是说 pi-durable 是一个**独立的、可嵌入的运行时**：你用它来搭建「长期运行、能扛崩溃、多端可连」的 agent 应用（官方有 Cloudflare 上的示例 harness，社区已经有移动端 Web App、Swift 封装等基于它的项目）。

## 3. 核心概念

| 概念 | 是什么 | Python 教学版 |
|---|---|---|
| **Harness** | 一个打开的存储 + 在上面跑 agent 的机器。所有修改走同一条原子提交线 | `harness/harness.py` |
| **Session** | Harness 的底座：唯一的「变更线」（mutation line），一次只执行一个提交 | `session.py` |
| **Conversation** | 一个 transcript（条目序列）。`root()` 是根会话；可以创建更多、可以 fork | `Conversation` |
| **Entry** | transcript 里的一条**不可变**记录：`pi.user` / `pi.assistant` / `pi.tool-result` / `pi.system` / `pi.reset` / 你自己的 kind | `records.py` |
| **Commit** | 原子写：可以同时追加条目、改文档、创建任务，要么全成要么全不成 | `Session.commit` |
| **Document** | 和 transcript 放在一起的类型化 JSON 状态，在提交里修改。内置 `pi.agent` `pi.live` `pi.inbox` `pi.usage` | `documents.py` |
| **Task** | 每一步都存 checkpoint 的状态机。进程重启后从最后一个 checkpoint 继续 | `tasks.py` |
| **Submission** | 交给会话的东西（用户输入 / 要写入的条目），可以等待它的结果 | `Submission` |
| **Turn / Run** | turn = 一次模型回复 + 它的工具调用；run = 从一个输入到最终回答的所有 turn。run 进行中会话就是「忙」 | — |
| **Extension** | 一个有名字的包：tools / sections（系统提示片段）/ hooks / tasks | `registry.py` |
| **Registry** | 本进程装了哪些扩展。**从不存储**；会话只存名字 | `Registry` |
| **Agent** | 会话用什么跑：模型、工具、扩展、指令。以名字存在 `pi.agent`，每次使用时对着注册表解析 | `Agent` |

最核心的一句（spec 原文意译）：

> **Session 原子地提交「不可变条目 + 完整任务记录 + 文档」。只有已提交的状态是可观察的。**

## 4. 分层架构

```text
┌───────────────────────────────────────────────────────────────────────┐
│ 宿主 Host：CLI / Web UI / Cloudflare Durable Object / 你的应用            │
│   root() · submit() · wait() · watch()/viewState() · abort() · configure()│
├───────────────────────────────────────────────────────────────────────┤
│ Harness 层                     harness.ts / submissions.ts / inbox.ts   │
│   Conversation · Submission · inbox 与 turn 边界 · Agent 解析             │
│   内置任务：pi.generation（调模型）  pi.tool（跑工具）  pi.compaction（压缩）│
│   ┌─────────────────────────────────────────────────────────────┐       │
│   │ Registry / Extension：tools · sections · hooks · tasks       │ ← 代码 │
│   └─────────────────────────────────────────────────────────────┘ 不存储 │
├───────────────────────────────────────────────────────────────────────┤
│ 任务层                                              harness/scheduler.ts │
│   TaskScheduler：open 恢复 · 预留 · phase 循环 + 6 条 step 规则             │
│                  中止协议 · completing 挂起 · 级联 · failFast · 睡眠计时器   │
│   TaskRuntime：commit · memo · sleepUntil · outcomes · waitForTask       │
├───────────────────────────────────────────────────────────────────────┤
│ Session 层                          session/session.ts, transaction.ts   │
│   唯一变更线 · Transaction（表读写规则）· Chord 文档 · 提交后发布            │
├───────────────────────────────────────────────────────────────────────┤
│ Storage                       Memory · SQLite · JSONL · Cloudflare DO    │ ← 数据
│   原子 commit(writes) · 全局递增 mint_id() · 读返回分离副本                 │
└───────────────────────────────────────────────────────────────────────┘
```

一个关键的设计取向：**调度器对「run / 模型 / 工具」一无所知**，它只认识「任务记录」这个状态机。Harness 把对话语义实现成几个内置任务，再给调度器一个钩子（调度器自己写下 faulted / orphaned 结局时，由 Harness 做清理）。

## 5. 一次问答的完整生命周期

README 里那张图：

```text
submit(input) → pi.user
  pi.generation → pi.system（只有提示词或工具变了才写）, pi.assistant（工具调用）
    pi.tool × n → pi.tool-result × n   （被 generation 拥有，generation 等它们）
  pi.generation → pi.assistant（回答）→ submission done
```

`examples/02_tool_round.py` 实际打印出来的（编号就是全局递增 ID）：

```text
#3   pi.user         user: Weather in Paris? Then clean up /
#5   pi.assistant    assistant[tool_use]: -> calls get_weather(...), delete_files(...)  (by task 4)
#8   pi.tool-result  tool delete_files -> ERROR blocked: needs approval                (by task 7)
#9   pi.tool-result  tool get_weather -> Sunny, 21°C in Paris                          (by task 6)
#11  pi.assistant    assistant[stop]: Done. (...)                                      (by task 10)

task 4   pi.generation  completed   owner=None   ← 第一个 generation
task 6   pi.tool        completed   owner=4      ← 工具任务被 generation 拥有
task 7   pi.tool        completed   owner=4
task 10  pi.generation  completed   owner=None   ← 工具轮结束后「交接」给新的 generation
```

逐步展开：

| 步骤 | 一次原子提交里写了什么 | 之后 |
|---|---|---|
| ① `submit()` 准入 | submission(placed) + `pi.user` 条目 + 创建 generation 任务 + `pi.live.run = {task, inputs}` | 会话变「忙」 |
| ② 调度器预留 | generation: pending → running | 线外跑 `prepare` phase |
| ③ `prepare` | （必要时）`pi.system` 条目 + checkpoint → `{phase: request, model, tools, cutoff}` | 请求参数被**钉死**，恢复时重发同一个请求 |
| ④ `request` 流式 | 每 100ms 一次：`pi.live.generation.partial = "..."` | UI 通过 watch 看到的 partial 都已落盘 |
| ⑤ 分类：tool_use | `pi.assistant` 条目 + 每个调用一个 `pi.tool` 子任务 + `pi.live.tools` 槽位 + generation → **waiting**(on=工具任务) | generation 不占用任何调用 |
| ⑥ 每个 tool：`call` | 校验 + `before_tool` 钩子 → **提交意图** checkpoint `{phase: execute, arguments, replay}` | 线外执行工具（外部副作用） |
| ⑦ tool 结果 | `pi.tool-result` 条目 + 槽位 done + tool → terminal | 全部终结后 generation 被唤醒 |
| ⑧ `tools` phase | postTools 边界（放置 steer）+ 创建**新的** generation + 把 `pi.live.run` 交给它 + 旧 generation completed | 回到 ② |
| ⑨ 分类：stop | `pi.assistant` 回答 + submission done(answer) + final 边界（有 followUp 就开下一轮 run，否则清掉 run） | 会话空闲 |

## 6. 任务：持久化状态机

### 6.1 定义一个任务

```python
Ticker = define_task(
    name="example.ticker", version=1,                      # 记录里只存 name/version，重启后靠 name 找回代码
    initial=lambda input: {"phase": "tick", "n": 1},       # 第一个 checkpoint
    phases={"tick": tick},                                 # checkpoint["phase"] 决定跑哪个 handler
    abort=lambda task, rt: rt.commit(lambda tx, cur: aborted()),   # 中止处理器负责撤销副作用并写结局
)

async def tick(task, rt):
    n = task["state"]["checkpoint"]["n"]
    if await rt.memo(f"printed-{n}") is None:              # 先写者胜：防止重跑时重复副作用
        await rt.memo(f"printed-{n}", True)
        print(f"tick {n}")
    if n == task["input"]["to"]:
        return await rt.commit(lambda tx, cur: completed(f"counted to {n}"))
    await rt.sleep_until(rt.now() + 50, {"phase": "tick", "n": n + 1})   # 持久化睡眠，结束本 phase
```

### 6.2 状态机

```text
                 预留(reserve)            phase 提交新 checkpoint
   pending ───────────────▶ running ──────────────────────────▶ running ─▶ ...
      ▲                       │ │ │
      │ open 恢复(崩溃)        │ │ └─▶ sleeping(until) ──到点──▶ 再预留
      │ close 写回             │ └───▶ waiting(on, policy) ──on 全部终结──▶ 再预留
      └───────────────────────┘
                              └─────▶ terminal(outcome)
                                  或 completing(outcome) ──拥有的工作排空──▶ terminal

outcome: completed | failed | aborted（自己的 abort 处理器决定的）
         | orphaned（要中止但代码不在了，没人清理）| faulted（代码 bug / 无进展）
```

### 6.3 调度器的 6 条 step 规则（spec §5.1，`scheduler.ts #decide`）

每个 phase 之前和之后，调度器都在变更线上读一次任务记录，套用第一条匹配的规则：

| # | 条件 | 结果 |
|---|---|---|
| 1 | 已 terminal / completing / waiting / sleeping | 本次调用结束（等待和睡眠**不占用调用**） |
| 2 | Harness 正在关闭 | 结束，保留 checkpoint，等重开 |
| 3 | 有持久化的中止标记 | 结束 run 调用；拥有的工作排空后另起 abort 调用 |
| 4 | phase 抛了异常 | 任务 **faulted** |
| 5 | checkpoint 变了（有进展） | 刷新注册表快照；定义被热替换就交接（handover），否则跑下一个 phase |
| 6 | checkpoint 没变 | **faulted**：「phase returned without durable progress」 |

规则 6 很有意思：它强制每个 phase 都必须留下持久化的进展，否则「看起来跑完了其实什么都没存」的 bug 会被立刻暴露。

### 6.4 预留时的阻塞原因（运行时推导，不持久化）

```text
restarts ≥ restartLimit           → 停在 pending，blocked: restart_limit（崩溃循环保护，见 7.5）
注册表里找不到定义                  → 停在 pending，blocked: missing_task（装上扩展自动继续）
存储的 version 比定义新             → blocked: task_too_old
存储的 version 比定义旧             → migrate(input, checkpoint, from)；失败 → blocked: migration_failed
```

Harness **从不**因为「代码缺失」就让任务失败。只有显式中止一个缺代码的任务，才会把它结为 `orphaned`。

## 7. 崩溃恢复：五个机制

### 7.1 Effect sandwich（spec §5.2）

```text
commit 意图（intent）      ← 之后崩溃 = 「副作用可能发生了」
执行外部副作用
commit 结果 / 下一阶段
```

`pi.tool` 就是这么做的：`call` phase 先提交 `{phase: "execute", arguments, replay}`，再执行，再提交结果。重开后停在 `execute` 的任务说明「意图已提交、结果没提交」。

### 7.2 工具的 replay 策略

| `replay` | 崩溃时正在执行的调用，重开后… |
|---|---|
| `"safe"` | 用存下的参数重新执行（工具自己保证幂等） |
| `"unsafe"`（默认） | 不重跑。模型得到 `interrupted` 错误结果，带着崩溃前已提交的输出 |

`examples/04_crash_mid_tool.py` 用 `os._exit(1)` **真的杀掉进程**来演示：

```text
--- reopened; live tasks（running → pending，restarts+1）---
task 4 pi.generation  waiting  checkpoint={'phase': 'tools', ...}                  restarts=0
task 6 pi.tool        pending  checkpoint={'phase': 'execute', ..., 'replay': 'safe'}   restarts=1
task 7 pi.tool        pending  checkpoint={'phase': 'execute', ..., 'replay': 'unsafe'} restarts=1
...
#8 pi.tool-result  charge_card -> ERROR interrupted: ... Output so far: 'charging 42 ... '
#9 pi.tool-result  fetch_page  -> <html>https://example.com ok</html>
```

注意 generation 的 `restarts=0`：它在 `waiting`，崩溃时根本没有占用调用。

### 7.3 memo：先写者胜

`runtime.memo(name, value)` 是一次带门控的提交：已有值就返回已有值。用在副作用之前，重跑时就知道「已经做过了」（见 `examples/03_task_recovery.py`）。

### 7.4 持久化的等待和睡眠

**phase 中间不能 sleep。** phase 崩溃后会从头重跑，内存里的等待会占着调用、截止时间还会在每次重跑时漂移。要等时间就 `sleep_until(until, 下一个 checkpoint)` 结束这个 phase；要等别的任务就提交 `waiting(on=[...])`。模型调用的重试退避、deferred 响应的轮询，内置任务也都是这么做的。

宿主可以通过 `harness.wake()` → `{active, at}` 知道下次什么时候需要进程活着（Cloudflare Durable Object 用它来设 alarm）。

### 7.5 崩溃循环保护（restarts / parking）

一个 phase 如果每次都在下一个 checkpoint 之前把进程弄死（OOM、CPU 超限），会被无限重跑。所以：
- `open()` 时，把每个还是 `running` 的任务改回 `pending`，`restarts + 1`；
- 正常 `close()` 写回 `pending` 但**不计数**（关闭不是崩溃）；
- 任务提交了一个新状态（任何进展）→ 计数清零；
- 计数达到 `restart_limit`（默认 5）→ 任务被**停放**：`blocked: restart_limit`，报告 `task_parked`，宿主展示出来并提供 `retryTask(id)`。

### 7.6 幂等的提交

`submit({..., request_id: "x"})`：同一会话里同一个 `request_id` 永远找回同一个 submission。重启后重新提交不会问两次。

## 8. 结构化并发：所有权树

```text
Conversation(root)
 ├── pi.generation#4            (会话拥有)
 │    ├── pi.tool#6              (generation 拥有)
 │    └── pi.tool#7 ── subagent ─▶ Conversation#20 (被 tool#7 拥有)
 │                                  └── pi.generation#21 ...
 └── app.reporter#30 [background]  ← 后台任务是边界：会话 abort / idle 不穿过它
```

两条规则：

1. **中止向下流动，自下而上执行**：给一个任务打中止标记 → 调度器在下一次 reconcile 提交里给它所有「普通拥有工作」打标记 → 最底层的 abort 处理器先跑 → 父任务的 abort 处理器最后跑，并且能看到孩子们的最终结局。
2. **拥有的工作活不过主人的结束**：任务提交 terminal 时如果还有活着的子工作，就先存成 `completing(outcome)`，子工作全部排空后才写最终的 `terminal`。非 completed 的挂起结局本身就是「取消意图」，会让下面的工作被中止。

`waiting` 的两种策略：`allSettled`（全部终结才恢复）、`failFast`（第一个失败就中止其他的）。

`examples/05_child_tasks.py` 场景 A（一张卡过期，failFast）：

```text
payment expired-2: declined
payment visa-1: abort handler -> refund
payment visa-3: abort handler -> refund
payment visa-4: abort handler -> refund
checkout: resumed, payment outcomes = ['aborted', 'failed', 'aborted', 'aborted']
```

## 9. 忙碌的会话：inbox 与 turn 边界

会话忙 ⇔ `pi.live.run` 存在。忙的时候提交：

| 提交 | 去哪 | 何时放进 transcript |
|---|---|---|
| `when_busy: "steer"` | inbox | 当前工具轮结束后（postTools 边界），**加入当前 run** |
| `followUp`（默认） | inbox | 当前 run 回答后（final 边界），**开启下一轮 run** |
| 纯展示的 `write`（只有 kind/data） | 不排队 | **立刻**追加（聊天消息即时可见） |
| 带 model/head 的 `write`（如 reset） | inbox | 下一个边界 |
| `when_busy: "reject"` | — | 抛 `ConversationBusy`，什么都不写 |

`submission.abort()` 只能撤回还在排队的；`conversation.abort()` 撤回排队的输入（保留 write）并中止当前 run 的所有任务。见 `examples/06_busy_inbox.py`。

## 10. 存储契约

- 四张表：conversations、entries、tasks、submissions；外加 documents。
- `commit(writes)` 原子；会话和条目只能创建不能改；任务、submission 每次整条替换。
- `mint_id()` 一个全局递增数字空间（跨重启），所以「按 ID 排序 = 按创建顺序」。
- 读返回分离副本。
- 后端：Memory、SQLite（WAL + `synchronous=NORMAL`）、JSONL（追加写）、Cloudflare Durable Object SQLite。
- 一个存储同一时刻只能被一个进程打开（没有跨进程锁）。

事务内的表规则（`ReadAfterWrite`）：第一次表写入之后不能再读表（表写是缓冲的，读打到存储看不到它们）；文档随时可读写。

## 11. 错误三原则（spec §10.1）

1. **存储错误终结 Harness**：触发的调用抛错，之后一切抛 `SessionFailed`，必须重新打开（提交是原子的，所以存储总是一致的）。
2. **非法读取只让那一次调用失败**。
3. **你的代码只让它自己的单元失败**：phase 抛错 → 该任务 faulted；工具抛错 → 错误结果（run 继续）；钩子抛错 → 报告并跳过；提交回调抛错 → 那次提交回滚。

## 12. TS 源码 ↔ Python 教学版对照

| TS（`packages/durable/src/`） | 行数 | Python（`pi_durable/`） | 说明 |
|---|---:|---|---|
| `types.ts` | 1274 | `records.py` + `storage/base.py` | 记录类型、Storage 接口 |
| `storage/memory.ts` | 864 | `storage/memory.py` | |
| `storage/sqlite/storage.ts` | 957 | `storage/sqlite.py` | TS 版跑在异步 `SqliteDatabase` 门面上，能跑在 Node / Bun / CF DO |
| `storage/jsonl/storage.ts` | 846 | — | 省略 |
| `session/session.ts` + `transaction.ts` | 2194 | `session.py` | 变更线、事务、文档缓存、发布 |
| `documents.ts` | 208 | `documents.py` | TS 用 Chord 增量 ops，这里整值读写 |
| `harness/scheduler.ts` | 2507 | `scheduler.py` | **最值得读**：`open` `#reserve` `#run` `#decide` `#runAbort` `#reconcile` |
| `harness/define.ts` / `types.ts` | — | `tasks.py` | `defineTask`、`TaskRuntime` |
| `harness/harness.ts` | 548 | `harness/harness.py` | Harness / Conversation / Submission |
| `harness/submissions.ts` + `inbox.ts` | 451 | `harness/submissions.py` | 准入、边界 |
| `harness/generation.ts` | 774 | `harness/generation.py` | prepare / request / retry / poll / tools |
| `harness/tool.ts` | 962 | `harness/tool.py` | call / execute，replay，嵌套调用（省略） |
| `harness/context.ts` | 333 | `context.py` | 条目 → 模型消息 |
| `harness/registry.ts` + `agent.ts` + `prompt.ts` | 625 | `registry.py` | 扩展、注册表、Agent 解析、section |
| `harness/compaction.ts` | 483 | — | 省略 |
| `codemode/` | 1702 | — | 省略 |

## 13. Python 版省略了什么

为了让核心机制一眼能看清，以下内容只在文档里讲、代码里没做：

- **Chord**：文档的增量操作（ops）、`watch()` 帧合并（慢消费者超过 100 帧就换成一帧完整视图）、`viewState()`、`Context`（取消 + 遥测 span）参数；
- **Compaction**：`pi.compaction` 任务、阻塞 / 后台阈值、context overflow 后压缩并重试一次、stale 的摘要；
- **Code mode**：模型写 JS、在 QuickJS 沙箱里调工具；确定性沙箱 + 结果 journal，重启后重放脚本到原位置；
- **嵌套工具调用**（`api.executeTool()`）、`callers`、`deferred` 工具与 `tool_search`；
- **Forks**、context edits（omit / replace）、`rewindable` 文档历史、文档内存回收（`documentRetentionMs`）；
- 顺序工具轮（`executionMode: "sequential"`）、`onYield` 续写钩子、deferred / poll 型 provider；
- 遥测（`pi.durable.phase` 等 span / 指标）、`ExecutionEnv`（read / write / edit / bash 工具）、图片处理、MCP；
- 等待环检测（只检查了「等自己」）、JSONL 存储、`abandonOnRestart`。

## 14. 怎么跑、怎么学

```bash
cd pi-durable-py
python3 run_all.py                      # 7 个示例 + 10 个测试
python3 examples/04_crash_mid_tool.py   # 单独跑某一个
```

| 示例 | 演示 | 对应 TS 示例 |
|---|---|---|
| `01_chat.py` | 一问一答、流式 partial 都是已提交状态、request_id 去重 | 14-chat |
| `02_tool_round.py` | 并行工具、waiting、交接 generation、before_tool 拦截 | 17-coding-tools |
| `03_task_recovery.py` | 自定义任务、memo、持久化睡眠、关闭后重开（SQLite） | 13-recovery |
| `04_crash_mid_tool.py` | **真实进程崩溃**，safe 重跑 / unsafe 得到 interrupted | — |
| `05_child_tasks.py` | 子任务、failFast、中止自下而上、睡眠跨崩溃保留 | 24-child-tasks |
| `06_busy_inbox.py` | steer / followUp / write / reject / 撤回 | 20-inbox |
| `07_registry_and_parking.py` | 缺定义被阻塞、崩溃循环停放、热修复 + retry | 10-registry-reload、31-reload-and-restart |

推荐阅读顺序：

1. **Python 版**，自底向上：`records.py` → `storage/` → `session.py` → `tasks.py` → `scheduler.py` → `harness/submissions.py` → `harness/generation.py` → `harness/tool.py`；
2. **TS 原版**：`packages/durable/README.md` → `docs/spec.md` 的 §1（不变量）、§5（任务）、§6（submission）、§8（内置任务）→ `src/harness/scheduler.ts` 的 `#run`/`#decide`/`#reserve` → `generation.ts` → `tool.ts`；
3. 跑 TS 示例：`node --conditions=source --experimental-strip-types test/examples/24-child-tasks.ts`（在 `packages/durable` 目录下，需要 Node ≥ 22.19）。

## 出处

本项目是对 [earendil-works/pi](https://github.com/earendil-works/pi) 中 `packages/durable` 的**学习性重写**：Python 代码是独立实现，设计、术语和规则来自上游的 README 与 `docs/spec.md`，文中的示例思路对应上游 `test/examples/`。上游项目以 MIT License 发布（Copyright (c) 2025 Mario Zechner）。这里的实现只用于理解其架构，不是官方移植，也不跟随上游 API 变化。
