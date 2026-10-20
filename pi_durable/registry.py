"""扩展、注册表与 Agent 解析。对应 TS: src/harness/registry.ts、src/harness/agent.ts、src/harness/tool.ts
（defineTool）、src/harness/prompt.ts（section），spec §7。

- Extension（扩展）：一个有名字的包，可以带 tools / sections（系统提示片段）/ hooks / tasks。
- Registry（注册表）：本进程装了哪些扩展。**注册表里的东西从不存储**；会话只存扩展 / 工具的「名字」。
  所以「代码」可以热替换（同名 install 即原地替换），重启后再 install 一遍，挂起的任务就能接着跑。
- Agent：某个会话此刻用什么跑 —— 把 pi.agent 文档里的名字，对着注册表快照解析成真正的对象。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence

from .tasks import TaskDefinition


@dataclass(eq=False)
class Tool:
    name: str
    description: str
    # 参数：{参数名: Python 类型}。TS 版用 TypeBox schema，并用 pi-ai 的 validateToolArguments() 校验。
    parameters: Dict[str, type]
    # execute(args, api) -> str | {"content": str, "is_error"?: bool, "control"?: {...}} | None
    #   返回 None 时，api.output() 累积的输出就是结果。
    execute: Callable[..., Any]
    # 崩溃时正在执行的调用，重开后要不要重跑？
    #   "safe"   → 重跑（工具自己保证幂等）
    #   "unsafe" → 不重跑，模型得到一个 interrupted 错误结果（带着已提交的部分输出）
    replay: str = "unsafe"


def define_tool(name: str, description: str, parameters: Dict[str, type], execute: Callable[..., Any],
                replay: str = "unsafe") -> Tool:
    return Tool(name=name, description=description, parameters=parameters, execute=execute, replay=replay)


@dataclass(eq=False)
class Section:
    """系统提示的一个具名片段。render(input) 返回 None 表示省略。"""

    name: str
    render: Callable[[Dict[str, Any]], Optional[str]]


def section(name: str, render: Callable[[Dict[str, Any]], Optional[str]]) -> Section:
    return Section(name=name, render=render)


@dataclass(eq=False)
class Hook:
    """target: "tool" 或 "generation"。

    tool:       before_tool(call) -> None | {"block": 原因} | {"arguments": 新参数}
                after_tool(call, result) -> None | 新 result
    generation: before_request(messages) -> None | 新 messages（只影响这一次请求）
                after_tools(outcomes) -> None   （一轮工具全部结束后观察）
    """

    target: str
    handlers: Dict[str, Callable[..., Any]]


def hook(target: str, **handlers: Callable[..., Any]) -> Hook:
    return Hook(target=target, handlers=handlers)


@dataclass(eq=False)
class Extension:
    name: str
    tools: Sequence[Tool] = ()
    sections: Sequence[Section] = ()
    hooks: Sequence[Hook] = ()
    tasks: Sequence[TaskDefinition] = ()


def define_extension(name: str, tools: Sequence[Tool] = (), sections: Sequence[Section] = (),
                     hooks: Sequence[Hook] = (), tasks: Sequence[TaskDefinition] = ()) -> Extension:
    return Extension(name=name, tools=tuple(tools), sections=tuple(sections), hooks=tuple(hooks), tasks=tuple(tasks))


@dataclass
class Agent:
    model: Optional[str]
    instructions: Optional[str]
    extensions: List[Extension]
    tools: Dict[str, Tool]
    sections: List[Section]
    hooks: List[Hook]

    def handlers(self, target: str, name: str) -> List[Callable[..., Any]]:
        return [h.handlers[name] for h in self.hooks if h.target == target and name in h.handlers]

    def render_sections(self, input: Dict[str, Any]) -> Dict[str, str]:
        rendered: Dict[str, str] = {}
        for s in self.sections:
            text = s.render(input)
            if text is not None:
                rendered[s.name] = text
        if self.instructions:  # 会话自己的 instructions 永远最后渲染
            rendered["instructions"] = self.instructions
        return rendered


class RegistrySnapshot:
    """注册表在某一时刻的不可变快照。一个任务 phase 从头到尾用同一个快照。"""

    def __init__(self, extensions: Sequence[Extension], builtins: Dict[str, TaskDefinition]) -> None:
        self.extensions = tuple(extensions)
        self._tasks: Dict[str, TaskDefinition] = dict(builtins)
        for ext in self.extensions:
            for task in ext.tasks:
                self._tasks[task.name] = task

    def task(self, kind: str) -> Optional[TaskDefinition]:
        return self._tasks.get(kind)

    def resolve(self, agent_doc: Dict[str, Any]) -> Agent:
        installed = {e.name: e for e in self.extensions}
        names = agent_doc.get("extensions")
        selected = list(self.extensions) if names is None else [installed[n] for n in names if n in installed]
        tools: Dict[str, Tool] = {}
        for ext in selected:
            for tool in ext.tools:
                tools[tool.name] = tool  # 后装的同名工具覆盖先装的
        enabled = agent_doc.get("tools")
        if enabled is not None:
            tools = {n: t for n, t in tools.items() if n in enabled}
        return Agent(
            model=agent_doc.get("model"),
            instructions=agent_doc.get("instructions"),
            extensions=selected,
            tools=tools,
            sections=[s for ext in selected for s in ext.sections],
            hooks=[h for ext in selected for h in ext.hooks],
        )


class Registry:
    def __init__(self) -> None:
        self._extensions: List[Extension] = []
        self._listeners: List[Callable[[], None]] = []

    def install(self, extension: Extension) -> None:
        """同名扩展原地替换（热重载）；正在跑的 phase 继续用旧代码，下一个 phase 用新代码。"""
        for i, existing in enumerate(self._extensions):
            if existing.name == extension.name:
                self._extensions[i] = extension
                break
        else:
            self._extensions.append(extension)
        self._changed()

    def uninstall(self, extension: Any) -> None:
        name = extension if isinstance(extension, str) else extension.name
        self._extensions = [e for e in self._extensions if e.name != name]
        self._changed()

    def snapshot(self, builtins: Dict[str, TaskDefinition]) -> RegistrySnapshot:
        return RegistrySnapshot(self._extensions, builtins)

    def subscribe(self, listener: Callable[[], None]) -> None:
        self._listeners.append(listener)

    def _changed(self) -> None:
        for listener in list(self._listeners):
            listener()


def validate_arguments(tool: Tool, arguments: Any) -> Optional[str]:
    if not isinstance(arguments, dict):
        return "arguments must be an object"
    for name, kind in tool.parameters.items():
        if name not in arguments:
            return f"missing argument {name!r}"
        if kind is float and isinstance(arguments[name], int):
            continue
        if not isinstance(arguments[name], kind):
            return f"argument {name!r} must be {kind.__name__}"
    return None
