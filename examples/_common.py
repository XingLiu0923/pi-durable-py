"""示例共用的小工具：让示例能直接 `python3 examples/xx.py` 运行，以及打印 transcript。"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def describe(entry):
    kind = entry["kind"]
    message = (entry.get("model") or [{}])[0]
    role = message.get("role")
    if role == "user":
        text = f"user: {message['content']}"
    elif role == "assistant":
        calls = ", ".join(f"{c['name']}({c['arguments']})" for c in message.get("tool_calls") or [])
        text = f"assistant[{message.get('stop_reason')}]: {message.get('content') or ''}"
        if calls:
            text += f" -> calls {calls}"
        if message.get("error"):
            text += f" error={message['error']!r}"
    elif role == "tool_result":
        text = f"tool {message['name']} -> {'ERROR ' if message.get('is_error') else ''}{message['content']}"
    elif role == "system":
        text = f"system sections {list(message.get('sections', {}))}"
    else:
        text = str(entry.get("data"))
    by = f" (by task {entry['by_task_id']})" if entry.get("by_task_id") else ""
    return f"  #{entry['id']:<3} {kind:<15} {text}{by}"


async def print_transcript(conversation, title="transcript"):
    print(f"--- {title} ---")
    for entry in await conversation.entries():
        print(describe(entry))


async def print_tasks(harness, ids):
    for id in ids:
        record = await harness.get_task(id)
        state = record["state"]
        detail = state.get("outcome") or state.get("checkpoint")
        print(f"  task {id:<3} {record['kind']:<15} {state['status']:<10} {detail}"
              f"  restarts={record.get('restarts', 0)}")
