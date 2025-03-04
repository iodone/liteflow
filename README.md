# Flow

A lightweight task engine for building AI agents that prioritizes simplicity and flexibility.

## Core Concept

Unlike traditional node and edge-based workflows, Flow uses a dynamic task queue system built on three simple principles:

1. **Concurrent Execution** - Tasks run in parallel automatically
2. **Dynamic Scheduling** - Tasks can schedule new tasks at runtime
3. **Smart Dependencies** - Tasks can await results from previous operations

Results of all tasks are stored in a thread-safe `Context`.

This task-based architecture makes complex workflows surprisingly simple:

- [x] Parallel task execution without explicit threading code
- [x] Self-modifying dynamic workflows and cycles
- [x] Conditional branching and control flow
- [x] Streaming of tasks execution
- [x] State management, load previous state and save current state
- [x] Start execution from a specific task
- [x] Dynamically push next tasks with specific inputs
- [x] Map Reduce, running the same task in parallel on multiple inputs and collecting results

By removing the need to predefine edges between nodes, and opting for a dynamic task scheduling architecture, Flow helps you write better and cleaner code by making it easier to reason about control flow and dependencies.

Flow is lightweight, bloat-free, and has no external dependencies for the engine. It is designed to be simple, flexible and very powerful, and is maintained by the [Laminar](https://github.com/lmnr-ai/lmnr) team.

## Auto-instrumentation
Flow comes with auto-instrumentation for tracing using [Laminar](https://github.com/lmnr-ai/lmnr). To enable OpenTelemetry-based tracing, initialize the Laminar SDK before using Flow.

```python
from lmnr import Laminar
Laminar.initialize(project_api_key="...")
```

> Tracing is extremely useful for debugging and state reconstruction. When tracing is enabled, Flow will automatically capture the state at each step. During debugging, you can load the captured state and inspect the context. To learn more about tracing, see the [Laminar docs](https://docs.lmnr.ai).

## Installation

```bash
pip install lmnr-flow
```

## Getting started

### Basic Usage
```python
from concurrent.futures import ThreadPoolExecutor
from lmnr_flow import Flow, TaskOutput, NextTask, Context, StreamChunk

# thread pool executor is optional, defaults to 4 workers
flow = Flow(thread_pool_executor=ThreadPoolExecutor(max_workers=4))

# Simple task that returns a result
def my_task(context: Context) -> TaskOutput:
    return TaskOutput(output="Hello World!")

flow.add_task("greet", my_task)
result = flow.run("greet")  # Returns {"greet": "Hello World!"}
```

### Task Chaining
```python
# Tasks can trigger other tasks
def task1(context: Context) -> TaskOutput:
    return TaskOutput(output="result1", [NextTask("task2")])

def task2(context: Context) -> TaskOutput:
    # Access results from previous tasks
    t1_result = context.get("task1")  # waits for task1 to complete
    return TaskOutput(output="result2")

flow.add_task("task1", task1)
flow.add_task("task2", task2)
flow.run("task1")  # Returns {"task2": "result2"}
```

### Parallel Execution
```python
def starter(context: Context) -> TaskOutput:
    # Launch multiple tasks in parallel by simply adding them to the next_tasks list
    return TaskOutput(output="started", [NextTask("slow_task1"), NextTask("slow_task2")])

def slow_task1(context: Context) -> TaskOutput:
    time.sleep(1)
    return TaskOutput(output="result1")

def slow_task2(context: Context) -> TaskOutput:
    time.sleep(1)
    return TaskOutput(output="result2")

# Both slow_tasks execute in parallel, taking ~1 second total
flow.add_task("starter", starter)
flow.add_task("slow_task1", slow_task1)
flow.add_task("slow_task2", slow_task2)
flow.run("starter")
```

### Streaming Results
```python
def streaming_task(context: Context) -> TaskOutput:
    # Stream intermediate results
    stream = context.get_stream()
    for i in range(3):
        # (task_id, chunk_value)
        stream.put(StreamChunk("streaming_task", f"interim_{i}"))
    return TaskOutput(output="final")

flow.add_task("streaming_task", streaming_task)

# Get results as they arrive
for task_id, output in flow.stream("streaming_task"):
    print(f"{task_id}: {output}")
    # Prints:
    # streaming_task: interim_0
    # streaming_task: interim_1
    # streaming_task: interim_2
    # streaming_task: final
```

### Dynamic Workflows
```python
def conditional_task(context: Context) -> TaskOutput:
    count = context.get("count", 0)
    
    if count >= 3:
        return TaskOutput(output="done")
    
    context.set("count", count + 1)
    return TaskOutput(output=f"iteration_{count}", [NextTask("conditional_task")])

# Task will loop 3 times before finishing
flow.add_task("conditional_task", conditional_task)
flow.add_task("finish", lambda ctx: TaskOutput("completed", None))
flow.run("conditional_task")
```

### Input Parameters
```python
def parameterized_task(context: Context) -> TaskOutput:
    name = context.get("user_name")
    return TaskOutput(output=f"Hello {name}!")

flow.add_task("greet", parameterized_task)
result = flow.run("greet", inputs={"user_name": "Alice"})
# Returns {"greet": "Hello Alice!"}
```

### Push next task with inputs
```python
def task1(ctx):
    return TaskOutput("result1", [NextTask("task2", inputs={"input1": "value1"})])

# task2 will be called with inputs={"input1": "value1"}
def task2(ctx, inputs):
    assert inputs == {"input1": "value1"}
    return TaskOutput("result2")

flow.add_task("task1", task1)
flow.add_task("task2", task2)
result = flow.run("task1")
# Returns {"task2": "result2"}
```

### Dynamic Routing
```python
def router(context: Context) -> TaskOutput:
    task_type = context.get("type")
    routes = {
        "process": [NextTask("process_task")],
        "analyze": [NextTask("analyze_task")],
        "report": [NextTask("report_task")]
    }
    return TaskOutput(output=f"routing to {task_type}", routes.get(task_type, []))

def process_task(context: Context) -> TaskOutput:
    return TaskOutput(output="processed data")

flow.add_task("router", router)
flow.add_task("process_task", process_task)
result = flow.run("router", inputs={"type": "process"})
# Returns {"process_task": "processed data"}
```

### State Management

```python
context = Context()
context.from_dict({"task1": "result1"})

flow = Flow(context=context)
flow.add_task("task2", lambda ctx: TaskOutput("result2"))
flow.run("task2")

assert flow.context.get("task1") == "result1" # True, because it was set in the context
assert flow.context.get("task2") == "result2"


# Serialize the context to a dictionary
flow.get_context().to_dict()
# Returns {"task1": "result1", "task2": "result2"}
```

### Map Reduce
```python
def task1(ctx):
    ctx.set("collector", [])

    return TaskOutput("result1", [
        NextTask("task2", spawn_another=True),
        NextTask("task2", spawn_another=True),
        NextTask("task2", spawn_another=True)
    ])

def task2(ctx):
    collector = ctx.get("collector")
    collector.append("result2")
    ctx.set("collector", collector)

    return TaskOutput("", [NextTask("task3")])

def task3(ctx):
    collector = ctx.get("collector")
    return TaskOutput(collector)

flow.add_task("task1", task1)
flow.add_task("task2", task2)
flow.add_task("task3", task3)

result = flow.run("task1")
assert result == {"task3": ["result2", "result2", "result2"]}
```

### LLM Agent with Dynamic Tool Selection

```python
from typing import List
import json

def llm_agent(context: Context) -> TaskOutput:
    # Simulated LLM response that determines which tools to use
    prompt = context.get("user_input")
    llm_response = {
        "reasoning": "Need to search database and format results",
        "tools": ["search_db", "format_results"]
    }
    
    # Schedule the selected tools in sequence
    next_tasks: List[NextTask] = []
    for tool in llm_response["tools"]:
        next_tasks.append(NextTask(tool))
    
    return TaskOutput(output=llm_response["reasoning"], next_tasks)

def search_db(context: Context) -> TaskOutput:
    # Simulate database search
    results = ["result1", "result2"]
    return TaskOutput(output=results)

def format_results(context: Context) -> TaskOutput:
    # Format results from previous task
    search_results = context.get("search_db")
    formatted = json.dumps(search_results, indent=2)
    return TaskOutput(output=formatted)

# Set up the agent
flow = Flow()
flow.add_task("llm_agent", llm_agent)
flow.add_tool("search_db", search_db)
flow.add_tool("format_results", format_results)

# Run the agent
result = flow.run("llm_agent", inputs={"user_input": "Find and format data"})
# Returns the final formatted results
```

## Advanced Features

- **Context Sharing**: All tasks share the same context, allowing for complex data flows
- **Error Handling**: Exceptions in tasks are properly propagated
- **Thread Safety**: All operations are thread-safe
- **Minimal Dependencies**: Core engine has zero external dependencies

## Roadmap
- [ ] Add async support
- [ ] Serverless deployment

