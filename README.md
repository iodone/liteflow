# LiteFlow

A lightweight task flow framework

## Getting started

### Installation

```bash
pip install liteflow
```

### Basic Usage

Flow initializes a flow with a thread pool executor. Tasks are added to the flow and can be run by their name.

```python
from concurrent.futures import ThreadPoolExecutor
from liteflow import Flow, TaskOutput, NextTask, Context, StreamChunk
from liteflow.executor import PoolExecutor

executor = PoolExecutor(ThreadPoolExecutor(max_workers=4))

# thread pool executor is optional, defaults to 4 workers
flow = Flow(executor=executor)

# Simple task that returns a result
@flow.task("greet")
def my_task(context: Context) -> TaskOutput:
    return TaskOutput(output="Hello World!")

result = flow.run("greet")  # Returns {"greet": "Hello World!"}
print(result)
```

### Task Chaining
```python
# Tasks can trigger other tasks
@flow.task("task1")
def task1(context: Context) -> TaskOutput:
    return TaskOutput(output="result1", next_tasks=[NextTask("task2")])

@flow.task("task2")
def task2(context: Context) -> TaskOutput:
    # Access results from previous tasks
    t1_result = context.get("task1")  # waits for task1 to complete
    print(t1_result)
    return TaskOutput(output="result2")

result = flow.run("task1")  # Returns {"task2": "result2"}
print(result)
```

### Parallel Execution
```python
import time

@flow.task("starter")
def starter(context: Context) -> TaskOutput:
    # Launch multiple tasks in parallel by simply adding them to the next_tasks list
    return TaskOutput(output="started", next_tasks=[NextTask("slow_task1"), NextTask("slow_task2")])

@flow.task("slow_task1")
def slow_task1(context: Context) -> TaskOutput:
    time.sleep(1)
    return TaskOutput(output="result1")

@flow.task("slow_task2")
def slow_task2(context: Context) -> TaskOutput:
    time.sleep(1)
    return TaskOutput(output="result2")

# Both slow_tasks execute in parallel, taking ~1 second total
result = flow.run("starter")
print(flow.context.get("starter"))
print(result)
```

### Streaming Results
```python
@flow.task("streaming_task")
def streaming_task(context: Context) -> TaskOutput:
    # Stream intermediate results
    stream = context.get_stream()
    for i in range(3):
        # (task_id, chunk_value)
        stream.put(StreamChunk("streaming_task", f"interim_{i}"))
    return TaskOutput(output="final")

# Get results as they arrive
for stream_chunk in flow.stream("streaming_task"):
    print(f"{stream_chunk.task_id}: {stream_chunk.value}")

# Prints:
# streaming_task: interim_0
# streaming_task: interim_1
# streaming_task: interim_2
# streaming_task: final

```

### Dynamic Workflows
```python
@flow.task("conditional_task")
def conditional_task(context: Context) -> TaskOutput:
    count = context.get("count", 0)

    if count >= 3:
        return TaskOutput(output="done")

    context.set("count", count + 1)
    return TaskOutput(
        output=f"iteration_{count}", next_tasks=[NextTask("conditional_task")]
    )


# Task will loop 3 times before finishing
flow.add_task("finish", lambda ctx: TaskOutput("completed", None))
result = flow.run("conditional_task")
print(result)

# Prints:
# {'conditional_task': 'done'}
```

### Input Parameters
```python
@flow.task("greet")
def parameterized_task(context: Context) -> TaskOutput:
    name = context.get("user_name")
    return TaskOutput(output=f"Hello {name}!")

result = flow.run("greet", inputs={"user_name": "Alice"})
print(result)
# Returns {"greet": "Hello Alice!"}
```

### Push next task with inputs
```python
def task1(ctx):
    return TaskOutput("result1", [NextTask("task2", inputs={"input1": "value1"})])

def task2(ctx, inputs):
    assert inputs == {"input1": "value1"}
    return TaskOutput("result2")

flow.add_task("task1", task1)
flow.add_task("task2", task2)
result = flow.run("task1")
print(result)
# Returns {"task2": "result2"}
```

### Dynamic Routing
```python
@flow.task("router")
def router(context: Context) -> TaskOutput:
    task_type = context.get("type")
    routes = {
        "process": [NextTask("process_task")],
        "analyze": [NextTask("analyze_task")],
        "report": [NextTask("report_task")]
    }
    return TaskOutput(output=f"routing to {task_type}", next_tasks=routes.get(task_type, []))

@flow.task("process_task")
def process_task(context: Context) -> TaskOutput:
    return TaskOutput(output="processed data")

result = flow.run("router", inputs={"type": "process"})
print(result)
# Returns {"process_task": "processed data"}
```

### State Management

```python
context = Context()
context.from_dict({"task1": "result1"})

flow = Flow(executor=executor, context=context)
flow.add_task("task2", lambda ctx: TaskOutput("result2"))
flow.run("task2")

print(flow.context.get("task1"))  # Should print "result1"
print(flow.context.get("task2"))  # Should print "result2"

# Serialize the context to a dictionary
print(flow.get_context().to_dict())
# Returns {"task1": "result1", "task2": "result2"}
```

### Map Reduce
```python
@flow.task("task1")
def task1(ctx):
    ctx.set("collector", [])

    return TaskOutput("result1", [
        NextTask("task2", spawn_another=True),
        NextTask("task2", spawn_another=True),
        NextTask("task2", spawn_another=True)
    ])

@flow.task("task2")
def task2(ctx):
    collector = ctx.get("collector")
    collector.append("result2")
    ctx.set("collector", collector)

    return TaskOutput("", [NextTask("task3")])

@flow.task("task3")
def task3(ctx):
    collector = ctx.get("collector")
    return TaskOutput(collector)

result = flow.run("task1")
print(result)
assert result == {"task3": ["result2", "result2", "result2"]}
```
