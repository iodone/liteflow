import logging
import threading
import traceback
import uuid
from concurrent.futures import Future
from dataclasses import dataclass
from inspect import signature
from queue import Empty, Queue
from threading import Lock
from typing import Any, Callable, Dict, List, Optional

from lmnr import Laminar, observe

from liteflow.executor import Executor

from .context import Context
from .state import State

__ERROR__ = "__ERROR__"
__OUTPUT__ = "__OUTPUT__"
__HASH_SPLIT__ = "____"


@dataclass
class NextTask:
    """
    Represents a task that is scheduled to run next in the flow.

    Attributes:
        id (str): The unique identifier of the next task.
        inputs (Optional[Dict[str, Any]]): A dictionary of inputs to be passed to the next task. Defaults to None.
        spawn_another (bool): If true, task will be executed even though there is already an instance of the same task running. This is useful when you want to run the same task in parallel. Defaults to False.
    """

    id: str
    inputs: Optional[Dict[str, Any]] = None
    spawn_another: bool = False


@dataclass
class TaskOutput:
    output: Any
    next_tasks: Optional[List[NextTask]] = None


@dataclass
class Task:
    id: str
    action: Callable[[Context], TaskOutput]


@dataclass
class StreamChunk:
    task_id: str
    value: Any


def execute_task(
    action: Callable[[Context], TaskOutput],
    task: NextTask,
    context: Context,
    logger: logging.Logger,
) -> TaskOutput:
    """Execute a task and return its output

    Args:
        action: The task function to execute
        task: The task configuration
        context: Context for task execution
        logger: Logger for error reporting

    Returns:
        TaskOutput containing the task's result
    """
    logger.info(f"Starting execution of task '{task.id}'")

    with Laminar.start_as_current_span(
        task.id,
        input={"context": context.to_dict(), "inputs": task.inputs},
    ):
        # Check if action accepts inputs parameter
        sig = signature(action)
        params = list(sig.parameters.keys())

        # 根据函数签名决定如何调用 action
        if len(params) > 1 and params[1] == "inputs":  # 第一个参数是 context
            result = action(context, inputs=task.inputs)
        else:
            result = action(context)

        Laminar.set_span_output(result)

    return result


class Flow:
    """Flow execution engine that manages task scheduling and execution"""

    def __init__(self, executor: Executor, context: Optional[Context] = None):
        """Initialize Flow with an executor

        Args:
            executor: Task executor implementation
            context: Optional shared context
        """
        self.tasks = {}  # str -> Task
        self.active_tasks = set()  # Set of str
        self.context = context or Context()  # Global context
        self.output_task_ids = set()  # Set of str
        self._executor = executor

        # Thread-safety locks
        self.active_tasks_lock = Lock()
        self.output_ids_lock = Lock()
        self.logger = logging.getLogger(__name__)

    def add_task(self, name: str, action: Callable[[Context], TaskOutput]):
        self.context.set_state(name, State.empty())
        self.tasks[name] = Task(name, action)
        self.logger.info(f"Added task '{name}'")

    def task(
        self, name: str
    ) -> Callable[[Callable[[Context], TaskOutput]], Callable[[Context], TaskOutput]]:
        """
        Decorator to register a task in the flow.

        Args:
            name: Name for the task.

        Returns:
            Decorated function.

        Example:
            @flow.task("process_data")
            def process_data(ctx):
                return TaskOutput(output=process(data))
        """

        def decorator(
            func: Callable[[Context], TaskOutput],
        ) -> Callable[[Context], TaskOutput]:
            self.add_task(name, func)
            return func

        return decorator

    def _process_task_result(
        self,
        task_id: str,
        result: TaskOutput,
        task_queue: Queue,
        stream_queue: Optional[Queue] = None,
    ) -> None:
        """处理任务执行结果"""
        # Update context with task output
        self.context.set(task_id, result.output)

        # Push to stream queue if streaming is enabled
        if stream_queue is not None:
            stream_queue.put(StreamChunk(task_id, result.output))

        self.active_tasks.remove(task_id)
        self.logger.info(f"Completed execution of task '{task_id}'")

        # If no next tasks specified, this is an output task
        if not result.next_tasks or len(result.next_tasks) == 0:
            self.logger.info(f"Task '{task_id}' completed as output node")
            self.output_task_ids.add(task_id)
            task_queue.put(NextTask(__OUTPUT__, None))
        else:
            self.logger.debug(
                f"Task '{task_id}' scheduling next tasks: {result.next_tasks}"
            )
            for next_task in result.next_tasks:
                if next_task.id.split(__HASH_SPLIT__)[0] in self.tasks:
                    if next_task.id not in self.active_tasks:
                        self.active_tasks.add(next_task.id)
                        task_queue.put(NextTask(next_task.id, next_task.inputs))
                    elif next_task.spawn_another:
                        self.logger.info(
                            f"Spawning another instance of task '{next_task.id}'"
                        )
                        task_id_with_hash = (
                            next_task.id
                            + __HASH_SPLIT__
                            + str(uuid.uuid4())[0:8]
                        )
                        self.active_tasks.add(task_id_with_hash)
                        task_queue.put(
                            NextTask(task_id_with_hash, next_task.inputs)
                        )
                else:
                    raise Exception(f"Task {next_task.id} not found")

    def _execute_engine(
        self,
        task_queue: Queue,
        stream_queue: Optional[Queue] = None,
    ) -> None:
        """执行任务引擎的核心逻辑"""
        futures: Dict[str, Future] = {}

        while True:
            try:
                next_task = task_queue.get_nowait()
            except Empty:
                if not futures and len(self.active_tasks) == 0:
                    break
            else:
                if next_task.id == __ERROR__:
                    # Cancel all pending futures on error
                    for _, f in futures.items():
                        f.cancel()
                    if stream_queue:
                        stream_queue.put(StreamChunk(__ERROR__, None))
                        break
                    err = self.context.get(__ERROR__)
                    raise Exception(err)

                if next_task.id == __OUTPUT__:
                    if len(self.active_tasks) == 0:
                        if stream_queue:
                            stream_queue.put(StreamChunk(__OUTPUT__, None))
                        break
                    continue

                action = self.tasks[next_task.id.split(__HASH_SPLIT__)[0]].action
                future = self._executor.submit(
                    execute_task,
                    action,
                    next_task,
                    self.context,
                    self.logger,
                )
                futures[next_task.id] = future

            # 处理已完成的任务
            completed_futures_ids = set()
            for task_id, future in futures.items():
                if future.done():
                    try:
                        result = future.result()
                        self._process_task_result(
                            task_id, result, task_queue, stream_queue
                        )
                    except Exception as e:
                        self.context.set(
                            __ERROR__,
                            {
                                "error": str(e),
                                "traceback": traceback.format_exc(),
                            },
                        )
                        self.logger.error(
                            f"Error in executing task '{task_id}': {str(e)}"
                        )
                        self.active_tasks.clear()
                        task_queue.put(NextTask(__ERROR__, None))
                    completed_futures_ids.add(task_id)

            for task_id in completed_futures_ids:
                del futures[task_id]

    @observe(name="flow.run")
    def run(
        self, start_task_id: str, inputs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        self.logger.info(f"Starting engine run with initial task: {start_task_id}")
        # thread-safe queue of task ids
        task_queue = Queue()
        self.active_tasks.add(start_task_id)
        task_queue.put(NextTask(start_task_id, inputs))

        if inputs:
            for key, value in inputs.items():
                self.context.set(key, value)

        self._execute_engine(task_queue)

        return {task_id: self.context.get(task_id) for task_id in self.output_task_ids}

    @observe(name="flow.stream")
    def stream(self, start_task_id: str, inputs: Optional[Dict[str, Any]] = None):
        """流式运行工作流"""
        task_queue = Queue()
        stream_queue = Queue()

        self.active_tasks.add(start_task_id)
        task_queue.put(NextTask(start_task_id, inputs))

        if inputs:
            for key, value in inputs.items():
                self.context.set(key, value)

        self.context.set_stream(stream_queue)

        def run_engine():
            self._execute_engine(task_queue, stream_queue)

        thread = threading.Thread(target=run_engine)
        thread.start()

        while True:
            stream_chunk: StreamChunk = stream_queue.get()
            if stream_chunk.task_id in {__OUTPUT__, __ERROR__}:
                break
            yield stream_chunk

    def get_context(self) -> Context:
        return self.context
