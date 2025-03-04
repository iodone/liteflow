from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

import ray


class Executor(ABC):
    """Abstract base class for task executors"""
    @abstractmethod
    def submit(self, fn: Callable, *args, **kwargs) -> Any:
        """Submit a task for execution"""
        pass

    @abstractmethod
    def shutdown(self):
        """Shutdown the executor and cleanup resources"""
        pass

class PoolExecutor(Executor):
    """ThreadPoolExecutor-based task executor"""
    def __init__(self, executor: ThreadPoolExecutor):
        self._executor = executor

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        return self._executor.submit(fn, *args, **kwargs)

    def shutdown(self):
        self._executor.shutdown(wait=True)

# TODO: RayExecutor is not working yet
class RayExecutor(Executor):
    """Ray-based task executor"""
    def __init__(self):
        ray.init(ignore_reinit_error=True)

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        remote_fn = ray.remote(fn)
        return remote_fn.remote(*args, **kwargs)

    def shutdown(self):
        ray.shutdown()

# Additional executors (like ray) can be added here in the future
