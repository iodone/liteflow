from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable


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

class RayExecutor(Executor):
    """Ray-based task executor for distributed execution"""
    def __init__(self, address: str = None, ignore_reinit_error: bool = True, **ray_init_kwargs):
        """Initialize Ray executor
        
        Args:
            address: Optional Ray cluster address to connect to
            ignore_reinit_error: Whether to ignore Ray reinitialization errors
            ray_init_kwargs: Additional keyword arguments to pass to ray.init()
        """
        # Initialize Ray with the provided parameters
        ray_init_kwargs.setdefault('ignore_reinit_error', ignore_reinit_error)
        if address:
            ray_init_kwargs['address'] = address
        ray.init(**ray_init_kwargs)
        
    def submit(self, fn: Callable, *args, **kwargs) -> Any:
        """Submit a task for execution on Ray
        
        This wraps the function in a Ray remote task and returns
        a Ray ObjectRef that can be used like a Future.
        """
        # Create a Ray remote function from the provided function
        remote_fn = ray.remote(fn)
        # Execute the remote function and return the ObjectRef
        return remote_fn.remote(*args, **kwargs)
        
    def shutdown(self):
        """Shutdown Ray and cleanup resources"""
        ray.shutdown()

# Additional executors (like ray) can be added here in the future