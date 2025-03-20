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
    """Ray-based task executor"""
    def __init__(self, address=None, **ray_init_kwargs):
        try:
            import ray
        except ImportError:
            raise ImportError("Ray is not installed. Please install it with 'pip install ray'.")
        
        self._ray = ray
        
        # Initialize Ray if it's not already initialized
        if not ray.is_initialized():
            ray_init_kwargs.setdefault('ignore_reinit_error', True)
            ray.init(address=address, **ray_init_kwargs)
            
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a task to Ray for execution and return a Future-like object"""
        # Create a remote function from the provided function
        remote_fn = self._ray.remote(fn)
        
        # Submit the task to Ray
        ray_object_ref = remote_fn.remote(*args, **kwargs)
        
        # Wrap the Ray ObjectRef in a Future-like object
        return RayFuture(ray_object_ref, self._ray)

    def shutdown(self):
        """Shutdown Ray"""
        if self._ray.is_initialized():
            self._ray.shutdown()


class RayFuture:
    """A Future-like wrapper for Ray ObjectRef"""
    def __init__(self, object_ref, ray_module):
        self.object_ref = object_ref
        self._ray = ray_module
        self._done = False
        self._result = None
        self._exception = None
        
    def done(self):
        """Return True if the task is done, False otherwise"""
        if self._done:
            return True
            
        # Check if the result is ready without blocking
        ready_refs, _ = self._ray.wait([self.object_ref], timeout=0)
        if ready_refs:
            self._done = True
            return True
        return False
        
    def result(self):
        """Return the result of the task, blocking if necessary"""
        if not self._done:
            try:
                self._result = self._ray.get(self.object_ref)
                self._done = True
            except Exception as e:
                self._exception = e
                self._done = True
                raise e
        
        if self._exception:
            raise self._exception
            
        return self._result
        
    def cancel(self):
        """Cancel the task if possible"""
        # Ray doesn't support cancellation directly, but we can try to kill the task
        try:
            self._ray.cancel(self.object_ref, force=True)
            return True
        except:
            return False

# Additional executors (like ray) can be added here in the future