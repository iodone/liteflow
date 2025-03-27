from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable

# Make Ray an optional dependency
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False



class Executor(ABC):
    """Abstract base class for task executors"""
    @abstractmethod
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
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

# Only define RayExecutor if Ray is available
if RAY_AVAILABLE:
    class RayObjectRefFuture(Future):
        """A Future-like wrapper for Ray ObjectRef"""
        
        def __init__(self, object_ref):
            super().__init__()
            self._object_ref = object_ref
            
        def cancel(self):
            # Ray doesn't support cancellation directly
            return False
            
        def cancelled(self):
            # Ray doesn't support cancellation
            return False
            
        def running(self):
            # We can't easily check if a Ray task is running
            return not self.done()
            
        def done(self):
            # Check if the Ray task has completed
            return ray.wait([self._object_ref], timeout=0)[0] != []
            
        def result(self, timeout=None):
            # Get the result from Ray
            try:
                return ray.get(self._object_ref, timeout=timeout)
            except Exception as e:
                # Set the exception in the Future
                self.set_exception(e)
                raise
                
        def exception(self, timeout=None):
            # Ray doesn't provide a way to get the exception without raising it
            # We'll have to try to get the result and catch any exception
            try:
                ray.get(self._object_ref, timeout=timeout)
                return None
            except Exception as e:
                return e
                
        def add_done_callback(self, fn):
            # Ray doesn't support callbacks directly
            # This is a simplified implementation that polls for completion
            def _callback_wrapper():
                import threading
                import time
                while not self.done():
                    time.sleep(0.1)
                fn(self)
            threading.Thread(target=_callback_wrapper).start()


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
            
        def submit(self, fn: Callable, *args, **kwargs) -> Future:
            """Submit a task for execution on Ray
            
            This wraps the function in a Ray remote task and returns
            a Future-like object that wraps the Ray ObjectRef.
            """
            # Create a Ray remote function from the provided function
            remote_fn = ray.remote(fn)
            # Execute the remote function and get the ObjectRef
            object_ref = remote_fn.remote(*args, **kwargs)
            # Wrap the ObjectRef in a Future-like object
            return RayObjectRefFuture(object_ref)
            
        def shutdown(self):
            """Shutdown Ray and cleanup resources"""
            ray.shutdown()
else:
    # Provide a placeholder class that raises an error when instantiated
    class RayExecutor:
        """Placeholder for Ray executor when Ray is not installed"""
        def __init__(self, *args, **kwargs):
            raise ImportError(
                "Ray is not installed. Please install ray package to use RayExecutor. "
                "You can install it with: pip install ray"
            )

# Additional executors can be added here in the future