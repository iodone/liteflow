from abc import ABC, abstractmethod
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Callable
import threading



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
    def __init__(self, address: str = None, num_cpus: int = None, ignore_reinit_error: bool = True):
        """Initialize Ray executor
        
        Args:
            address: Optional Ray cluster address to connect to
            num_cpus: Optional number of CPUs to use
            ignore_reinit_error: Whether to ignore Ray reinitialization errors
        """
        try:
            import ray
        except ImportError:
            raise ImportError("Ray is not installed. Please install it with 'pip install ray'")
        
        # Initialize Ray with the provided parameters
        if address:
            ray.init(address=address, ignore_reinit_error=ignore_reinit_error)
        else:
            ray.init(num_cpus=num_cpus, ignore_reinit_error=ignore_reinit_error)
        
        self._ray = ray

    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a task to Ray for execution
        
        This method creates a wrapper that converts Ray's ObjectRef to a Future
        to maintain compatibility with the Flow class which expects a Future.
        """
        # Create a Future that will be resolved when the Ray task completes
        future = Future()
        
        # Create a Ray remote function
        remote_fn = self._ray.remote(fn)
        
        # Execute the remote function and get a Ray ObjectRef
        ray_future = remote_fn.remote(*args, **kwargs)
        
        # Start a thread to wait for the Ray result and update the Future
        def _wait_for_ray_result():
            try:
                # Wait for the Ray task to complete and get the result
                result = self._ray.get(ray_future)
                future.set_result(result)
            except Exception as e:
                # If there's an exception, set it on the Future
                future.set_exception(e)
        
        # Start the thread to wait for the Ray result
        threading.Thread(target=_wait_for_ray_result).start()
        
        return future

    def shutdown(self):
        """Shutdown Ray"""
        self._ray.shutdown()

# Additional executors (like ray) can be added here in the future