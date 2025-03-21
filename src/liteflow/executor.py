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
        
        # Define a Ray remote function for task execution
        @ray.remote
        def ray_execute_task(action_fn, task_id, task_inputs, context_data, logger_name=None):
            """Execute a task in a Ray worker"""
            from liteflow.flow import TaskOutput, NextTask
            from liteflow.context import Context
            import logging
            import inspect
            
            # Create a context with the provided data
            ctx = Context()
            if context_data:
                for key, value in context_data.items():
                    ctx.set(key, value)
            
            # Create a logger
            logger = logging.getLogger(logger_name or "liteflow.ray")
            
            # Execute the action
            try:
                # Check if function accepts inputs parameter
                sig = inspect.signature(action_fn)
                params = list(sig.parameters.keys())
                
                if len(params) > 1 and params[1] == "inputs":
                    result = action_fn(ctx, inputs=task_inputs)
                else:
                    result = action_fn(ctx)
                
                # Convert result to dict if it's a TaskOutput
                if isinstance(result, TaskOutput):
                    next_tasks = None
                    if result.next_tasks:
                        next_tasks = [
                            {
                                'id': nt.id,
                                'inputs': nt.inputs,
                                'spawn_another': nt.spawn_another
                            }
                            for nt in result.next_tasks
                        ]
                    
                    return {
                        'output': result.output,
                        'next_tasks': next_tasks,
                        'is_task_output': True
                    }
                
                return result
            except Exception as e:
                import traceback
                error_info = {
                    'error': str(e),
                    'traceback': traceback.format_exc(),
                    'is_error': True
                }
                return error_info
        
        self._ray_execute_task = ray_execute_task
            
    def submit(self, fn: Callable, *args, **kwargs) -> Future:
        """Submit a task to Ray for execution and return a Future-like object"""
        # Extract arguments
        action = args[0]  # The task function
        next_task = args[1]  # NextTask object
        context = args[2]  # Context object
        logger = args[3] if len(args) > 3 else None  # Logger
        
        # Extract context data - only include serializable data
        context_data = {}
        for key in context.states:
            try:
                value = context.get(key, None)
                if value is not None:
                    context_data[key] = value
            except:
                pass
        
        # Get logger name
        logger_name = logger.name if logger else None
        
        # Submit the task to Ray
        ray_object_ref = self._ray_execute_task.remote(
            action, next_task.id, next_task.inputs, context_data, logger_name
        )
        
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
                result = self._ray.get(self.object_ref)
                self._done = True
                
                # Check if the result is an error
                if isinstance(result, dict) and result.get('is_error', False):
                    self._exception = Exception(result['error'])
                    raise self._exception
                
                # Check if the result is a TaskOutput
                if isinstance(result, dict) and result.get('is_task_output', False):
                    from liteflow.flow import TaskOutput, NextTask
                    
                    next_tasks = None
                    if result.get('next_tasks'):
                        next_tasks = [
                            NextTask(
                                nt['id'],
                                nt['inputs'],
                                nt['spawn_another']
                            )
                            for nt in result['next_tasks']
                        ]
                    
                    self._result = TaskOutput(result['output'], next_tasks)
                else:
                    self._result = result
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