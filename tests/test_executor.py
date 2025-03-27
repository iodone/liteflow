import time
import pytest
from concurrent.futures import ThreadPoolExecutor

# Import ray conditionally
try:
    import ray
    RAY_AVAILABLE = True
except ImportError:
    RAY_AVAILABLE = False

from liteflow.executor import RayExecutor, PoolExecutor

# Skip Ray tests if Ray is not available
pytestmark = pytest.mark.skipif(not RAY_AVAILABLE, reason="Ray is not installed")


@pytest.fixture
def ray_executor():
    """Fixture for RayExecutor"""
    executor = RayExecutor(ignore_reinit_error=True)
    yield executor
    executor.shutdown()


@pytest.fixture
def thread_pool():
    """Fixture for ThreadPoolExecutor"""
    with ThreadPoolExecutor(max_workers=2) as executor:
        yield PoolExecutor(executor)


def test_ray_executor_init():
    """Test RayExecutor initialization"""
    executor = RayExecutor()
    assert ray.is_initialized()
    executor.shutdown()
    

def test_ray_executor_submit(ray_executor):
    """Test submitting a task to RayExecutor"""
    def simple_task(x):
        return x * 2
    
    # Submit the task
    future = ray_executor.submit(simple_task, 5)
    
    # Get the result using Future interface
    result = future.result()
    assert result == 10


def test_ray_executor_multiple_tasks(ray_executor):
    """Test submitting multiple tasks to RayExecutor"""
    def simple_task(x):
        return x * 2
    
    # Submit multiple tasks
    futures = [ray_executor.submit(simple_task, i) for i in range(5)]
    
    # Get results using Future interface
    results = [future.result() for future in futures]
    assert results == [0, 2, 4, 6, 8]


def test_ray_executor_parallel_execution(ray_executor):
    """Test that tasks run in parallel"""
    def slow_task(x):
        time.sleep(0.5)  # Sleep for 500ms
        return x
    
    # Submit tasks that would take 1.5 seconds sequentially
    start_time = time.time()
    futures = [ray_executor.submit(slow_task, i) for i in range(3)]
    results = [future.result() for future in futures]
    execution_time = time.time() - start_time
    
    # If tasks run in parallel, it should take ~0.5 seconds (plus overhead)
    assert execution_time < 1.0  # Allow some overhead but ensure parallel execution
    assert results == [0, 1, 2]


def test_ray_executor_with_error(ray_executor):
    """Test error handling in RayExecutor"""
    def error_task():
        raise ValueError("Task error")
    
    # Submit task that raises an error
    future = ray_executor.submit(error_task)
    
    # Verify error is propagated
    with pytest.raises(ValueError):
        future.result()