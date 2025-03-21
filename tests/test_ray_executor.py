import pytest
import time
from concurrent.futures import ThreadPoolExecutor

from liteflow.executor import PoolExecutor, RayExecutor
from liteflow.flow import Context, Flow, NextTask, TaskOutput

# Skip all tests in this module if Ray is not installed
try:
    import ray
    has_ray = True
except ImportError:
    has_ray = False

pytestmark = pytest.mark.skipif(not has_ray, reason="Ray is not installed")


@pytest.fixture
def ray_executor():
    """Create a RayExecutor for testing"""
    try:
        executor = RayExecutor()
        yield executor
    finally:
        executor.shutdown()


@pytest.fixture
def ray_flow(ray_executor):
    """Create a Flow with RayExecutor"""
    return Flow(ray_executor)


def test_ray_executor_simple_task(ray_flow):
    """Test simple task execution with RayExecutor"""
    def action(ctx):
        return TaskOutput("result")
    
    ray_flow.add_task("task1", action)
    result = ray_flow.run("task1")

    assert result == {"task1": "result"}
    assert ray_flow.context.get("task1") == "result"


def test_ray_executor_sequential_tasks(ray_flow):
    """Test sequential tasks with RayExecutor"""
    def task1(ctx):
        return TaskOutput("result1", [NextTask("task2")])

    def task2(ctx):
        assert ctx.get("task1") == "result1"
        return TaskOutput("result2")

    ray_flow.add_task("task1", task1)
    ray_flow.add_task("task2", task2)

    result = ray_flow.run("task1")
    assert result == {"task2": "result2"}


def test_ray_executor_parallel_tasks(ray_flow):
    """Test parallel tasks with RayExecutor"""
    def task1(ctx):
        return TaskOutput("result1", [NextTask("task2"), NextTask("task3")])

    def task2(ctx):
        return TaskOutput("result2")

    def task3(ctx):
        return TaskOutput("result3")

    ray_flow.add_task("task1", task1)
    ray_flow.add_task("task2", task2)
    ray_flow.add_task("task3", task3)

    result = ray_flow.run("task1")
    assert result == {"task2": "result2", "task3": "result3"}


def test_ray_executor_actual_parallel_execution(ray_flow):
    """Test that tasks actually run in parallel with RayExecutor"""
    def task1(ctx):
        return TaskOutput("result1", [NextTask("slow_task1"), NextTask("slow_task2")])

    def slow_task1(ctx):
        time.sleep(0.5)  # Sleep for 500ms
        return TaskOutput("slow_result1", None)

    def slow_task2(ctx):
        time.sleep(0.5)  # Sleep for 500ms
        return TaskOutput("slow_result2", None)

    ray_flow.add_task("task1", task1)
    ray_flow.add_task("slow_task1", slow_task1)
    ray_flow.add_task("slow_task2", slow_task2)

    start_time = time.time()
    result = ray_flow.run("task1")
    execution_time = time.time() - start_time

    # If tasks run sequentially, it would take > 1 second
    # If parallel, it should take ~0.5 seconds (plus small overhead)
    assert execution_time < 0.8  # Allow some overhead but ensure parallel execution
    assert result == {"slow_task1": "slow_result1", "slow_task2": "slow_result2"}


def test_ray_executor_error_handling(ray_flow):
    """Test error handling with RayExecutor"""
    def failing_task(ctx):
        raise ValueError("Task failed")

    ray_flow.add_task("failing", failing_task)

    with pytest.raises(Exception) as exc_info:
        ray_flow.run("failing")

    assert "Task failed" in str(exc_info.value)


def test_ray_executor_with_inputs(ray_flow):
    """Test running tasks with initial inputs using RayExecutor"""
    def task1(ctx, inputs):
        assert inputs == {"input1": "value1"}
        return TaskOutput("result1")

    ray_flow.add_task("task1", task1)
    result = ray_flow.run("task1", inputs={"input1": "value1"})
    assert result == {"task1": "result1"}