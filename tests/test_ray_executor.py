import time
import pytest
from unittest.mock import patch, MagicMock

from liteflow.flow import Flow, TaskOutput, NextTask
from liteflow.executor import RayExecutor


# Skip these tests if Ray is not installed
ray = pytest.importorskip("ray")


@pytest.fixture
def ray_executor():
    """Create a RayExecutor for testing"""
    executor = RayExecutor(ignore_reinit_error=True)
    yield executor
    executor.shutdown()


@pytest.fixture
def flow_with_ray(ray_executor):
    """Create a Flow with RayExecutor"""
    return Flow(ray_executor)


def test_ray_executor_simple_task(flow_with_ray):
    """Test that a simple task works with RayExecutor"""
    # Define a simple task
    def simple_task(ctx):
        return TaskOutput("result")

    # Add the task to the flow
    flow_with_ray.add_task("simple_task", simple_task)

    # Run the task
    result = flow_with_ray.run("simple_task")

    # Check the result
    assert result == {"simple_task": "result"}
    assert flow_with_ray.context.get("simple_task") == "result"


def test_ray_executor_sequential_tasks(flow_with_ray):
    """Test that sequential tasks work with RayExecutor"""
    # Define tasks
    def task1(ctx):
        return TaskOutput("result1", [NextTask("task2")])

    def task2(ctx):
        assert ctx.get("task1") == "result1"
        return TaskOutput("result2")

    # Add tasks to the flow
    flow_with_ray.add_task("task1", task1)
    flow_with_ray.add_task("task2", task2)

    # Run the first task
    result = flow_with_ray.run("task1")

    # Check the results
    assert result == {"task2": "result2"}
    assert flow_with_ray.context.get("task1") == "result1"
    assert flow_with_ray.context.get("task2") == "result2"


def test_ray_executor_parallel_tasks(flow_with_ray):
    """Test that parallel tasks work with RayExecutor"""
    # Define tasks
    def task1(ctx):
        return TaskOutput("result1", [NextTask("task2"), NextTask("task3")])

    def task2(ctx):
        time.sleep(0.5)  # Add some delay
        return TaskOutput("result2")

    def task3(ctx):
        time.sleep(0.5)  # Add some delay
        return TaskOutput("result3")

    # Add tasks to the flow
    flow_with_ray.add_task("task1", task1)
    flow_with_ray.add_task("task2", task2)
    flow_with_ray.add_task("task3", task3)

    # Run the first task and measure execution time
    start_time = time.time()
    result = flow_with_ray.run("task1")
    execution_time = time.time() - start_time

    # Check the results
    assert result == {"task2": "result2", "task3": "result3"}
    
    # If tasks run in parallel, execution time should be less than 1 second
    # (allowing for some overhead)
    assert execution_time < 1.0


def test_ray_executor_with_inputs(flow_with_ray):
    """Test that inputs are correctly passed to tasks with RayExecutor"""
    # Define a task that uses inputs
    def task_with_inputs(ctx, inputs):
        assert inputs == {"input1": "value1"}
        return TaskOutput(f"Processed {inputs['input1']}")

    # Add the task to the flow
    flow_with_ray.add_task("task_with_inputs", task_with_inputs)

    # Run the task with inputs
    result = flow_with_ray.run("task_with_inputs", inputs={"input1": "value1"})

    # Check the result
    assert result == {"task_with_inputs": "Processed value1"}


def test_ray_executor_error_handling(flow_with_ray):
    """Test that errors in tasks are properly handled with RayExecutor"""
    # Define a task that raises an error
    def failing_task(ctx):
        raise ValueError("Task failed")

    # Add the task to the flow
    flow_with_ray.add_task("failing_task", failing_task)

    # Run the task and expect an exception
    with pytest.raises(Exception) as exc_info:
        flow_with_ray.run("failing_task")

    # Check that the error message is correct
    assert "Task failed" in str(exc_info.value)


def test_ray_executor_init_parameters():
    """Test RayExecutor initialization with different parameters"""
    # Test with default parameters
    with patch.object(ray, 'init') as mock_init:
        executor = RayExecutor(ignore_reinit_error=True)
        mock_init.assert_called_once_with(num_cpus=None, ignore_reinit_error=True)
        
        # Test shutdown
        with patch.object(ray, 'shutdown') as mock_shutdown:
            executor.shutdown()
            mock_shutdown.assert_called_once()
    
    # Test with address parameter
    with patch.object(ray, 'init') as mock_init:
        executor = RayExecutor(address="ray://localhost:10001", ignore_reinit_error=True)
        mock_init.assert_called_once_with(address="ray://localhost:10001", ignore_reinit_error=True)
    
    # Test with num_cpus parameter
    with patch.object(ray, 'init') as mock_init:
        executor = RayExecutor(num_cpus=4, ignore_reinit_error=True)
        mock_init.assert_called_once_with(num_cpus=4, ignore_reinit_error=True)