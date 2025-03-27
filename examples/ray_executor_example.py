"""
Example demonstrating the use of RayExecutor with liteflow.

This example shows how to:
1. Initialize a RayExecutor
2. Create a Flow with the RayExecutor
3. Define tasks
4. Run the flow with distributed execution using Ray
"""

import time
from liteflow import Flow, TaskOutput, NextTask, RayExecutor


def main():
    # Initialize the Ray executor
    # You can pass additional Ray configuration parameters here
    executor = RayExecutor(ignore_reinit_error=True)
    
    # Create a flow with the Ray executor
    flow = Flow(executor=executor)
    
    # Define tasks using decorators
    @flow.task("start")
    def start_task(ctx):
        print("Starting the flow...")
        # Return output and specify next tasks
        return TaskOutput(
            output="Started",
            next_tasks=[
                NextTask("process_1", inputs={"data": 10}),
                NextTask("process_2", inputs={"data": 20}),
                NextTask("process_3", inputs={"data": 30})
            ]
        )
    
    @flow.task("process_1")
    def process_1(ctx, inputs):
        print(f"Processing in task 1 with data: {inputs['data']}")
        # Simulate some work
        time.sleep(1)
        return TaskOutput(
            output=f"Processed {inputs['data']} in task 1",
            next_tasks=[NextTask("combine")]
        )
    
    @flow.task("process_2")
    def process_2(ctx, inputs):
        print(f"Processing in task 2 with data: {inputs['data']}")
        # Simulate some work
        time.sleep(1.5)
        return TaskOutput(
            output=f"Processed {inputs['data']} in task 2",
            next_tasks=[NextTask("combine")]
        )
    
    @flow.task("process_3")
    def process_3(ctx, inputs):
        print(f"Processing in task 3 with data: {inputs['data']}")
        # Simulate some work
        time.sleep(2)
        return TaskOutput(
            output=f"Processed {inputs['data']} in task 3",
            next_tasks=[NextTask("combine")]
        )
    
    @flow.task("combine")
    def combine(ctx):
        # This task will be called multiple times as each process task completes
        # We can access the results of all previous tasks from the context
        results = []
        if ctx.has("process_1"):
            results.append(ctx.get("process_1"))
        if ctx.has("process_2"):
            results.append(ctx.get("process_2"))
        if ctx.has("process_3"):
            results.append(ctx.get("process_3"))
            
        print(f"Combining results: {results}")
        return TaskOutput(output=results)
    
    # Run the flow starting with the "start" task
    start_time = time.time()
    result = flow.run("start")
    end_time = time.time()
    
    print(f"\nFlow completed in {end_time - start_time:.2f} seconds")
    print(f"Final result: {result}")
    
    # Don't forget to shut down the executor when done
    executor.shutdown()


if __name__ == "__main__":
    main()