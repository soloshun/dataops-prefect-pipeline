import logging
import uuid
from datetime import datetime
from prefect import flow, task, get_run_logger
from prefect import runtime
from prefect.client.orchestration import get_client
import asyncio

# --- MOCKING YOUR FRAMEWORK/JOB.PY ---
class PrefectJobLoggerAdapter:
    """
    Wraps Prefect's logger to inject flow_run_id into every log message.
    Works with Prefect's native logging system.
    """
    def __init__(self, logger, flow_run_id: str):
        self.logger = logger
        self.flow_run_id = flow_run_id
    
    def info(self, msg, **kwargs):
        self.logger.info(f"[{self.flow_run_id}] {msg}", **kwargs)
    
    def warning(self, msg, **kwargs):
        self.logger.warning(f"[{self.flow_run_id}] {msg}", **kwargs)
    
    def error(self, msg, **kwargs):
        self.logger.error(f"[{self.flow_run_id}] {msg}", **kwargs)
    
    def debug(self, msg, **kwargs):
        self.logger.debug(f"[{self.flow_run_id}] {msg}", **kwargs)

class BaseJob:
    """
    Simulating 'framework.job.Job'.
    """
    def __init__(self, run_id: str = "N/A"):
        self.run_id = run_id
        
        # Use Prefect's logger instead of standard logging
        prefect_logger = get_run_logger()
        
        # Wrap it to inject flow_run_id into messages
        self.logger = PrefectJobLoggerAdapter(prefect_logger, self.run_id)

    def run(self):
        # This log will have the ID attached automatically
        self.logger.info("Starting the job execution...")
        self.logger.warning("This is a warning log to test Datadog filtering.")
        self.heavy_computation()
        self.logger.info("Job finished successfully.")

    def heavy_computation(self):
        self.logger.info("Processing data...")
        # Simulate work
        pass

# --- MOCKING YOUR SPECIFIC JOB IMPLEMENTATION ---
class SolarForecastJob(BaseJob):
    pass

# --- YOUR FLOW CODE (Modified) ---
@flow(name="EnBW-PoC-Flow", log_prints=True)
def job_flow_poc():
    # 1. Get the Context (The ID Prefect assigned)
    # Note: If running locally without a server, this might be mock data.
    ctx = runtime.flow_run
    run_id = ctx.id
    
    # 2. Dynamic Renaming (The "UI Traceability" part)
    # We use the client to update the name *during* the run.
    new_flow_name = f"Solar-Job-EXEC-{str(run_id)}"
    # new_flow_name = f"Solar-Job-EXEC-{str(run_id)[:8]}"
    print(f"--> Changing Flow Name to: {new_flow_name}")
    
    # We need to run the async client update in a sync flow
    asyncio.run(_rename_flow_run(run_id, new_flow_name))

    # 3. Pass ID to Job (The "Datadog Traceability" part)
    # In your real code, you'd load the class dynamically here.
    job = SolarForecastJob(run_id=str(run_id))
    job.run()

async def _rename_flow_run(flow_run_id, new_name):
    """Helper to update flow run name via API"""
    async with get_client() as client:
        await client.update_flow_run(flow_run_id, name=new_name)

if __name__ == "__main__":
    # Run the flow
    job_flow_poc()