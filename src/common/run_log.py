"""Execution information"""

import uuid

from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, lit

RUN_LOG_TABLE = "job_market.meta.pipeline_runs"

def new_run_id() -> str:
    """Generate a unique ID that identifies one complete pipeline run."""
    return str(uuid.uuid4())

def log_stage_start(spark, run_id: str, source: str, stage: str):
    """Recored that a pipeline stage has started running."""

    spark.sql(
        f"""
            INSERT INTO {RUN_LOG_TABLE} 
                (
                    run_id, source, stage, status, started_at, ended_at, rows_in, rows_out, error_message
                )
            VALUES 
                (
                    :run_id, :source, :stage, 'running', current_timestamp(), NULL, NULL, NULL, NULL
                )
        """,
        args={
                "run_id": run_id, "source": source, "stage": stage
              },
    )

def log_stage_end(spark, run_id: str, stage: str, status: str, 
                  rows_in: int = None, rows_out: int = None, error: str = None) -> None:
    
    """ Update a stage's run-log recored when the stage finishes."""
    delta_table = DeltaTable.forName(spark, RUN_LOG_TABLE)
    delta_table.update(
        condition=(col("run_id") == run_id) & (col("stage") == stage),
        set={
            "status": (lit(status)),
            "ended_at": current_timestamp(),
            "rows_in": lit(rows_in),
            "rows_out": lit(rows_out),
            "error_message": lit(error)
        }
    )