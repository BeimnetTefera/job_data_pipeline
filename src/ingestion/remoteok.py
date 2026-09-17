import json
from datetime import datetime, timezone

import requests
from src.common.config import load_source_config
from src.common.run_log import log_stage_start, log_stage_end

def fetch_and_save(spark, run_id: str, source_name: str = "remoteok") -> dict:
    """Fetch and save job data from RemoteOK."""
    config = load_source_config(source_name)
    log_stage_start(spark, run_id, source_name, "ingestion")

    try:
        response = requests.get(
            config["endpoint"],
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        jobs =  payload[1:] if payload and "legal" in payload[0] else payload

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        file_path = f"{config['raw_path']}/{source_name}_{timestamp}.json"

        with open(file_path, "w") as f:
            json.dump(jobs, f)

        log_stage_end(spark, run_id, "ingestion", "success", rows_in=len(jobs), rows_out=len(jobs))
        return {"file_path": file_path, "row_count": len(jobs)}

    except Exception as e:
        log_stage_end(spark, run_id, "ingestion", "failed", error=str(e))
        raise
    