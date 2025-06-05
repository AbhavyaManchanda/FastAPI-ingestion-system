 
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
from typing import List, Dict
import uuid, time, asyncio, heapq
from enum import Enum
from datetime import datetime

app = FastAPI()

 
class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

 
class IngestRequest(BaseModel):
    ids: List[int]
    priority: Priority

 
ingestion_store: Dict[str, Dict] = {}

 
job_queue = []

 
priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}

 
is_processing = False
 
@app.post("/ingest")
async def ingest(request: IngestRequest, background_tasks: BackgroundTasks):
    ingestion_id = str(uuid.uuid4())
    created_at = time.time()

     
    ids = request.ids
    batches = []
    for i in range(0, len(ids), 3):
        batch_ids = ids[i:i + 3]
        batches.append({
            "batch_id": str(uuid.uuid4()),
            "ids": batch_ids,
            "status": "yet_to_start"
        })

    ingestion_store[ingestion_id] = {
        "priority": request.priority,
        "created_at": created_at,
        "status": "yet_to_start",
        "batches": batches
    }

    
    heapq.heappush(job_queue, (
        priority_order[request.priority],
        created_at,
        ingestion_id
    ))

    if not is_processing:
        background_tasks.add_task(process_queue)

    return {"ingestion_id": ingestion_id}

 
@app.get("/status/{ingestion_id}")
def get_status(ingestion_id: str):
    if ingestion_id not in ingestion_store:
        return {"error": "Ingestion ID not found"}

    ingestion = ingestion_store[ingestion_id]
    batch_statuses = [b["status"] for b in ingestion["batches"]]

    if all(s == "yet_to_start" for s in batch_statuses):
        overall_status = "yet_to_start"
    elif all(s == "completed" for s in batch_statuses):
        overall_status = "completed"
    else:
        overall_status = "triggered"

    return {
        "ingestion_id": ingestion_id,
        "status": overall_status,
        "batches": ingestion["batches"]
    }

 
async def process_queue():
    global is_processing
    is_processing = True

    while job_queue:
        _, _, ingestion_id = heapq.heappop(job_queue)
        ingestion = ingestion_store[ingestion_id]

        ingestion["status"] = "triggered"
        for batch in ingestion["batches"]:
            if batch["status"] == "completed":
                continue

            batch["status"] = "triggered"
            await asyncio.sleep(5)  # Rate limit
            await simulate_external_api(batch["ids"])
            batch["status"] = "completed"

    is_processing = False

 
async def simulate_external_api(ids: List[int]):
    print(f"Processing: {ids}")
    await asyncio.sleep(1)   
    return [{"id": i, "data": "processed"} for i in ids]
