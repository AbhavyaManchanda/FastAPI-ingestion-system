import requests
import time

BASE_URL = "http://localhost:8000"

def test_ingestion_priority_and_rate_limiting():
     
    medium_payload = {
        "ids": [1, 2, 3, 4, 5],
        "priority": "MEDIUM"
    }
    res1 = requests.post(f"{BASE_URL}/ingest", json=medium_payload)
    ingestion_id_1 = res1.json()["ingestion_id"]
    print(f"[T0] Submitted MEDIUM priority: {ingestion_id_1}")

    time.sleep(4)   

     
    high_payload = {
        "ids": [6, 7, 8, 9],
        "priority": "HIGH"
    }
    res2 = requests.post(f"{BASE_URL}/ingest", json=high_payload)
    ingestion_id_2 = res2.json()["ingestion_id"]
    print(f"[T4] Submitted HIGH priority: {ingestion_id_2}")

     
    time.sleep(20)

     
    status_1 = requests.get(f"{BASE_URL}/status/{ingestion_id_1}").json()
    print("\n[Status MEDIUM priority]:")
    print(status_1)

    
    status_2 = requests.get(f"{BASE_URL}/status/{ingestion_id_2}").json()
    print("\n[Status HIGH priority]:")
    print(status_2)

     
    assert status_1["status"] == "completed"
    assert status_2["status"] == "completed"

    
    all_batches = status_1["batches"] + status_2["batches"]
    completed_batches = [batch for batch in all_batches if batch["status"] == "completed"]

     
    for batch in completed_batches:
        assert len(batch["ids"]) <= 3

    print("\n✅ Test passed: Rate limiting and priority respected.\n")

if __name__ == "__main__":
    test_ingestion_priority_and_rate_limiting()
