# FastAPI-ingestion-system
# Async Ingestion System with Priority Queue & Rate Limiting

A RESTful API system that ingests ID batches, processes them asynchronously, and honors rate limits + priority.

## 📌 Features

- Asynchronous processing (FastAPI + background tasks)
- Priority queue: HIGH > MEDIUM > LOW
- Rate limiting: 1 batch (3 IDs) every 5 seconds
- Status tracking per ingestion + batch level

## 🚀 Endpoints

### `POST /ingest`
Ingest a list of IDs.

**Payload:**
```json
{
  "ids": [1, 2, 3, 4, 5],
  "priority": "HIGH"
}
