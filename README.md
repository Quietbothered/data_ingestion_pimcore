# data_ingestion_pimcore
This repo holds the code for fast-api micro service for data ingestion from excel and json files and help improve the performance for data ingestion in a pim-core project

## Project architecture
### Directories 
```bash
app
├── api
│   ├── ingest_data.py
│   ├── __init__.py
├── controllers
│   ├── ingestion_controllers.py
├── core
│   ├── config.py
│   └── __init__.py
├── logs
│   ├── debug
│   │   └── debug.log
│   ├── error
│   │   └── error.log
│   └── info
│       └── info.log
├── main.py
├── old_code_backup
│   ├── excel_reader_new1.py
│   ├── excel_reader_new.py
│   └── excel_reader_old.py
├── schemas
│   ├── __init__.py
│   ├── request_model.py
│   └── response_model.py
├── services
│   ├── data_integrity_manager.py
│   ├── excel_reader.py
│   ├── __init__.py
│   ├── json_reader.py
└── utils
    ├── error_messages.py
    ├── field_descriptions.py
    ├── __init__.py
    ├── json_decimal_encoder.py
    ├── logger_info_messages.py
    ├── logger.py
    ├── log_initializer.py
    ├── logs_re_namer.py
```

#### **app/api/**
Purpose: Defines HTTP endpoints and request/response boundaries.

Contains: ingest_data.py

Responsibilities:
- Define FastAPI routes (@app.post, @router.post)
- Accept HTTP requests
- Validate incoming payloads using schemas
- Delegate execution to controllers
- Return HTTP responses

#### **app/controllers/**
Purpose: Coordinates what should happen, not how it happens.

Contains: ingestion_controllers.py

Responsibilities:
- Generate ingestion IDs
- Decide which service to invoke (JSON vs Excel)
- Attach background tasks
- Handle high-level success/failure
- Translate service outcomes into API responses

Why I chose to write services orchestration logic inside controllers instead of writing it in the api?
- Keeps API thin
- Keeps services reusable
- Allows orchestration changes without touching core logic

#### **app/core/**
Purpose: Holds application-wide configuration.

Contains: config.py

Typical responsibilities:
- Environment-based settings
- Timeouts
- Feature flags
- Global constants

Why this exists:
- Avoids hardcoding values across the codebase
- Central source of truth for configuration

#### **app/logs/**
```bash
logs/
 ├── debug/
 ├── info/
 └── error/
```

Key points:
- Generated at runtime
- Not part of business logic
- Managed by logging utilities
- Can be mounted to volumes in Docker
- Can be rotated & archived

Separation by level:
- debug → internal execution flow
- info → business milestones
- error → failures & alerts

#### **app/schemas/**
Purpose: Defines what data looks like, not what you do with it.

Contains:
- ```request_model.py```
- ```response_model.py```

Responsibilities:
- Validate incoming API requests
- Define response formats
- Ensure type safety
- Prevent invalid data entering the system

Why I chose to enforce pydantic models both in request and response?
- Strong contract between API and services
- Early failure instead of silent corruption
- Self-documenting APIs

#### **app/services/**
Purpose: Implements what the system actually does.

Contains:
- json_reader.py
- excel_reader.py
- data_integrity_manager.py

Responsibilities:
- Stream files
- Chunk records
- Compute checksums
- Send data to Pimcore
- Retry on failures
- Ensure idempotency & ordering

Key characteristics:
- Stateless or controlled state
- Testable independently
- Reusable
- Framework-agnostic

#### **app/utils/**
Purpose: Provides generic, reusable helpers that support services but are not business logic.

Contains:
- logger.py
- log_initializer.py
- logs_re_namer.py
- json_decimal_encoder.py
- error_messages.py
- field_descriptions.py

Responsibilities:
- Logging infrastructure
- JSON serialization helpers
- Error constants
- Formatting helpers
- Shared enums & mappings

Utilities are:
- Stateless
- Side-effect free (mostly)
- Not domain-specific

## How to run the project?
If you want to run just the microservice then you need to do this : <br>
Go to this directory 
```bash
cd /home/aditya/github/data_ingestion_pimcore
```
and then run this command 
```bash
uvicorn app.main:app --reload
```
But if you want to also test this microservice before you integrate it with your pim-core project then you also need to do this : 
```bash
cd /home/aditya/github/data_ingestion_pimcore/tests/pim_core_mock_test
```
and run this command 
```bash
uvicorn pim_core_mock_test:app --port 9000 --reload
```
This command will run a mock pim-core callback url and this particular url is used by the microservice to dump all the streamed data. <br>
After you run this mock pim-core callback url api you will have to include this url 
```bash 
http://127.0.0.1:9000/callback
``` 
in your request when hitting the api for data ingestion.

If you want to know more about the api then after running the micro-service server you need to go to this url to access the self documenting docs for the apis of the microservice.
```bash
http://127.0.0.1:8000/docs
```

If want to access the self documenting docs for the mock pim-core apis when go to this url
```bash
http://127.0.0.1:9000/docs
``` 

## Purpose of this project 
Ingest data in fast and reliable way and send the ingested data back to pim-core. 

For this I have implemented 
- Streaming ingestion (not batch load)
- Chunked transfer
- Back-pressure aware (chunk sizing)
- Network-fault tolerant
- Data-integrity guaranteed
- Asynchronous ingestion
- External system ACK-driven

### Streaming ingestion (not batch load)
What this means:
- The entire file is never loaded into memory
- Records are processed one by one
- The system scales to very large files
```python
for record in ijson.items(f, "item"):
```
How it works?
- ijson parses JSON incrementally
- Each record is yielded as soon as it’s read
- Memory usage stays bounded regardless of file size
Why this matters for Pimcore?
- Pimcore does not get overwhelmed
- Large catalog/product files can be ingested safely
- No JVM/PHP memory spikes on the Pimcore side
#### **Chunked transfer**
```python
chunk.append(record)

if self._should_flush(...):
    await self._send_chunk(...)
```
How chunks are formed?
- Records accumulate in chunk
- Chunk is flushed when:
    - record count limit OR
    - memory limit is reached
Pimcore impact
- Each request is small
- Failures are isolated per chunk
- Easy retry & validation
#### **Back-pressure aware (chunk sizing)**
The sender adapts how much data it sends at once to avoid overwhelming the receiver.
```python
def _should_flush(self, request, chunk, chunk_bytes, next_record_bytes):
```
```python
len(chunk) >= request.chunk_size_by_records
OR
(chunk_bytes + next_record_bytes) > request.chunk_size_by_memory
```
Runtime behavior
- Pimcore decides its tolerance
- Pimcore passes:
    - chunk_size_by_records OR
    - chunk_size_by_memory
- The ingestion service respects it strictly
Why this is important?
- Pimcore controls ingestion pressure
- No uncontrolled payload sizes
- Prevents PHP request size / proxy truncation issues
#### **Network-fault tolerant**
What this means ? <br>
Temporary network failures do not break ingestion.
```python
for attempt in range(3):
    try:
        resp = await client.post(...)
        if ack is not True:
            raise Exception
        return
    except Exception:
        if attempt == 2:
            raise
```
Runtime behavior
- Each chunk:
    - is retried up to 3 times
- Failure of one chunk:
    - does NOT affect previous chunks
- Hard failure only after retries exhausted
Pimcore impact
- Temporary downtime does not cause data loss
- Retries are chunk-scoped, not file-scoped

#### **Data-integrity guaranteed**
Pimcore receives exactly the same data that was sent — no corruption, no truncation, no reordering.
Checksum creation (microservice side) (sender)
```python
checksum = ChunkIntegrityManager.compute_checksum(records)
``` 
```python
orjson.dumps(records, option=OPT_SORT_KEYS, default=orjson_default)
```
Validation (Pimcore callback side)
```python
calculated = sha256(canonical_dumps(records))
if calculated != checksum:
    ack = False
```
What problems this prevents?
- Partial transmission
- Proxy truncation
- Corrupted JSON
- Re-ordered keys
- Duplicate chunk replays
```python
orjson.OPT_SORT_KEYS
```
This guarantees:
- Deterministic JSON byte representation
- Same checksum on both sides

#### **Asynchronous ingestion**
The API responds immediately, ingestion continues in background.
```python
bg.add_task(self.json_streamer.stream_and_push, ingestion_id, request)
```
Runtime behavior
- Client calls /api/ingest
- Response returns:
    ```json
    { "status": "STARTED", "ingestion_id": "..." }
    ```
- Actual ingestion continues asynchronously

#### **External system ACK-driven**
HTTP success ≠ data accepted. Pim-core explicitely say ```ack = true```
```python
ack = ack_response.get("ack")

if ack is not True:
    raise Exception(...)
```
Pimcore controls ingestion correctness
- Pimcore validates:
    - ordering
    - checksum
    - duplicates
- Pimcore decides acceptance
- Sender reacts accordingly
Why this is critical?
- HTTP 200 alone is meaningless
- Business-level success must be explicit

#### **Ordered, idempotent delivery**
What this means?
- Chunks arrive in order
- Duplicate chunks don’t cause duplication
```python
chunk_id = f"{ingestion_id}:{chunk_number}"
```
```python
if chunk_id in processed_chunks:
    return ack=True
```
```python
if chunk_number != last + 1:
    return OUT_OF_ORDER_CHUNK
```
Pimcore guarantees
- No duplicate writes
- No skipped chunks
- Deterministic ingestion

#### **Completion handshake**
Pim-core explicitely told all chunks are done.
```python
await client.post(callback_url, {
    "status": "COMPLETED",
    "ingestion_id": ...,
    "total_records": ...
})
```
Why this matters?
- Pimcore can:
    - finalize transactions
    - release locks
    - update ingestion status
- No guessing based on last chunk number

#### **Logging & observability**
Implemented:
- Structured logs
- Rotating logs
- Separate debug/info/error
- Deterministic log directories
- Production-safe logging
This makes:
- Debugging ingestion issues practical
- Auditing possible
- Post-mortems feasible

## Sequence Diagram 
Below is a runtime sequence for a complete ingestion. 
```bash
sequenceDiagram
    participant Client
    participant API as FastAPI API
    participant Controller
    participant Service as JsonIngestionService
    participant Pimcore

    Client->>API: POST /api/ingest
    API->>Controller: validate request
    Controller->>Service: start ingestion (background task)
    API-->>Client: 200 STARTED + ingestion_id

    loop For each record
        Service->>Service: stream record (ijson)
        Service->>Service: accumulate chunk
        Service->>Pimcore: POST chunk (records, checksum, chunk_id)
        Pimcore-->>Service: ACK / NACK
        alt NACK
            Service->>Service: retry chunk (max 3)
        end
    end

    Service->>Pimcore: POST status=COMPLETED
    Pimcore-->>Service: ACK COMPLETED
```










What happens today (exact behavior)
Scenario

FastAPI ingestion service is dockerized

It starts streaming JSON

It sends chunks 0…N to PIM Core

PIM Core successfully persists those chunks

FastAPI container crashes/restarts

PIM Core re-triggers ingestion with:

same file_path

same data

FastAPI generates a new ingestion_id

Starts streaming from record 0 again

What breaks?
❌ Problem 1: Ingestion ID changes
ingestion_id = str(uuid.uuid4())


New ingestion_id = PIM Core sees this as a brand-new ingestion

Chunk IDs differ → duplicate detection fails

❌ Problem 2: FastAPI has no state

No persisted:

last sent chunk number

last acknowledged chunk

last byte offset / record index

Restart = total amnesia

❌ Problem 3: PIM Core validator only protects within one ingestion_id
self.last_chunk_number[ingestion_id]


New ingestion_id → validator resets

PIM Core accepts duplicate data

DB ends up with duplicated rows

❌ Verdict

Your current system is NOT restart-safe and NOT exactly-once.

It is at-most-once per container lifetime.