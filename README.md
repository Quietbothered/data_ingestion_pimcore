# data_ingestion_pimcore
This repo holds the code for fast-api micro service for data ingestion from excel and json files and help improve the performance for data ingestion in a pim-core project

## My goal
My goal is to design a micro-service that is 
- crash resistence 
- restart-safe
- network-fault tolerant 
- chunk-exactly-once
- externally ACK-driven
- resume ingestion mechanism
- re-ingestion mechanism

This is **not a best-effort ingestion service** — correctness is prioritized over speed.

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
├── ingestion_state_data
│   └── ingestion_state.db
├── logs
│   ├── debug
│   ├── error
│   └── info
├── main.py
├── schemas
│   ├── request_model.py
│   └── response_model.py
├── services
│   ├── data_integrity_manager.py
│   ├── excel_reader.py
│   ├── ingestion_state_store.py
│   ├── json_reader.py
├── utils
│   ├── error_messages.py
│   ├── field_descriptions.py
│   ├── generate_ingestion_id.py
│   ├── get_project_dir.py
│   ├── json_decimal_encoder.py
│   ├── logger.py
│   ├── logger_info_messages.py
│   ├── log_initializer.py
│   └── logs_re_namer.py
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

## Scenario: FastAPI crashes AFTER COMPLETED is sent
```python
resp = await client.post(COMPLETED)
if ack:
    self.state_store.mark_completed(ingestion_id)
```
This ensures:
- No “false completed” locally
- PIM Core and FastAPI agree on final state
One thing to understand (not a bug, just reality) : I am chunk-exactly-once, not record-exactly-once.


## Implemented deterministic ingestion
This is done to handle fault cases like
- In cases where the fast-api microservice server experience some fault and the service gets restarted for some reason 
    - In this case my microservice should be able to resume the data streaming from the last known chunk that was sent and was successfully acknowledged by pim-core callback url
### Steps that I took make sure that my micro service is fault tolerant 
- Defined a deterministic ingestion identity
    - ```python
        raw = f"{file_path}|{file_type}"
        ingestion_id = sha256(raw)
        ```
    - Why it matters
        - Same file → same ingestion
        - Enables:
            - resume after crash
            - idempotent re-ingestion
            - prevention of duplicate processing
    - Fault tolerance achieved : 
        - Service can restart and still know which ingestion it was processing.
- Decoupled API request lifecycle from ingestion execution
    - Used BackgroundTasks to run ingestion asynchronously.
    - Why it matters:
        - API remains responsive
        - Ingestion can be retried or resumed independently
    - Fault tolerance achieved
        - API crash ≠ ingestion logic corruption
- Introduced a persistent ingestion state store
    - Created IngestionStateStore backed by SQLite
    - Persisted:
        - ingestion_id
        - last_chunk
        - total_records
        - status
    - ```python
            CREATE TABLE ingestion_state (
                ingestion_id TEXT PRIMARY KEY,
                last_chunk INTEGER,
                total_records INTEGER,
                status TEXT
            )
        ```
    - Why it matters
        - State survives:
            - process crashes
            - container restarts
            - redeployments
    - Fault tolerance achieved
        - No reliance on in-memory state
- Persisted progress only after external ACK
    - Updated state store only after PIM-core ACK.
    - ```python
        self.state_store.update_chunk(ingestion_id, chunk_number, total_records)
        ```
    - Why it matters
        - Prevents “false progress”
        - Guarantees:
            - at-least-once delivery
            - no skipped chunks
        - Fault tolerance achieved
            - Partial network failures do not corrupt progress
- Implemented chunk-level idempotency
    - Each chunk has a deterministic identity:
    - ```python
        chunk_id = f"{ingestion_id}:{chunk_number}"
        ```
    - Why it matters
        - Duplicate sends are harmless
        - Retries do not cause duplication
    - Fault tolerance achieved
        - Safe retries
        - Safe replay after crash
- Added checksum-based data integrity validation
    - Canonical JSON serialization
    - SHA-256 checksum per chunk
    - ```python
        checksum = sha256(canonical_dumps(records))
        ```
    - Why it matters
        - Detects:
            - partial transmissions
            - corrupted payloads
            - proxy truncation
    - Fault tolerance achieved
        - Silent corruption is impossible
- Enforced strict chunk ordering
    - PIM-core rejects out-of-order chunks
    - Producer retries correctly
    - ```python
        if chunk_number != last + 1:
        return OUT_OF_ORDER
        ```
    - Why it matters
        - Prevents race conditions
        - Guarantees deterministic replay
    - Fault tolerance achieved
        - No ordering bugs after restarts
- Designed resume logic on service startup
    - On start of ingestion:
    - ```python
        last_chunk = state_store.get_last_chunk(ingestion_id)
        chunk_number = last_chunk + 1
        ```
    - Why it matters
        - Skips already ACKed chunks
        - Continues exactly where it stopped
    - Fault tolerance achieved
        - Crash-safe streaming
- Persisted and resumed total_records
    - Stored total_records in DB
    - Reloaded it on restart
    - ```python
        self.total_records = state_store.get_total_records(ingestion_id)
        ```
    - Why it matters
        - Accurate metrics across restarts
        - No double-counting
    - Fault tolerance achieved
        - Correct completion reporting
- Ensured storage availability at runtime
    - Created DB directory automatically if missing
    - ```python
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        ```
    - Why it matters
        - First run works
        - Docker restarts work
        - No manual setup
    - Fault tolerance achieved
        - No startup-time failures
- Used streaming I/O instead of loading files into memory
    - Used ijson + fsspec
    - ```python
        for record in ijson.items(file, "item"):
        ```
    - Why it matters
        - Large files don’t crash the process
        - Memory usage is bounded
    - Fault tolerance achieved
        - Stable under large workloads
- Explicit completion handshake
    - Sent a final COMPLETED event
    - Marked ingestion complete only after ACK
    - ```python
        self.state_store.mark_completed(ingestion_id)
        ```
    - Why it matters
        - Prevents premature completion
        - Enables downstream consistency
    - Fault tolerance achieved
        - Clean termination semantics
### My ingestion_id is deterministic
```python
def generate_ingestion_id(file_path: str, file_type: str) -> str:
    raw = f"{file_path}|{file_type}"
    return hashlib.sha256(raw.encode()).hexdigest()
```
This means:
- Same file_path + same file_type = same ingestion_id
So when I ingest the same file again:
- I am continuing the same ingestion
- Not starting a new one
My resume logic is doing exactly what I intended to
In ```stream_and_push```:
```python
self.total_records = self.state_store.get_total_records(ingestion_id)
```
So:
- If the file was already ingested once
- total_records already exists in DB
- I resume counting from that number

Then during streaming:
```python
self.total_records += 1
```
So totals accumulate, not reset. This is intentional behavior, not an accident.

## Implemented Data Re-Ingestion & Ingestion Versioning
### Issue 1: Chunk mismatch / out-of-order errors
- If the pim-core callback service was restarted, it lost its in-memory chunk state.
- The FastAPI service resumed ingestion from a higher chunk number (because state was persisted in SQLite).
- Pim-core expected chunk 0, but received chunk N → OUT_OF_ORDER_CHUNK.
### Issue 2: Re-ingestion from the same file was logically ambiguous
- Re-ingesting the same file reused the same ingestion_id.
- Chunk numbers, total record counts, and progress were carried over from a previous run.
- This caused:
    - Incorrect total_records
    - Incorrect chunk_number
    - Confusing semantics (is this a resume or a new ingestion?)

### Correct Mental Model (After Fix)
- A file can be ingested multiple times.
- Each ingestion is a separate execution.
- Resume is allowed only within the same execution.

So I separate identities:
```python
file_id        → deterministic
ingestion_id   → versioned (execution-specific)
```

### What Was Implemented (Ingestion Versioning)
#### Deterministic File Identity
- Stable
- Identifies what is being ingested
- Never changes


```python
ingestion_id = sha256(file_id + version)
```

```python
if request.re_ingestion:
    version = str(int(time.time() * 1000))
else:
    version = "resume"
```

#### Resume Semantics (Correct & Intentional)
When re_ingestion = false:
- Same ingestion_id is reused
- SQLite state is reused
- Chunk numbers resume correctly
- Total records resume correctly

This supports:
- Service restarts
- Network failures
- Partial ingestion recovery

#### Re-Ingestion Semantics (New Behavior)
When re_ingestion = true:
- A new ingestion_id is generated
- SQLite has no existing state for that ingestion_id
- Chunk numbering starts from 0
- total_records starts from 0
- Pim-core treats it as a completely new ingestion

This enables:
- Backfills
- Reprocessing corrupted data
- Re-running ingestion after logic changes

This is the request json that pim-core will have to send to fast-api data ingestion micro-service
```json
{
  "file_path": "/home/aditya/github/data_ingestion_pimcore/tests/test_data/PIM_PRODIDSKU_20251222183200000_001.json",
  "file_type": "json",
  "callback_url": "http://127.0.0.1:9000/callback",
  "chunk_size_by_records": 10,
  "chunk_size_by_memory": 0,
  "re_ingestion":true
}
```

## Added test cases for the microservice
### How to run test cases
First you go into this directory using the command below
```bash
cd /home/aditya/github/data_ingestion_pimcore
```
Then you run this command to run all the test cases for this micro-service
```bash
pytest tests/unit_tests/main_test_orchestration.py
```
### Why do these test cases exists
This micro-service is not a best-effort ingestion system.
It is a correctness-first, failure-aware ingestion engine designed to operate reliably under real-world failure conditions such as crashes, restarts, partial network outages, and external system inconsistencies.

Because of this, tests are not optional, and they are not written merely to improve code coverage.

They exist to prove invariants.

### What These Tests Are Explicitly Designed to Guarantee
Each test exists to enforce a system contract that must remain true even as the code evolves.

#### **Crash Safety Guarantees**
The ingestion pipeline must remain correct if:
- The FastAPI process crashes
- The container restarts
- The machine reboots

Tests validate that:
- Progress is persisted
- No false completion occurs
- Resume logic starts from the correct chunk
- No already-ACKed chunk is resent incorrectly

#### **Restart & Resume Determinism**
This system relies on persistent ingestion state rather than in-memory assumptions.

Tests ensure:
- Resume starts from last_chunk + 1
- total_records is restored correctly
- Resume and re-ingestion semantics are not conflated
- Versioned ingestion behaves predictably

A single regression here causes:
- Duplicate data
- Skipped data
- Inconsistent Pimcore state

#### **Exactly-Once Chunk Semantics (By Design)**
This service is chunk-exactly-once, not record-exactly-once.

Tests assert that:
- Each chunk has a deterministic identity
- Duplicate chunk sends are harmless
- Out-of-order chunks are rejected
- Retry logic does not break ordering guarantees


