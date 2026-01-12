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
This command will run a mock pim-core callback url and this particular url is used by the microservice to dump all the streamed data.