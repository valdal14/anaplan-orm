# anaplan-orm

![CI Pipeline](https://github.com/valdal14/anaplan-orm/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=flat&logo=python)
![License](https://img.shields.io/badge/License-MIT-green)

A lightweight Python 3 library that abstracts the Anaplan API into an Object-Relational Mapper (ORM).

## Current Status
🚀 **Active Beta** 🚀
Core data transformation, parsing engine, and Anaplan chunked API client are complete.

## Features
* **Data Ingestion:** Extracts XML string payloads directly into strictly typed Pydantic V2 models.
* **Data Serialization:** Serializes Pydantic models into Anaplan-ready CSV payloads with robust alias and dynamic separator support.
* **API Client:** Handles Anaplan Basic Authentication with stateful token caching and automatic renewal.
* **Large File Streaming:** Automatically slices and streams massive datasets via Anaplan's Chunked Upload API to prevent memory crashes.
* **Process Automation:** Triggers Anaplan Import Actions and natively polls the database engine for completion status.

---

## Quick Start: XML Parsing & Data Upload
The `anaplan-orm` is designed to take raw XML strings (e.g., from MuleSoft or data pipeline payloads), validate them into Python objects, and stream them directly into Anaplan.

### 1. Define Your Model
Map your Anaplan target columns to Python using Pydantic fields. The `alias` parameter bridges the gap between external uppercase XML tags and internal Python `snake_case` variables. 

```python
from pydantic import Field
from anaplan_orm.models import AnaplanModel

class Developer(AnaplanModel):
    dev_id: int = Field(alias="DEV_ID")
    dev_name: str = Field(alias="DEV_NAME")
    dev_age: int = Field(alias="DEV_AGE")
    dev_location: str = Field(alias="DEV_LOCATION")
```

### 2. Parse, Serialize, and Upload
Use the XMLStringParser to ingest your XML string payload, then use the AnaplanClient to stream the chunked data to Anaplan.

```python
import os
import json
from anaplan_orm.parsers import XMLStringParser
from anaplan_orm.client import AnaplanClient, BasicAuthenticator

# 1. Your incoming XML string payload
xml_string = """
<AnaplanExport>
    <Row>
        <DEV_ID>1001</DEV_ID>
        <DEV_NAME>Ada Lovelace</DEV_NAME>
        <DEV_AGE>36</DEV_AGE>
        <DEV_LOCATION>London</DEV_LOCATION>
    </Row>
</AnaplanExport>
"""

def run_pipeline():
    # 2. Parse and Validate the data using the ORM
    parser = XMLStringParser()
    developers = Developer.from_payload(payload=xml_string, parser=parser)
    
    # 3. Serialize to Anaplan-ready CSV (using a Pipe separator)
    csv_data = Developer.to_csv(developers, separator="|")

    # 4. Authenticate with Anaplan
    auth = BasicAuthenticator(
        email="ANAPLAN_EMAIL", 
        pwd="ANAPLAN_PASSWORD"
    )

    client = AnaplanClient(authenticator=auth)

    # 5. Stream the file chunks safely
    client.upload_file_chunked(
        workspace_id="YOUR_WORKSPACE_ID", 
        model_id="YOUR_MODEL_ID", 
        file_id="YOUR_FILE_ID", 
        csv_data=csv_data,
        chunk_size_mb=10
    )

    # 6. Execute the Import Process
    task_id = client.execute_process(
        workspace_id="YOUR_WORKSPACE_ID", 
        model_id="YOUR_MODEL_ID", 
        process_id="YOUR_PROCESS_ID"
    )
    
    # 7. Actively poll the database for success/failure
    status = client.wait_for_process_completion(
        workspace_id="YOUR_WORKSPACE_ID", 
        model_id="YOUR_MODEL_ID", 
        process_id="YOUR_PROCESS_ID", 
        task_id=task_id
    )

if __name__ == "__main__":
    run_pipeline()
```