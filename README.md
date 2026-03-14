# anaplan-orm

![CI Pipeline](https://github.com/valdal14/anaplan-orm/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.10%2B-blue?style=flat&logo=python)
![License](https://img.shields.io/badge/License-MIT-green)

A lightweight Python 3 library that abstracts the Anaplan API into an Object-Relational Mapper (ORM).

## Current Status
🚀 **Active Beta** 🚀
Core data transformation, parsing engine, and Anaplan chunked API client are complete.

## 🌟 Features

* **Pydantic Data Ingestion:** Validates and maps Python objects to Anaplan models effortlessly.
* **Enterprise Security:** Supports standard Basic Authentication and Anaplan's proprietary RSA-SHA512 Certificate-based Authentication (mTLS).
* **Resilient Networking:** Built-in exponential backoff and automated retries to protect against dropped packets and network blips.
* **Massive Payloads:** Automatically handles chunked file uploads for multi-megabyte/gigabyte datasets without memory crashes.
* **Smart Polling:** Asynchronous process execution with configurable, patient polling for long-running database transactions.

---

## 🔐 Authentication

`anaplan-orm` uses a decoupled authentication strategy, allowing you to easily swap between development and production security standards.

### 1. Basic Authentication
Ideal for development and sandbox testing.

```python
from anaplan_orm.client import AnaplanClient
from anaplan_orm.authenticator import BasicAuthenticator

auth = BasicAuthenticator("your_email@company.com", "your_password")
client = AnaplanClient(authenticator=auth)
```

### 2. Certificate-Based Authentication (Enterprise Standard)

For production environments, Anaplan requires a custom RSA-SHA512 signature. The CertificateAuthenticator handles this cryptographic handshake automatically.

Note: The library expects a .pem file containing both your private key and public certificate. If your enterprise issues a .p12 keystore, you can extract it using your terminal:

```bash
openssl pkcs12 -in keystore.p12 -out certificate.pem
```

```python
from anaplan_orm.client import AnaplanClient
from anaplan_orm.authenticator import CertificateAuthenticator

# 1. Initialize the Certificate Authenticator
auth = CertificateAuthenticator(
    cert_path="path/to/your/certificate.pem",
    # Omit if your private key is unencrypted
    cert_password="your_secure_password", 
    # Set to False if you need to bypass a corporate proxy
    verify_ssl=True
)

# 2. Inject it into the Anaplan Client
client = AnaplanClient(authenticator=auth)

# 3. Execute a request
status = client.ping()
```

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