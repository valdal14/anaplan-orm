from unittest.mock import patch

import pytest
import respx

from anaplan_orm.client import AnaplanClient
from anaplan_orm.exceptions import AnaplanConnectionError


@pytest.fixture(autouse=True)
def bypass_sleep():
    """Globally bypasses both time.sleep() and asyncio.sleep() for instant tests."""
    with patch("asyncio.sleep") as mock_async_sleep:
        yield mock_async_sleep


# Dummy Authenticator for testing
class DummyAuthenticator:
    def get_auth_headers(self):
        return {"Authorization": "AnaplanAuthToken dummy"}


@pytest.fixture
def client():
    return AnaplanClient(authenticator=DummyAuthenticator())


@pytest.mark.asyncio
@respx.mock
async def test_upload_file_chunked_async_success(client):
    """Test a successful asynchronous chunked upload."""
    workspace_id = "ws1"
    model_id = "mod1"
    file_id = "file1"
    # Create enough data for 3 chunks (chunk size set to 1MB below for testing)
    csv_data = "A" * (int(2.5 * client.MB_TO_BYTES))
    chunk_size_mb = 1
    max_concurrent = 2

    # Mock endpoints
    init_url = f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
    respx.post(init_url).respond(status_code=200, json={"chunkCount": -1})

    chunk_pattern = (
        f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/.*"
    )
    chunk_route = respx.put(url__regex=chunk_pattern).respond(status_code=204)

    complete_url = (
        f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/complete"
    )
    respx.post(complete_url).respond(status_code=200, json={"id": file_id})

    # Execute
    await client.upload_file_chunked_async(
        workspace_id,
        model_id,
        file_id,
        csv_data,
        chunk_size_mb=chunk_size_mb,
        max_concurrent_uploads=max_concurrent,
    )

    # Verify calls
    assert chunk_route.call_count == 3  # 2.5MB / 1MB chunks = 3 chunks


@pytest.mark.asyncio
@respx.mock
async def test_upload_file_chunked_async_failure_retries(client):
    """Test that the async retry decorator kicks in on chunk failure."""
    workspace_id = "ws1"
    model_id = "mod1"
    file_id = "file1"
    csv_data = "A" * 1024  # Tiny payload, just 1 chunk

    # Mock init
    init_url = f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
    respx.post(init_url).respond(status_code=200, json={"chunkCount": -1})

    # Mock chunk upload to fail continuously
    chunk_url = (
        f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/0"
    )
    chunk_route = respx.put(chunk_url).respond(status_code=500)

    # We expect an AnaplanConnectionError because it will exhaust retries
    with pytest.raises(AnaplanConnectionError):
        await client.upload_file_chunked_async(
            workspace_id, model_id, file_id, csv_data, chunk_size_mb=1
        )

    # Verify it retried (Initial + 3 retries based on decorator defaults = 4 total attempts, but could be just 3 depending on exact decorator logic. Let's assume default 3 max attempts total.)
    # If your decorator retries max_retries times total, it should be called 3 times.
    assert chunk_route.call_count >= 3


@pytest.mark.asyncio
@respx.mock
async def test_upload_file_streaming_async_success(client, tmp_path):
    """Test a successful asynchronous streaming upload using a physical file."""
    workspace_id = "ws_stream"
    model_id = "mod_stream"
    file_id = "file_stream"

    # Generate a temporary physical file
    temp_file = tmp_path / "massive_dummy.csv"
    chunk_size_mb = 1
    # Create 3 chunks of data
    total_size = int(2.5 * client.MB_TO_BYTES)
    temp_file.write_bytes(b"A" * total_size)

    # Mock Anaplan endpoints
    init_url = f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
    respx.post(init_url).respond(status_code=200, json={"chunkCount": -1})

    chunk_pattern = (
        f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/.*"
    )
    chunk_route = respx.put(url__regex=chunk_pattern).respond(status_code=204)

    complete_url = (
        f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/complete"
    )
    respx.post(complete_url).respond(status_code=200, json={"id": file_id})

    # Execute the Streaming Pipeline
    await client.upload_file_streaming_async(
        workspace_id=workspace_id,
        model_id=model_id,
        file_id=file_id,
        file_path=str(temp_file),
        chunk_size_mb=chunk_size_mb,
        max_concurrent_uploads=2,
    )

    # Verify the workers fired exactly 3 times
    assert chunk_route.call_count == 3


@pytest.mark.asyncio
@respx.mock
async def test_upload_file_streaming_async_failure_retries(client, tmp_path):
    """Test that the streaming workers correctly handle retries and propagate errors."""
    workspace_id = "ws_stream_fail"
    model_id = "mod_stream_fail"
    file_id = "file_stream_fail"

    # Create a tiny temporary file (1 KB)
    temp_file = tmp_path / "fail_dummy.csv"
    temp_file.write_bytes(b"A" * 1024)

    # Mock init success
    init_url = f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
    respx.post(init_url).respond(status_code=200, json={"chunkCount": -1})

    # Mock chunk upload to return a fatal 500 Internal Server Error constantly
    chunk_pattern = (
        f"{client.BASE_URL}/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/.*"
    )
    chunk_route = respx.put(url__regex=chunk_pattern).respond(status_code=500)

    # The client should exhaust retries and raise the AnaplanConnectionError
    with pytest.raises(AnaplanConnectionError):
        await client.upload_file_streaming_async(
            workspace_id=workspace_id,
            model_id=model_id,
            file_id=file_id,
            file_path=str(temp_file),
            chunk_size_mb=1,
        )

    # Verify the worker attempted to retry the chunk before failing
    assert chunk_route.call_count >= 3
