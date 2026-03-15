import time
from unittest.mock import MagicMock, Mock, mock_open, patch

import httpx
import pytest

from anaplan_orm.authenticator import Authenticator, BasicAuthenticator, CertificateAuthenticator
from anaplan_orm.client import AnaplanClient
from anaplan_orm.exceptions import AnaplanConnectionError


@pytest.fixture(autouse=True)
def bypass_sleep():
    """Globally bypasses time.sleep() for all tests in this file so they run instantly."""
    with patch("time.sleep") as mock_sleep:
        yield mock_sleep


# Create our Dummy Authenticator
class DummyAuthenticator(Authenticator):
    def __init__(self):
        super().__init__()  # Initialize the base class token caching

    def authenticate(self) -> None:
        """Fakes the network request and instantly provisions a dummy token."""
        self._cached_token = "FakeTestToken"
        self._token_timestamp = time.time()


def test_ping_returns_status_code():
    """Test that the client correctly returns the status code from the HTTP response."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    # Act: Hijack the httpx 'get' method
    with patch.object(httpx.Client, "get") as mock_get:
        fake_response = Mock()
        fake_response.status_code = 401
        mock_get.return_value = fake_response

        status = client.ping()

    # Assert: Verify ping method returned the mocked 401
    assert status == 401
    # Verify the client actually tried to pass the correct headers to httpx
    mock_get.assert_called_once_with(
        "/users/me",
        headers={
            "Authorization": "AnaplanAuthToken FakeTestToken",
            "Content-Type": "application/json",
        },
    )


def test_ping_raises_custom_connection_error():
    """Test that httpx network failures are caught and raised as AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with patch.object(httpx.Client, "get") as mock_get:
        # Force httpx to simulate a complete network failure
        mock_get.side_effect = httpx.RequestError("DNS resolution failed", request=Mock())

        # Assert that our custom exception is raised
        with pytest.raises(AnaplanConnectionError) as exc_info:
            client.ping()

        # Verify our custom error message is inside the exception
        assert "Network error communicating with Anaplan" in str(exc_info.value)


def test_upload_file_success():
    """Test that the client correctly formats the URL, headers, and byte payload for file upload."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    # Arrange: Setup dummy data
    workspace_id = "ws_123"
    model_id = "mod_456"
    file_id = "file_789"
    csv_payload = "EmployeeID,Salary\n101,75000"

    # Act: Hijack the httpx 'put' method
    with patch.object(httpx.Client, "put") as mock_put:
        # Create a fake successful HTTP response
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        mock_put.return_value = fake_response

        # Execute our method
        client.upload_file(workspace_id, model_id, file_id, csv_payload)

        # Assert: Verify httpx.put was called with the exact expected arguments
        expected_url = f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
        expected_headers = {
            "Authorization": "AnaplanAuthToken FakeTestToken",
            "Content-Type": "application/octet-stream",
        }

        mock_put.assert_called_once_with(
            expected_url,
            headers=expected_headers,
            content=csv_payload.encode("utf-8"),  # Verifying the byte conversion!
        )


def test_upload_file_raises_custom_error():
    """Test that failed uploads raise our custom AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with patch.object(httpx.Client, "put") as mock_put:
        # Force httpx to simulate a 404 Not Found or 500 Server Error
        mock_put.side_effect = httpx.HTTPError("404 Client Error: Not Found for url")

        with pytest.raises(AnaplanConnectionError) as exc_info:
            client.upload_file("w", "m", "f", "data")

        assert "Failed to upload file to Anaplan" in str(exc_info.value)


def test_execute_process_success():
    """Test that the client correctly formats the URL, headers, and JSON payload for a process."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    # Arrange: Setup dummy data
    workspace_id = "ws_123"
    model_id = "mod_456"
    process_id = "imp_789"

    # Act: Hijack the httpx 'post' method
    with patch.object(httpx.Client, "post") as mock_post:
        # Create a fake successful HTTP response returning a mock Task ID
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {"task": {"taskId": "task_abc123"}}
        mock_post.return_value = fake_response

        # Execute our method
        result_task_id = client.execute_process(workspace_id, model_id, process_id)

        # 3. Assert: Verify the method returned the exact string from the JSON
        assert result_task_id == "task_abc123"

        # Verify httpx.post was called with the correct payload and headers
        expected_url = f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks"
        expected_headers = {
            "Authorization": "AnaplanAuthToken FakeTestToken",
            "Content-Type": "application/json",
        }

        mock_post.assert_called_once_with(
            expected_url, headers=expected_headers, json={"localeName": "en_US"}
        )


def test_execute_process_raises_custom_error():
    """Test that failed process raise our custom AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with patch.object(httpx.Client, "post") as mock_post:
        # Force httpx to simulate a 400 Bad Request or 500 Server Error
        mock_post.side_effect = httpx.HTTPError("400 Client Error: Bad Request for url")

        with pytest.raises(AnaplanConnectionError) as exc_info:
            client.execute_process("w", "m", "i")

        assert "Failed to execute process in Anaplan" in str(exc_info.value)


def test_get_process_task_status_success():
    """Test that the client correctly formats the URL and headers to fetch task status."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    # Arrange: Setup dummy IDs
    workspace_id = "ws_123"
    model_id = "mod_456"
    process_id = "proc_789"
    task_id = "task_abc"

    # Act: Hijack the httpx 'get' method
    with patch.object(httpx.Client, "get") as mock_get:
        # Create a fake successful HTTP response returning a mock task state
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "task": {"taskId": "task_abc", "taskState": "COMPLETE", "successful": True}
        }
        mock_get.return_value = fake_response

        # Execute our method
        result_task = client._get_process_task_status(workspace_id, model_id, process_id, task_id)

        # Assert: Verify the method returned the exact inner 'task' dictionary
        assert result_task["taskState"] == "COMPLETE"
        assert result_task["successful"] is True

        # Verify httpx.get was called with the correct URL and headers keyword argument
        expected_url = (
            f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks/{task_id}"
        )
        expected_headers = {
            "Authorization": "AnaplanAuthToken FakeTestToken",
            "Content-Type": "application/json",
        }

        mock_get.assert_called_once_with(expected_url, headers=expected_headers)


def test_get_process_task_status_raises_custom_error():
    """Test that failed task status polls raise our custom AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with patch.object(httpx.Client, "get") as mock_get:
        # Force httpx to simulate a 404 Not Found
        mock_get.side_effect = httpx.HTTPError("404 Client Error: Not Found for url")

        with pytest.raises(AnaplanConnectionError) as exc_info:
            client._get_process_task_status("w", "m", "p", "t")

        assert "Failed to fetch task status from Anaplan" in str(exc_info.value)


def test_wait_for_process_completion_success_first_try():
    """Test that the waiter returns immediately if the task is already COMPLETE."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with patch.object(client, "_get_process_task_status") as mock_status:
        # Mock Anaplan returning a success on the very first check
        mock_status.return_value = {"taskState": "COMPLETE", "result": {"successful": True}}

        result = client.wait_for_process_completion("w", "m", "p", "t", retry=3)

        assert result["taskState"] == "COMPLETE"
        assert mock_status.call_count == 1


def test_wait_for_process_completion_recursive_success():
    """Test that the waiter sleeps, decrements the retry counter, and eventually succeeds."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with (
        patch.object(client, "_get_process_task_status") as mock_status,
        patch.object(client, "_process_to_sleep") as mock_sleep,
    ):
        # Mock Anaplan returning IN_PROGRESS on the first try, then COMPLETE on the second
        mock_status.side_effect = [
            {"taskState": "IN_PROGRESS"},
            {"taskState": "COMPLETE", "result": {"successful": True}},
        ]

        result = client.wait_for_process_completion("w", "m", "p", "t", retry=3)

        assert result["taskState"] == "COMPLETE"
        assert mock_status.call_count == 2
        # It should have slept exactly once
        assert mock_sleep.call_count == 1


def test_wait_for_process_completion_times_out():
    """Test that the script raises a connection error when it runs out of retries."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with pytest.raises(AnaplanConnectionError) as exc_info:
        # We pass retry=0 to instantly trigger the timeout base case
        client.wait_for_process_completion("w", "m", "p", "t", retry=0)

    assert "did not complete within the assigned time" in str(exc_info.value)


def test_authenticator_caches_token_successfully():
    """Test that rapid consecutive calls only hit the Anaplan API once."""
    auth = get_basic_authenticator(is_mocked_authenticator=False)

    with patch("httpx.post") as mock_post:
        # Arrange: Setup a fake successful Anaplan response
        fake_response = MagicMock()
        fake_response.json.return_value = {
            "status": "SUCCESS",
            "tokenInfo": {"tokenValue": "First_Token_ABC"},
        }
        mock_post.return_value = fake_response

        # Act: First call should hit the network
        headers_one = auth.get_auth_headers()

        # Act: Second call immediately after should use the cache
        headers_two = auth.get_auth_headers()

        # Assert
        assert headers_one["Authorization"] == "AnaplanAuthToken First_Token_ABC"
        assert headers_two["Authorization"] == "AnaplanAuthToken First_Token_ABC"

        # Ensure we only sent ONE network request
        assert mock_post.call_count == 1


def test_authenticator_refreshes_expired_token():
    """Test that the authenticator fetches a new token after 30 minutes."""
    auth = get_basic_authenticator(is_mocked_authenticator=False)

    # We patch both the network call AND the system clock
    with patch("httpx.post") as mock_post, patch("time.time") as mock_time:
        # Arrange: Create two different responses for the two network calls
        fake_response_1 = MagicMock()
        fake_response_1.json.return_value = {
            "status": "SUCCESS",
            "tokenInfo": {"tokenValue": "First_Token_ABC"},
        }

        fake_response_2 = MagicMock()
        fake_response_2.json.return_value = {
            "status": "SUCCESS",
            "tokenInfo": {"tokenValue": "New_Token_XYZ"},
        }

        # side_effect allows us to return different things on the 1st vs 2nd call
        mock_post.side_effect = [fake_response_1, fake_response_2]

        # Act: First call at an arbitrary start time
        mock_time.return_value = 100.0
        headers_one = auth.get_auth_headers()

        # Act: Fast forward time by 1801 seconds (just past the 30 min limit)
        mock_time.return_value = 1901.0
        headers_two = auth.get_auth_headers()

        # Assert
        assert headers_one["Authorization"] == "AnaplanAuthToken First_Token_ABC"
        assert headers_two["Authorization"] == "AnaplanAuthToken New_Token_XYZ"

        # Ensure it realized the cache expired and made a SECOND network request
        assert mock_post.call_count == 2


def test_upload_file_chunked_success_single_chunk():
    """Test that a small file correctly triggers the 3-step chunked process exactly once."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    workspace_id = "ws_1"
    model_id = "mod_1"
    file_id = "file_1"
    csv_payload = "EmployeeID,Salary\n101,75000"

    with (
        patch.object(httpx.Client, "post") as mock_post,
        patch.object(httpx.Client, "put") as mock_put,
    ):
        # Setup fake successful responses
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        mock_post.return_value = fake_response
        mock_put.return_value = fake_response

        # Act
        client.upload_file_chunked(workspace_id, model_id, file_id, csv_payload, chunk_size_mb=1)

        # Assert: POST should be called twice (Init and Complete)
        assert mock_post.call_count == 2
        # Assert: PUT should be called exactly once for this tiny string
        assert mock_put.call_count == 1

        # Verify the Init POST payload
        mock_post.assert_any_call(
            f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}",
            headers={
                "Authorization": "AnaplanAuthToken FakeTestToken",
                "Content-Type": "application/json",
            },
            json={"chunkCount": -1},
        )


def test_upload_file_chunked_multiple_chunks():
    """Test that a large file is sliced into the correct number of chunks."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    # Create a fake string that is exactly 2.5 MB in size
    # 1 MB = 1048576 bytes. 2.5 MB = 2621440 bytes
    large_csv_payload = "A" * 2621440

    with (
        patch.object(httpx.Client, "post") as mock_post,
        patch.object(httpx.Client, "put") as mock_put,
    ):
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        mock_post.return_value = fake_response
        mock_put.return_value = fake_response

        # Act: Request 1 MB chunks for a 2.5 MB file
        client.upload_file_chunked("w", "m", "f", large_csv_payload, chunk_size_mb=1)

        # Assert: It should take exactly 3 chunks (1MB, 1MB, 0.5MB)
        assert mock_put.call_count == 3

        # Assert: Ensure chunk 0, 1, and 2 were called in the URLs
        put_calls = mock_put.call_args_list
        # Inspecting the first positional argument (URL) of the first call
        assert "chunks/0" in put_calls[0][0][0]
        assert "chunks/1" in put_calls[1][0][0]
        assert "chunks/2" in put_calls[2][0][0]


def test_upload_file_chunked_raises_custom_error():
    """Test that network failures during the chunked upload raise AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    with patch.object(httpx.Client, "post") as mock_post:
        # Force the initialization to fail
        mock_post.side_effect = httpx.HTTPError("500 Server Error")

        with pytest.raises(AnaplanConnectionError) as exc_info:
            client.upload_file_chunked("w", "m", "f", "data")

        assert "Failed during chunked upload process" in str(exc_info.value)


# NOTE: Helpers Methods
def get_basic_authenticator(is_mocked_authenticator: bool = True) -> Authenticator:
    if is_mocked_authenticator:
        return DummyAuthenticator()
    else:
        return BasicAuthenticator("test@company.com", "pwd123")


# NOTE: Certificate Authentication Tests ########################################################################

# A fake PEM file string to trick the file reader
FAKE_PEM = b"""-----BEGIN PRIVATE KEY-----
FakeKeyData
-----END PRIVATE KEY-----
-----BEGIN CERTIFICATE-----
FakeCertData
-----END CERTIFICATE-----"""


@patch("anaplan_orm.authenticator.httpx.post")
@patch("anaplan_orm.authenticator.serialization.load_pem_private_key")
@patch("anaplan_orm.authenticator.os.urandom", return_value=b"fake_random_bytes")
@patch("builtins.open", new_callable=mock_open, read_data=FAKE_PEM)
def test_certificate_authenticator_success(mock_file, mock_urandom, mock_load_key, mock_post):
    """Test that CertificateAuthenticator correctly signs the payload and parses the token."""
    # Setup the cryptography mock
    mock_private_key = Mock()
    mock_private_key.sign.return_value = b"fake_signature"
    mock_load_key.return_value = mock_private_key

    # Setup the network mock
    mock_response = Mock()
    mock_response.json.return_value = {
        "status": "SUCCESS",
        "tokenInfo": {"tokenValue": "CertToken123"},
    }
    mock_post.return_value = mock_response

    # Execute
    auth = CertificateAuthenticator(
        cert_path="/fake/path/cert.pem", cert_password="mule", verify_ssl=False
    )
    auth.authenticate()

    # Verify the internal state updated correctly
    assert auth._cached_token == "CertToken123"

    # Verify the exact Anaplan cryptographic payload was sent
    call_kwargs = mock_post.call_args.kwargs
    assert call_kwargs["headers"]["Authorization"] == "CACertificate FakeCertData"
    # "ZmFrZV9yYW5kb21fYnl0ZXM=" is the base64 string for "fake_random_bytes"
    assert call_kwargs["json"]["encodedData"] == "ZmFrZV9yYW5kb21fYnl0ZXM="
    # "ZmFrZV9zaWduYXR1cmU=" is the base64 string for "fake_signature"
    assert call_kwargs["json"]["encodedSignedData"] == "ZmFrZV9zaWduYXR1cmU="


@patch("anaplan_orm.authenticator.httpx.post")
@patch("anaplan_orm.authenticator.serialization.load_pem_private_key")
@patch("builtins.open", new_callable=mock_open, read_data=FAKE_PEM)
def test_certificate_authenticator_failure(mock_file, mock_load_key, mock_post):
    """Test that CertificateAuthenticator handles Anaplan rejection properly."""
    mock_private_key = Mock()
    mock_private_key.sign.return_value = b"fake_signature"
    mock_load_key.return_value = mock_private_key

    mock_response = Mock()
    mock_response.json.return_value = {
        "status": "FAILURE",
        "statusMessage": "Invalid Certificate Signature",
    }
    mock_post.return_value = mock_response

    auth = CertificateAuthenticator(cert_path="/fake/path/cert.pem")

    with pytest.raises(AnaplanConnectionError) as exc_info:
        auth.authenticate()

    assert "Anaplan Auth Failed: Invalid Certificate Signature" in str(exc_info.value)


# NOTE: OUTBOUND PIPELINE TESTS #################################################################################


@patch("anaplan_orm.client.httpx.Client.post")
def test_execute_export_success(mock_post):
    """Test that execute_export triggers the Anaplan API and returns a task ID."""
    mock_response = Mock()
    mock_response.json.return_value = {"task": {"taskId": "export_task_999"}}
    mock_post.return_value = mock_response

    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    task_id = client.execute_export("workspace_id", "model_id", "export_id")

    assert task_id == "export_task_999"
    mock_post.assert_called_once()
    assert "/exports/export_id/tasks" in mock_post.call_args[0][0]


@patch("anaplan_orm.client.AnaplanClient._get_export_task_status")
def test_wait_for_export_completion_success(mock_status):
    """Test that the polling method successfully returns when the export completes."""
    # Mock the Anaplan response to instantly say "COMPLETE"
    mock_status.return_value = {"taskState": "COMPLETE", "result": {"successful": True}}

    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    result = client.wait_for_export_completion("w", "m", "e", "t")

    assert result["taskState"] == "COMPLETE"
    mock_status.assert_called_once()


@patch("anaplan_orm.client.AnaplanClient._get_download_chunk_count")
@patch("anaplan_orm.client.AnaplanClient._download_chunk")
def test_download_file_chunked_success(mock_download_chunk, mock_chunk_count):
    """Test that the download engine fetches the right chunk count and decodes them."""
    # Tell the script there are exactly 2 chunks to download
    mock_chunk_count.return_value = 2

    # Mock the raw bytes returned by each iteration of the loop
    mock_download_chunk.side_effect = [b"Row1,Data\n", b"Row2,MoreData"]

    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)

    # Execute the download engine
    final_csv_string = client.download_file_chunked("workspace_id", "model_id", "file_id")

    # Verify the bytes were concatenated and decoded correctly
    assert final_csv_string == "Row1,Data\nRow2,MoreData"

    # Verify the helpers were called the correct number of times
    assert mock_chunk_count.call_count == 1
    assert mock_download_chunk.call_count == 2


# NOTE: DECORATOR & RETRY TESTS #################################################################################


@patch("anaplan_orm.client.httpx.Client.post")
def test_retry_decorator_handles_401_token_expiry(mock_post):
    """Test that a 401 Unauthorized correctly triggers a token wipe and retry."""

    # 1. Setup the fake 401 Error
    mock_401_response = Mock()
    mock_401_response.status_code = 401
    error_401 = httpx.HTTPStatusError(
        message="Unauthorized", request=Mock(), response=mock_401_response
    )

    # 2. Setup the fake Success response
    mock_success_response = Mock()
    mock_success_response.json.return_value = {"task": {"taskId": "task_123"}}

    # 3. Tell the mock to fail on the first call, and succeed on the second
    mock_post.side_effect = [error_401, mock_success_response]

    # 4. Setup the Client with a fully mocked Authenticator
    mock_auth = Mock()
    mock_auth.get_auth_headers.return_value = {"Authorization": "AnaplanAuthToken Expired123"}
    client = AnaplanClient(authenticator=mock_auth)

    # 5. Execute a network method (execute_export is wrapped in our decorator)
    task_id = client.execute_export("workspace_id", "model_id", "export_id")

    # 6. The Assertions (The Proof)
    assert task_id == "task_123", "The method did not return the successful task ID."
    assert mock_post.call_count == 2, "The network call was not retried exactly once."
    mock_auth.clear_token.assert_called_once(), "The authenticator's cache was not wiped!"
