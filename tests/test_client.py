import pytest
import httpx
from unittest.mock import patch, Mock, MagicMock, call
from anaplan_orm.client import AnaplanClient, Authenticator, BasicAuthenticator
from anaplan_orm.exceptions import AnaplanConnectionError

# Create our Dummy Authenticator
class DummyAuthenticator(Authenticator):
    def get_auth_headers(self) -> dict:
        return {"Authorization": "AnaplanAuthToken FakeTestToken"}

def test_ping_returns_status_code():
    """Test that the client correctly returns the status code from the HTTP response."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    # Act: Hijack the httpx 'get' method
    with patch.object(httpx.Client, 'get') as mock_get:
        fake_response = Mock()
        fake_response.status_code = 401
        mock_get.return_value = fake_response
        
        status = client.ping()
        
    # Assert: Verify ping method returned the mocked 401
    assert status == 401
    # Verify the client actually tried to pass the correct headers to httpx
    mock_get.assert_called_once_with("/users/me", headers={"Authorization": "AnaplanAuthToken FakeTestToken"})

def test_ping_raises_custom_connection_error():
    """Test that httpx network failures are caught and raised as AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(httpx.Client, 'get') as mock_get:
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
    with patch.object(httpx.Client, 'put') as mock_put:
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
            "Content-Type": "application/octet-stream"
        }
        
        mock_put.assert_called_once_with(
            expected_url,
            headers=expected_headers,
            content=csv_payload.encode('utf-8') # Verifying the byte conversion!
        )

def test_upload_file_raises_custom_error():
    """Test that failed uploads raise our custom AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(httpx.Client, 'put') as mock_put:
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
    with patch.object(httpx.Client, 'post') as mock_post:
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
            "Content-Type": "application/json"
        }
        
        mock_post.assert_called_once_with(
            expected_url,
            headers=expected_headers,
            json={"localeName": "en_US"}
        )

def test_execute_process_raises_custom_error():
    """Test that failed process raise our custom AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(httpx.Client, 'post') as mock_post:
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
    with patch.object(httpx.Client, 'get') as mock_get:
        # Create a fake successful HTTP response returning a mock task state
        fake_response = Mock()
        fake_response.raise_for_status.return_value = None
        fake_response.json.return_value = {
            "task": {
                "taskId": "task_abc", 
                "taskState": "COMPLETE", 
                "successful": True
            }
        }
        mock_get.return_value = fake_response
        
        # Execute our method
        result_task = client._get_process_task_status(workspace_id, model_id, process_id, task_id)
        
        # Assert: Verify the method returned the exact inner 'task' dictionary
        assert result_task["taskState"] == "COMPLETE"
        assert result_task["successful"] is True
        
        # Verify httpx.get was called with the correct URL and headers keyword argument
        expected_url = f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks/{task_id}"
        expected_headers = {
            "Authorization": "AnaplanAuthToken FakeTestToken"
        }
        
        mock_get.assert_called_once_with(
            expected_url,
            headers=expected_headers
        )

def test_get_process_task_status_raises_custom_error():
    """Test that failed task status polls raise our custom AnaplanConnectionError."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(httpx.Client, 'get') as mock_get:
        # Force httpx to simulate a 404 Not Found
        mock_get.side_effect = httpx.HTTPError("404 Client Error: Not Found for url")
        
        with pytest.raises(AnaplanConnectionError) as exc_info:
            client._get_process_task_status("w", "m", "p", "t")
            
        assert "Failed to fetch task status from Anaplan" in str(exc_info.value)

def test_wait_for_process_completion_success_first_try():
    """Test that the waiter returns immediately if the task is already COMPLETE."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(client, '_get_process_task_status') as mock_status:
        # Mock Anaplan returning a success on the very first check
        mock_status.return_value = {
            "taskState": "COMPLETE",
            "result": {"successful": True}
        }
        
        result = client.wait_for_process_completion("w", "m", "p", "t", retry=3)
        
        assert result["taskState"] == "COMPLETE"
        assert mock_status.call_count == 1

def test_wait_for_process_completion_recursive_success():
    """Test that the waiter sleeps, decrements the retry counter, and eventually succeeds."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(client, '_get_process_task_status') as mock_status, \
         patch.object(client, 'process_to_sleep') as mock_sleep:
        
        # Mock Anaplan returning IN_PROGRESS on the first try, then COMPLETE on the second
        mock_status.side_effect = [
            {"taskState": "IN_PROGRESS"},
            {"taskState": "COMPLETE", "result": {"successful": True}}
        ]
        
        result = client.wait_for_process_completion("w", "m", "p", "t", retry=3)
        
        assert result["taskState"] == "COMPLETE"
        assert mock_status.call_count == 2
        # It should have slept exactly once
        assert mock_sleep.call_count == 1

def test_wait_for_process_completion_blocks_high_retries():
    """Test that the safeguard prevents users from setting retry > 5."""
    auth = get_basic_authenticator()
    client = AnaplanClient(authenticator=auth)
    
    with pytest.raises(ValueError) as exc_info:
        client.wait_for_process_completion("w", "m", "p", "t", retry=6)
        
    assert "maximum allowed retries for this recursive method is 5" in str(exc_info.value)

def test_authenticator_caches_token_successfully():
    """Test that rapid consecutive calls only hit the Anaplan API once."""
    auth = get_basic_authenticator(is_mocked_authenticator=False)
    
    with patch("httpx.post") as mock_post:
        # Arrange: Setup a fake successful Anaplan response
        fake_response = MagicMock()
        fake_response.json.return_value = {
            "status": "SUCCESS",
            "tokenInfo": {"tokenValue": "First_Token_ABC"}
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
        fake_response_1.json.return_value = {"status": "SUCCESS", "tokenInfo": {"tokenValue": "First_Token_ABC"}}
        
        fake_response_2 = MagicMock()
        fake_response_2.json.return_value = {"status": "SUCCESS", "tokenInfo": {"tokenValue": "New_Token_XYZ"}}
        
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

# NOTE: Helpers Methods
def get_basic_authenticator(is_mocked_authenticator: bool = True) -> Authenticator:
    if is_mocked_authenticator:
        return DummyAuthenticator()
    else:
        return BasicAuthenticator("test@company.com", "pwd123")