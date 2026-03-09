import pytest
import httpx
from unittest.mock import patch, Mock

from anaplan_orm.client import AnaplanClient, Authenticator
from anaplan_orm.exceptions import AnaplanConnectionError

# Create our Dummy Authenticator
class DummyAuthenticator(Authenticator):
    def get_auth_headers(self) -> dict:
        return {"Authorization": "AnaplanAuthToken FakeTestToken"}

def test_ping_returns_status_code():
    """Test that the client correctly returns the status code from the HTTP response."""
    auth = DummyAuthenticator()
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
    auth = DummyAuthenticator()
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
    auth = DummyAuthenticator()
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
    auth = DummyAuthenticator()
    client = AnaplanClient(authenticator=auth)
    
    with patch.object(httpx.Client, 'put') as mock_put:
        # Force httpx to simulate a 404 Not Found or 500 Server Error
        mock_put.side_effect = httpx.HTTPError("404 Client Error: Not Found for url")
        
        with pytest.raises(AnaplanConnectionError) as exc_info:
            client.upload_file("w", "m", "f", "data")
            
        assert "Failed to upload file to Anaplan" in str(exc_info.value)