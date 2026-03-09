import httpx
from abc import ABC, abstractmethod
from anaplan_orm.exceptions import AnaplanConnectionError

class Authenticator(ABC):
    """
    Abstract interface for Anaplan Authentication strategies.
    """
    
    @abstractmethod
    def get_auth_headers(self) -> dict:
        """
        Retrieves the necessary headers (e.g., Authorization string) 
        required to authenticate a request to the Anaplan API.
        
        Returns:
            dict: A dictionary of HTTP headers.
        """
        pass

class BasicAuthenticator(Authenticator):
    AUTH_URL = "https://auth.anaplan.com/token/authenticate"

    def __init__(self, email: str, pwd: str, verify_ssl: bool = True):
        self.email = email
        self.pwd = pwd
        self.verify_ssl = verify_ssl

    def get_auth_headers(self) -> dict:
        try:
            response = httpx.post(
                self.AUTH_URL, 
                auth=(self.email, self.pwd),
                verify=self.verify_ssl
            )
                       
            response.raise_for_status()
            json_payload = response.json()
            
            if json_payload.get("status") != "SUCCESS":
                err_msg = json_payload.get("statusMessage", "Unknown Error")
                raise AnaplanConnectionError(f"Anaplan Auth Failed. Status: {json_payload.get('status')} - {err_msg}")
            
            # Extract the actual token string
            # print(json_payload)
            # print(json_payload["status"])
            # print(json_payload["tokenInfo"]["expiresAt"])
            # print(json_payload["tokenInfo"]["tokenValue"])
            # print(json_payload["tokenInfo"]["refreshTokenId"])
            token_value = json_payload["tokenInfo"]["tokenValue"]
            
            return {
                "Authorization": f"AnaplanAuthToken {token_value}",
                "Content-Type": "application/json"
            }
            
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Authentication failed: {str(e)}") from e

class AnaplanClient:
    """
    The core client for interacting with the Anaplan REST API.
    """
    
    # Anaplan's base API URL
    BASE_URL = "https://api.anaplan.com/2/0"
    
    def __init__(self, authenticator: Authenticator, verify_ssl: bool = True):
        """
        Initializes the Anaplan client with a specific authentication strategy.
        
        Args:
            authenticator (Authenticator): An instance of a class that implements 
                the Authenticator interface.
        """
        self.authenticator = authenticator
        # Create a reusable HTTP session for performance
        self.http_client = httpx.Client(base_url=self.BASE_URL, verify=verify_ssl)

    def ping(self) -> int:
        """
        A simple test method to verify network connectivity and authentication.
        
        Returns:
            int: The HTTP status code from the Anaplan API.
        """
        headers = self.authenticator.get_auth_headers()
        try:
            response = self.http_client.get("/users/me", headers=headers)
            return response.status_code
        except httpx.RequestError as e:
            raise AnaplanConnectionError(f"Network error communicating with Anaplan: {str(e)}") from e
    
    def upload_file(self, workspace_id: str, model_id: str, file_id: str, csv_data: str):
        """
        Uploads a CSV string to an Anaplan data hub file placeholder.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string
            csv_data: The fully formatted CSV string to upload

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the upload.
        """
        headers = self.authenticator.get_auth_headers()
        headers["Content-Type"] = "application/octet-stream"
        
        url_path = self.url_builder(workspace_id, model_id, file_id)
        
        try:
            # We must pass the csv_data encoded as bytes to the 'content' parameter
            response = self.http_client.put(
                url_path, 
                headers=headers, 
                content=csv_data.encode('utf-8')
            )
            response.raise_for_status()
            
        except httpx.HTTPError as e:
            # Catch HTTPError to handle both network drops and 4xx/5xx responses
            raise AnaplanConnectionError(f"Failed to upload file to Anaplan: {str(e)}") from e

    def execute_import(self, workspace_id: str, model_id: str, import_id: str) -> str:
        """
        Execute an Anaplan import process after a CSV file has been successfully uploaded.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            import_id: Anaplan's destination process import id as string

        Returns:
            str: The Anaplan Task ID generated for this asynchronous import.

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
        """
        headers = self.authenticator.get_auth_headers()
        headers["Content-Type"] = "application/json"

        url_path = self.import_url_builder(workspace_id, model_id, import_id)

        try:
            response = self.http_client.post(
                url_path, 
                headers=headers, 
                json={"localeName": "en_US"}
            )
            response.raise_for_status()

            print(response) # REMOVE AFTER TEST
            return response.json()["task"]["taskId"]
            
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to execute import process in Anaplan: {str(e)}") from e

    # Helper Methods ##########################################################################
    def url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan file.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        # Notice the 'f' at the start of the string!
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
    
    def import_url_builder(self, workspace_id: str, model_id: str, import_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan import action.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            import_id: Anaplan's destination process import id as string
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/imports/{import_id}/tasks"