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