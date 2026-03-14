import httpx
import time
from abc import ABC, abstractmethod
from anaplan_orm.exceptions import AnaplanConnectionError
from anaplan_orm.utils import retry_network_errors

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
    # 30 mins default token refresh time
    DEFAULT_TOKEN_REFRESH_TIME: int = 1800 

    def __init__(self, email: str, pwd: str, verify_ssl: bool = True):
        self.email = email
        self.pwd = pwd
        self.verify_ssl = verify_ssl
        self._cached_token: str | None = None
        self._token_timestamp: float = 0.0

    def _requires_new_token(self) -> bool:
        """ Returns True if a new token is needed, False otherwise. """
        return (self._cached_token is None) or (time.time() - self._token_timestamp > self.DEFAULT_TOKEN_REFRESH_TIME)
    
    @retry_network_errors()
    def _perform_basic_auth_request(self) -> None:
        """ Fetches a new token from Anaplan and updates internal state. """
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
            
            # Save the new token
            self._cached_token = json_payload["tokenInfo"]["tokenValue"]
            
            # Start the stopwatch
            self._token_timestamp = time.time()
            
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Authentication failed: {str(e)}") from e

    def get_auth_headers(self) -> dict:
        """ Returns the authorization headers, fetching a new token only if necessary. """
        if self._requires_new_token():
            self._perform_basic_auth_request()
            
        # Returns the cached token (whether it was just fetched or is still valid)
        return {
            "Authorization": f"AnaplanAuthToken {self._cached_token}",
            "Content-Type": "application/json"
        }