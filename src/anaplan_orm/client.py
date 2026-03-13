import httpx
import time
from abc import ABC, abstractmethod
from anaplan_orm.exceptions import AnaplanConnectionError
import logging
logger = logging.getLogger(__name__)

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

class AnaplanClient:
    """
    The core client for interacting with the Anaplan REST API.
    """

    # Anaplan's base API URL
    BASE_URL = "https://api.anaplan.com/2/0"
    
    def __init__(self, authenticator: Authenticator, verify_ssl: bool = True, timeout: float = 30.0):
        """
        Initializes the Anaplan client with a specific authentication strategy.
        
        Args:
            authenticator (Authenticator): An instance of a class that implements 
                the Authenticator interface.
            
            verify_ssl: Default to True, used to bypass your corporate proxy if needed

            timeout: change default 5.0 httpx default timeout
        """
        self.authenticator = authenticator
        # Create a reusable HTTP session for performance
        self.http_client = httpx.Client(
            base_url=self.BASE_URL, 
            verify=verify_ssl,
            timeout=timeout
        )

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
    
    def upload_file(self, workspace_id: str, model_id: str, file_id: str, csv_data: str) -> None:
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
        
        url_path = self._upload_file_url_builder(workspace_id, model_id, file_id)
        
        try:
            # We must pass the csv_data encoded as bytes to the 'content' parameter
            response = self.http_client.put(
                url_path, 
                headers=headers, 
                content=csv_data.encode('utf-8')
            )
            response.raise_for_status()
            
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to upload file to Anaplan: {str(e)}") from e

    def execute_process(self, workspace_id: str, model_id: str, process_id: str) -> str:
        """
        Execute an Anaplan import process after a CSV file has been successfully uploaded.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            process_id: Anaplan's destination process id as string

        Returns:
            str: The Anaplan Task ID generated for this asynchronous import.

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
        """
        headers = self.authenticator.get_auth_headers()
        headers["Content-Type"] = "application/json"

        url_path = self._process_url_builder(workspace_id, model_id, process_id)

        try:
            response = self.http_client.post(
                url_path, 
                headers=headers, 
                json={"localeName": "en_US"}
            )
            response.raise_for_status()

            return response.json()["task"]["taskId"]
            
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to execute process in Anaplan: {str(e)}") from e
        
    def _get_process_task_status(self, workspace_id: str, model_id: str, process_id: str, task_id: str) -> dict:
        """
        Fetches the current status of an asynchronous Anaplan process task.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            process_id: Anaplan's destination process id as string
            task_id: Anaplan's task id used to check request status

        Returns:
            dict: Contains the information about the status of the process.

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
        """
        headers = self.authenticator.get_auth_headers()
        url_path = self._process_task_url_builder(workspace_id, model_id, process_id, task_id)

        try:
            response = self.http_client.get(url_path, headers=headers)
            response.raise_for_status()

            return response.json()["task"]
            
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to fetch task status from Anaplan: {str(e)}") from e

    def wait_for_process_completion(self, workspace_id: str, model_id: str, process_id: str, task_id: str, retry: int = 3, poll_interval: int = 5) -> dict:
        """
        Actively polls the Anaplan API to check the status of an asynchronous process.
        
        This method uses recursion to pause and re-check the task status until it 
        either completes successfully, fails internally, or exhausts the allowed retries.
        
        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            process_id (str): The Anaplan process ID being executed.
            task_id (str): The specific task ID generated by the initial process execution.
            retry (int, optional): The number of polling attempts remaining. Maximum allowed is 5. Defaults to 3.
            poll_interval (int, optional): The seconds to wait between polling attempts. Defaults to 5.
            
        Returns:
            dict: The complete task dictionary returned by Anaplan upon successful completion.
            
        Raises:
            ValueError: If the initial retry value passed is greater than 5.
            AnaplanConnectionError: If the process fails, is cancelled, or runs out of retries.
        """
        # Safeguard against deep recursion
        if retry > 5:
            raise ValueError("For script stability, the maximum allowed retries for this recursive method is 5.")

        if retry <= 0:
            raise AnaplanConnectionError("Anaplan process did not complete within the maximum number of retries.")

        anaplan_task = self._get_process_task_status(workspace_id, model_id, process_id, task_id)
        
        task_state = anaplan_task.get("taskState")
        is_successful = anaplan_task.get("result", {}).get("successful", False)

        # Evaluate if it finished completely
        if task_state == "COMPLETE":
            if is_successful:
                return anaplan_task
            else:
                # It finished, but Anaplan rejected the data
                raise AnaplanConnectionError(f"Anaplan process completed but failed internally. Task info: {anaplan_task}")

        # Evaluate if it is still working
        if task_state in ["IN_PROGRESS", "NOT_STARTED"]:
            self._process_to_sleep(poll_interval)
            return self.wait_for_process_completion(
                workspace_id, 
                model_id, 
                process_id, 
                task_id, 
                retry - 1, 
                poll_interval
            )
            
        # Evaluate if it was cancelled by an admin or hit an unknown state
        raise AnaplanConnectionError(f"Process execution halted. Final state: {task_state}")
            
    def _process_to_sleep(self, t: int) -> None:
        """Helper method to manage polling intervals by pausing script execution."""
        for _ in range(t):
            time.sleep(1)

    def upload_file_chunked(self, workspace_id: str, model_id: str, file_id: str, csv_data: str, chunk_size_mb: int = 10) -> None:
            """ 
            Uploads a large CSV string to Anaplan in sequential chunks. 
            
            Args:
                workspace_id (str): The Anaplan workspace ID.
                model_id (str): The Anaplan destination model ID.
                file_id (str): The specific file ID in Anaplan.
                csv_data (str): The string representing the model to be updated.
                chunk_size_mb (int): The size of the chunk to be uploaded in Megabytes. Defaults to 10.
                
            Raises:
                AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
            """
            # --- STEP 1: Initialise the partial upload stream ---
            headers = self.authenticator.get_auth_headers()
            headers["Content-Type"] = "application/json"

            init_url_path = self._upload_file_url_builder(workspace_id, model_id, file_id)

            try:
                init_response = self.http_client.post(
                    init_url_path, 
                    headers=headers, 
                    json={"chunkCount": -1}
                )
                init_response.raise_for_status()
                
                # --- STEP 2: Slice and Stream the bytes to Anaplan ---
                byte_data = csv_data.encode('utf-8')
                chunk_size_bytes = chunk_size_mb * 1024 * 1024
                total_bytes = len(byte_data)

                for i in range(0, total_bytes, chunk_size_bytes):
                    chunk = byte_data[i : i + chunk_size_bytes]
                    chunk_id = str(i // chunk_size_bytes)
                    
                    logger.info(f"Uploading Chunk {chunk_id} for file {file_id}...")

                    chunk_url = self._file_chunk_url_builder(workspace_id, model_id, file_id, chunk_id)
                    
                    chunk_headers = self.authenticator.get_auth_headers()
                    chunk_headers["Content-Type"] = "application/octet-stream"

                    chunk_response = self.http_client.put(
                        chunk_url,
                        headers=chunk_headers,
                        content=chunk 
                    )
                    chunk_response.raise_for_status()
                
                logger.info("Uploading Chunks Process Completed. Finalizing...")

                # --- STEP 3: Post the final request to inform the partial upload has completed ---
                complete_url = self._file_complete_url_builder(workspace_id, model_id, file_id)
                complete_headers = self.authenticator.get_auth_headers()
                complete_headers["Content-Type"] = "application/json"
                
                complete_response = self.http_client.post(
                    complete_url,
                    headers=complete_headers,
                    json={"id": file_id}
                )
                complete_response.raise_for_status()
                        
            except httpx.HTTPError as e:
                # Updated to a more generic error message
                raise AnaplanConnectionError(f"Failed during chunked upload process: {str(e)}") from e

    # Helper Methods ##########################################################################
    def _upload_file_url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan file.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}"
    
    def _process_url_builder(self, workspace_id: str, model_id: str, process_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan process action.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            process_id: Anaplan's destination process id as string
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks"
    
    def _process_task_url_builder(self, workspace_id: str, model_id: str, process_id: str, task_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan process task verification.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            process_id: Anaplan's destination process id as string
            task_id: Anaplan's task id used to check request status
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/processes/{process_id}/tasks/{task_id}"
    
    def _file_chunk_url_builder(self, workspace_id: str, model_id: str, file_id: str, chunk_id: str) -> str:
        """
        Constructs the specific endpoint path for an Anaplan chunk upload API.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string
            chunk_id: Index of the chunk to be uploaded
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/chunks/{chunk_id}"
    
    def _file_complete_url_builder(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Constructs the specific endpoint path to complete a chunked file upload.

        Args:
            workspace_id: Anaplan's workspace id as string
            model_id: Anaplan's destination model id as string
            file_id: Anaplan's destination file id as string
            
        Returns:
            str: The constructed Anaplan URL path.
        """
        return f"/workspaces/{workspace_id}/models/{model_id}/files/{file_id}/complete"