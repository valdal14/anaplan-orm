import asyncio
import logging
import time
from typing import AsyncGenerator

import aiofiles
import httpx

from anaplan_orm.authenticator import Authenticator
from anaplan_orm.exceptions import AnaplanConnectionError
from anaplan_orm.routes import AnaplanRouter
from anaplan_orm.utils import async_retry_network_errors, retry_network_errors

logger = logging.getLogger(__name__)


class AnaplanClient:
    """The core client for interacting with the Anaplan REST API."""

    # Anaplan's base API URL
    BASE_URL = "https://api.anaplan.com/2/0"
    # Standard Megabyte conversion constant
    MB_TO_BYTES = 1024 * 1024

    def __init__(
        self,
        authenticator: Authenticator,
        verify_ssl: bool = True,
        timeout: float = 30.0,
        router: AnaplanRouter | None = None,
    ):
        """
        Initializes the Anaplan client with a specific authentication strategy.

        Args:
            authenticator (Authenticator): An instance of a class that implements
                the Authenticator interface.
            verify_ssl: Default to True, used to bypass your corporate proxy if needed
            timeout: change default 5.0 httpx default timeout
            router: An instance of a url string builder by default AnaplanRouter
        """
        self.authenticator = authenticator
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.router = router or AnaplanRouter()
        self.http_client = httpx.Client(base_url=self.BASE_URL, verify=verify_ssl, timeout=timeout)

    @retry_network_errors()
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
            raise AnaplanConnectionError(
                f"Network error communicating with Anaplan: {str(e)}"
            ) from e

    # NOTE: Methods used to upload of CSVs into Anaplan ################################################################################

    @retry_network_errors()
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

        url_path = self.router.upload_file_url_builder(workspace_id, model_id, file_id)

        try:
            # We must pass the csv_data encoded as bytes to the 'content' parameter
            response = self.http_client.put(
                url_path, headers=headers, content=csv_data.encode("utf-8")
            )
            response.raise_for_status()

        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to upload file to Anaplan: {str(e)}") from e

    @retry_network_errors()
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

        url_path = self.router.process_url_builder(workspace_id, model_id, process_id)

        try:
            response = self.http_client.post(
                url_path, headers=headers, json={"localeName": "en_US"}
            )
            response.raise_for_status()

            return response.json()["task"]["taskId"]

        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to execute process in Anaplan: {str(e)}") from e

    def upload_file_chunked(
        self, workspace_id: str, model_id: str, file_id: str, csv_data: str, chunk_size_mb: int = 10
    ) -> None:
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
        # Initialise the partial upload stream
        headers = self.authenticator.get_auth_headers()
        headers["Content-Type"] = "application/json"

        init_url_path = self.router.upload_file_url_builder(workspace_id, model_id, file_id)

        try:
            # Send the initial request for chunks upload
            self._initialize_chunked_upload(init_url_path, headers)

            # Slice and Stream the bytes to Anaplan
            byte_data = csv_data.encode("utf-8")
            chunk_size_bytes = chunk_size_mb * self.MB_TO_BYTES
            total_bytes = len(byte_data)

            for i in range(0, total_bytes, chunk_size_bytes):
                chunk = byte_data[i : i + chunk_size_bytes]
                chunk_id = str(i // chunk_size_bytes)

                logger.info(f"Uploading Chunk {chunk_id} for file {file_id}...")

                chunk_url = self.router.file_chunk_url_builder(
                    workspace_id, model_id, file_id, chunk_id
                )
                chunk_headers = self.authenticator.get_auth_headers()
                chunk_headers["Content-Type"] = "application/octet-stream"
                # upload a single chunk
                self._send_chunk(chunk_url, chunk_headers, chunk)

            logger.info("Uploading Chunks Process Completed. Finalizing...")

            # Post the final request to inform the partial upload has completed
            complete_url = self.router.file_complete_url_builder(workspace_id, model_id, file_id)
            complete_headers = self.authenticator.get_auth_headers()
            complete_headers["Content-Type"] = "application/json"

            self._complete_chunked_upload(complete_url, complete_headers, file_id)

        except httpx.HTTPError as e:
            # Updated to a more generic error message
            raise AnaplanConnectionError(f"Failed during chunked upload process: {str(e)}") from e

    async def upload_file_chunked_async(
        self,
        workspace_id: str,
        model_id: str,
        file_id: str,
        csv_data: str,
        chunk_size_mb: int = 10,
        max_concurrent_uploads: int = 5,
    ) -> None:
        """
        Uploads a massive CSV string to Anaplan asynchronously.

        Utilizes an asyncio.Semaphore to throttle concurrent connections, preventing
        the Anaplan API from returning 429 Too Many Requests while maximizing throughput.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            file_id (str): The specific file ID in Anaplan.
            csv_data (str): The massive CSV string payload to be uploaded.
            chunk_size_mb (int, optional): The size of each upload chunk in Megabytes. Defaults to 10.
            max_concurrent_uploads (int, optional): The maximum number of simultaneous HTTP requests. Defaults to 5.

        Raises:
            AnaplanConnectionError: If a network connection fails or Anaplan rejects the upload.
        """

        byte_data = csv_data.encode("utf-8")
        chunk_size_bytes = chunk_size_mb * self.MB_TO_BYTES
        total_bytes = len(byte_data)

        # Concurrency Gatekeeper
        semaphore = asyncio.Semaphore(max_concurrent_uploads)

        # Define the isolated async worker task
        @async_retry_network_errors()
        async def _upload_single_chunk_async(
            async_client: httpx.AsyncClient,
            chunk_url: str,
            chunk_headers: dict,
            chunk_bytes: bytes,
            chunk_id: str,
        ):
            # The semaphore ensures only max_concurrent_uploads (5 by default) of these blocks can run simultaneously
            async with semaphore:
                logger.info(f"Async: Uploading Chunk {chunk_id} for file {file_id}...")
                response = await async_client.put(
                    chunk_url, headers=chunk_headers, content=chunk_bytes
                )
                response.raise_for_status()

        # Execute the Async Pipeline by spinnin up an ephemeral AsyncClient
        async with httpx.AsyncClient(
            base_url=self.BASE_URL, timeout=self.timeout, verify=self.verify_ssl
        ) as async_client:
            try:
                init_url = self.router.upload_file_url_builder(workspace_id, model_id, file_id)
                init_headers = self.authenticator.get_auth_headers()
                init_headers["Content-Type"] = "application/json"

                init_resp = await async_client.post(
                    init_url, headers=init_headers, json={"chunkCount": -1}
                )
                init_resp.raise_for_status()

                # Prepare Tasks
                tasks = []
                for i in range(0, total_bytes, chunk_size_bytes):
                    chunk = byte_data[i : i + chunk_size_bytes]
                    chunk_id = str(i // chunk_size_bytes)

                    chunk_url = self.router.file_chunk_url_builder(
                        workspace_id, model_id, file_id, chunk_id
                    )
                    chunk_headers = self.authenticator.get_auth_headers()
                    chunk_headers["Content-Type"] = "application/octet-stream"

                    # Add to our list of coroutines (they don't start executing yet)
                    tasks.append(
                        _upload_single_chunk_async(
                            async_client, chunk_url, chunk_headers, chunk, chunk_id
                        )
                    )

                logger.info(
                    f"Firing {len(tasks)} chunks asynchronously (Max concurrency: {max_concurrent_uploads})..."
                )
                await asyncio.gather(*tasks)

                logger.info("Async Uploading Chunks Completed. Finalizing...")
                complete_url = self.router.file_complete_url_builder(
                    workspace_id, model_id, file_id
                )
                complete_headers = self.authenticator.get_auth_headers()
                complete_headers["Content-Type"] = "application/json"

                complete_resp = await async_client.post(
                    complete_url, headers=complete_headers, json={"id": file_id}
                )
                complete_resp.raise_for_status()

            except httpx.HTTPError as e:
                raise AnaplanConnectionError(
                    f"Failed during async chunked upload process: {str(e)}"
                ) from e

    async def upload_file_streaming_async(
        self,
        workspace_id: str,
        model_id: str,
        file_id: str,
        file_path: str,
        chunk_size_mb: int = 25,
        max_concurrent_uploads: int = 5,
    ) -> None:
        """
        Streams a massive CSV file directly from the local disk to Anaplan asynchronously.

        Utilizes an asyncio.Queue to create a Producer-Consumer pipeline. This guarantees
        a flat, extremely low memory footprint regardless of the total file size, making
        it safe for multi-gigabyte uploads on memory-constrained systems.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            file_id (str): The specific file ID in Anaplan.
            file_path (str): The absolute or relative path to the local CSV file to be uploaded.
            chunk_size_mb (int, optional): The size of each upload chunk in Megabytes. Defaults to 25.
            max_concurrent_uploads (int, optional): The maximum number of simultaneous HTTP requests. Defaults to 5.

        Raises:
            AnaplanConnectionError: If a network connection fails, the file cannot be read, or Anaplan rejects the upload.
        """
        chunk_size_bytes = chunk_size_mb * self.MB_TO_BYTES

        async with httpx.AsyncClient(
            base_url=self.BASE_URL, timeout=self.timeout, verify=self.verify_ssl
        ) as async_client:
            try:
                # Initialize Upload
                init_url = self.router.upload_file_url_builder(workspace_id, model_id, file_id)
                init_headers = self.authenticator.get_auth_headers()
                init_headers["Content-Type"] = "application/json"

                init_resp = await async_client.post(
                    init_url, headers=init_headers, json={"chunkCount": -1}
                )
                init_resp.raise_for_status()

                # Instanciate the queue
                queue = asyncio.Queue(maxsize=max_concurrent_uploads * 2)

                # Decoupled Isolated Chunk Uploader
                @async_retry_network_errors()
                async def _upload_single_chunk(
                    chunk_url: str, chunk_headers: dict, chunk_bytes: bytes
                ):
                    response = await async_client.put(
                        chunk_url, headers=chunk_headers, content=chunk_bytes
                    )
                    response.raise_for_status()

                # Define the Consumer
                async def _upload_worker(worker_id: int):
                    while True:
                        item = await queue.get()

                        if item is None:
                            queue.task_done()
                            break

                        chunk_id, chunk_bytes = item
                        try:
                            logger.info(
                                f"Worker {worker_id}: Streaming Chunk {chunk_id} to Anaplan..."
                            )
                            chunk_url = self.router.file_chunk_url_builder(
                                workspace_id, model_id, file_id, str(chunk_id)
                            )
                            chunk_headers = self.authenticator.get_auth_headers()
                            chunk_headers["Content-Type"] = "application/octet-stream"

                            await _upload_single_chunk(chunk_url, chunk_headers, chunk_bytes)
                        finally:
                            # ALWAYS mark the task done, even if it crashed
                            queue.task_done()

                # Define the Producer (read from disk)
                async def _file_reader():
                    chunk_index = 0
                    async with aiofiles.open(file_path, mode="rb") as f:
                        while True:
                            chunk = await f.read(chunk_size_bytes)
                            if not chunk:
                                break

                            await queue.put((chunk_index, chunk))
                            chunk_index += 1

                    for _ in range(max_concurrent_uploads):
                        await queue.put(None)

                # Execute and Monitor Pipeline
                logger.info(f"Initiating Infinite Stream from disk: {file_path}")

                producer_task = asyncio.create_task(_file_reader())
                consumer_tasks = [
                    asyncio.create_task(_upload_worker(i)) for i in range(max_concurrent_uploads)
                ]

                all_tasks = [producer_task] + consumer_tasks

                # The Circuit Breaker: Wait for completion, but ABORT IMMEDIATELY if any task crashes
                done, pending = await asyncio.wait(all_tasks, return_when=asyncio.FIRST_EXCEPTION)

                # Check for errors and fail fast
                for task in done:
                    if task.exception():
                        # Cancel all pending worker tasks to prevent memory leaks
                        for p in pending:
                            p.cancel()
                        raise task.exception()

                # Finalize Upload
                logger.info("Streaming Upload Completed. Finalizing...")
                complete_url = self.router.file_complete_url_builder(
                    workspace_id, model_id, file_id
                )
                complete_headers = self.authenticator.get_auth_headers()
                complete_headers["Content-Type"] = "application/json"

                complete_resp = await async_client.post(
                    complete_url, headers=complete_headers, json={"id": file_id}
                )
                complete_resp.raise_for_status()

            except httpx.HTTPError as e:
                raise AnaplanConnectionError(
                    f"Failed during async streaming upload process: {str(e)}"
                ) from e

    # NOTE: Method used to exports/downloads From Anaplan ##############################################################################

    @retry_network_errors()
    def execute_export(self, workspace_id: str, model_id: str, export_id: str) -> str:
        """
        Executes an Anaplan export action to generate a downloadable file.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            export_id (str): Anaplan's destination export id as string.

        Returns:
            str: The Anaplan Task ID generated for this export process.

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
        """
        headers = self.authenticator.get_auth_headers()
        headers["Content-Type"] = "application/json"

        url_path = self.router.export_url_builder(workspace_id, model_id, export_id)

        try:
            response = self.http_client.post(
                url_path, headers=headers, json={"localeName": "en_US"}
            )
            response.raise_for_status()

            return response.json()["task"]["taskId"]

        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed to execute export in Anaplan: {str(e)}") from e

    def download_file_chunked(self, workspace_id: str, model_id: str, file_id: str) -> str:
        """
        Downloads a file from Anaplan in sequential chunks and assembles it into a string.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            file_id (str): The Anaplan destination file ID (usually the same as the export ID).

        Returns:
            str: The fully decoded UTF-8 string representation of the downloaded file.

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
        """
        try:
            # Get the chunk count directly from the /chunks endpoint
            chunks_url = self.router.file_chunk_list_url_builder(workspace_id, model_id, file_id)
            chunk_count = self._get_download_chunk_count(chunks_url)

            # Assemble the bytes
            downloaded_bytes = bytearray()

            for i in range(chunk_count):
                chunk_id = str(i)
                logger.info(
                    f"Downloading Chunk {chunk_id} of {chunk_count - 1} for file {file_id}..."
                )

                chunk_url = self.router.file_chunk_url_builder(
                    workspace_id, model_id, file_id, chunk_id
                )

                chunk_data = self._download_chunk(chunk_url)
                downloaded_bytes.extend(chunk_data)

            logger.info("Downloading Chunks Process Completed. Decoding...")

            # Decode to string
            return downloaded_bytes.decode("utf-8")

        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Failed during chunked download process: {str(e)}") from e

    async def download_file_streaming_async(
        self, workspace_id: str, model_id: str, file_id: str
    ) -> AsyncGenerator[str, None]:
        """
        Downloads a massive file from Anaplan as an Asynchronous Generator.

        Yields the file line-by-line (or complete row by complete row) by safely
        buffering chunk boundaries. This guarantees a flat memory footprint
        regardless of the export's total size.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            file_id (str): The Anaplan destination file ID (usually the export ID).

        Yields:
            str: A single, fully decoded complete row from the Anaplan CSV export.

        Raises:
            AnaplanConnectionError: If a network connection fails or Anaplan rejects the request.
        """

        async with httpx.AsyncClient(
            base_url=self.BASE_URL, timeout=self.timeout, verify=self.verify_ssl
        ) as async_client:
            # Isolated Async Helpers for Retries
            @async_retry_network_errors()
            async def _get_chunk_count() -> int:
                count_url = self.router.file_chunk_list_url_builder(workspace_id, model_id, file_id)
                headers = self.authenticator.get_auth_headers()
                response = await async_client.get(count_url, headers=headers)
                response.raise_for_status()
                return len(response.json().get("chunks", []))

            @async_retry_network_errors()
            async def _download_single_chunk(chunk_index: str) -> bytes:
                chunk_url = self.router.file_chunk_url_builder(
                    workspace_id, model_id, file_id, chunk_index
                )
                headers = self.authenticator.get_auth_headers()
                headers["Accept"] = "application/octet-stream"
                response = await async_client.get(chunk_url, headers=headers)
                response.raise_for_status()
                return response.content

            try:
                # Fetch total chunks
                chunk_count = await _get_chunk_count()
                if chunk_count == 0:
                    return

                logger.info(f"Initiating Streaming Download: {chunk_count} total chunks detected.")

                buffer = b""

                # Stream and Yield
                for i in range(chunk_count):
                    chunk_id = str(i)
                    logger.info(f"Downloading Chunk {chunk_id} of {chunk_count - 1}...")

                    chunk_data = await _download_single_chunk(chunk_id)
                    buffer += chunk_data

                    # Split the buffer by Newline
                    while b"\n" in buffer:
                        line, buffer = buffer.split(b"\n", 1)
                        # Yield the complete line (decoded to string), adding the newline back
                        yield line.decode("utf-8") + "\n"

                # Yield any remaining data in the buffer (the final row might not have a trailing newline)
                if buffer:
                    yield buffer.decode("utf-8")

                logger.info("Streaming Download Completed Successfully.")

            except httpx.HTTPError as e:
                raise AnaplanConnectionError(
                    f"Failed during async streaming download process: {str(e)}"
                ) from e

    # NOTE: Methods used to get confirmation from Anaplan ##############################################################################

    def wait_for_process_completion(
        self,
        workspace_id: str,
        model_id: str,
        process_id: str,
        task_id: str,
        retry: int = 60,
        poll_interval: int = 5,
    ) -> dict:
        """
        Actively polls the Anaplan API to check the status of an asynchronous import process.

        This method acts as a public facade, utilizing the internal Router to safely
        build the process-specific URL before passing it to the unified polling engine.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            process_id (str): The Anaplan process ID being executed.
            task_id (str): The specific task ID generated by the initial process execution.
            retry (int, optional): The number of polling attempts remaining. Defaults to 60.
            poll_interval (int, optional): The seconds to wait between polling attempts. Defaults to 5.

        Returns:
            dict: The complete task dictionary returned by Anaplan upon successful completion.

        Raises:
            AnaplanConnectionError: If the process fails, is cancelled, or runs out of retries.
        """
        url_path = self.router.process_task_url_builder(workspace_id, model_id, process_id, task_id)
        return self._wait_for_task(url_path, retry, poll_interval)

    def wait_for_export_completion(
        self,
        workspace_id: str,
        model_id: str,
        export_id: str,
        task_id: str,
        retry: int = 60,
        poll_interval: int = 5,
    ) -> dict:
        """
        Actively polls the Anaplan API to check the status of an asynchronous export.

        This method acts as a public facade, utilizing the internal Router to safely
        build the export-specific URL before passing it to the unified polling engine.

        Args:
            workspace_id (str): The Anaplan workspace ID.
            model_id (str): The Anaplan destination model ID.
            export_id (str): The specific export ID being executed.
            task_id (str): The specific task ID generated by the initial export execution.
            retry (int, optional): The number of polling attempts remaining. Defaults to 60.
            poll_interval (int, optional): The seconds to wait between polling attempts. Defaults to 5.

        Returns:
            dict: The complete task dictionary returned by Anaplan upon successful completion.

        Raises:
            AnaplanConnectionError: If the export fails, is cancelled, or runs out of retries.
        """
        url_path = self.router.export_task_url_builder(workspace_id, model_id, export_id, task_id)
        return self._wait_for_task(url_path, retry, poll_interval)

    # --- The Unified Internal Polling Engine ---

    def _wait_for_task(
        self,
        url_path: str,
        retry: int,
        poll_interval: int,
    ) -> dict:
        """
        Internal recursive engine that polls any Anaplan task URL until completion.

        This method uses recursion to pause and re-check the task status until it
        either completes successfully, fails internally, or exhausts the allowed retries.

        Args:
            url_path (str): The fully constructed Anaplan API endpoint for the specific task.
            retry (int): The current number of polling attempts remaining.
            poll_interval (int): The number of seconds to sleep between network requests.

        Returns:
            dict: The complete task dictionary returned by Anaplan upon successful completion.

        Raises:
            AnaplanConnectionError: If the task fails internally, hits an unknown state, or times out.
        """
        if retry <= 0:
            raise AnaplanConnectionError(
                "Anaplan task did not complete within the assigned time limit."
            )

        anaplan_task = self._get_task_status(url_path)

        task_state = anaplan_task.get("taskState")
        is_successful = anaplan_task.get("result", {}).get("successful", False)

        if task_state == "COMPLETE":
            if is_successful:
                return anaplan_task
            else:
                raise AnaplanConnectionError(
                    f"Anaplan task completed but failed internally. Task info: {anaplan_task}"
                )

        if task_state in ["IN_PROGRESS", "NOT_STARTED"]:
            self._process_to_sleep(poll_interval)
            return self._wait_for_task(url_path, retry - 1, poll_interval)

        raise AnaplanConnectionError(f"Task execution halted. Final state: {task_state}")

    # NOTE: HELPER METHODS #############################################################################################################

    def _process_to_sleep(self, t: int) -> None:
        """Helper method to manage polling intervals by pausing script execution."""
        for _ in range(t):
            time.sleep(1)

    @retry_network_errors()
    def _download_chunk(self, url_path: str) -> bytes:
        """Isolated helper to fetch a single file chunk, protected by retries."""
        headers = self.authenticator.get_auth_headers()
        # Must use 'Accept' to inform Anaplan we want raw bytes back
        headers["Accept"] = "application/octet-stream"

        response = self.http_client.get(url_path, headers=headers)
        response.raise_for_status()

        return response.content

    @retry_network_errors()
    def _get_task_status(self, url_path: str) -> dict:
        """Isolated helper to fetch task status, protected by retries."""
        headers = self.authenticator.get_auth_headers()
        try:
            response = self.http_client.get(url_path, headers=headers)
            response.raise_for_status()
            return response.json()["task"]
        except httpx.HTTPError as e:
            raise AnaplanConnectionError(
                f"Failed to fetch task status from Anaplan: {str(e)}"
            ) from e

    @retry_network_errors()
    def _initialize_chunked_upload(self, url: str, headers: dict) -> None:
        """Isolated helper to send the first request for chunks upload, protected by retries."""
        response = self.http_client.post(url, headers=headers, json={"chunkCount": -1})
        response.raise_for_status()

    @retry_network_errors()
    def _send_chunk(self, url: str, headers: dict, content: bytes) -> None:
        """Isolated helper to send a single chunk, protected by retries."""
        response = self.http_client.put(url, headers=headers, content=content)
        response.raise_for_status()

    @retry_network_errors()
    def _complete_chunked_upload(self, url: str, headers: dict, file_id: str) -> None:
        """Isolated helper to send the final request for chunks upload, protected by retries."""
        response = self.http_client.post(url, headers=headers, json={"id": file_id})
        response.raise_for_status()

    @retry_network_errors()
    def _get_download_chunk_count(self, url_path: str) -> int:
        """
        Fetches the total number of chunks available for a specific Anaplan file download.

        Args:
            url_path (str): The URL destination path to execute the GET request.

        Returns:
            int: The total number of chunks.

        Raises:
            AnaplanConnectionError: If a connection fails or Anaplan rejects the request.
        """
        headers = self.authenticator.get_auth_headers()

        try:
            response = self.http_client.get(url_path, headers=headers)
            response.raise_for_status()

            # Anaplan returns a JSON payload with a 'chunks' array. We just count the items!
            return len(response.json().get("chunks", []))

        except httpx.HTTPError as e:
            raise AnaplanConnectionError(
                f"Failed to fetch chunk count from Anaplan: {str(e)}"
            ) from e
