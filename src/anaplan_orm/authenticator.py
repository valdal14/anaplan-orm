import base64
import os
import time
from abc import ABC, abstractmethod

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from anaplan_orm.exceptions import AnaplanConnectionError
from anaplan_orm.utils import retry_network_errors


class Authenticator(ABC):
    """
    Abstract interface for Anaplan Authentication strategies.
    Handles token caching and lifecycle management automatically.
    """

    AUTH_URL = "https://auth.anaplan.com/token/authenticate"
    DEFAULT_TOKEN_REFRESH_TIME: int = 1800

    def __init__(self):
        self._cached_token: str | None = None
        self._token_timestamp: float = 0.0

    def _requires_new_token(self) -> bool:
        """Returns True if a new token is needed, False otherwise."""
        return (self._cached_token is None) or (
            time.time() - self._token_timestamp > self.DEFAULT_TOKEN_REFRESH_TIME
        )

    def clear_token(self) -> None:
        """Wipes the cached token, forcing a fresh handshake on the next request."""
        self._cached_token = None

    def get_auth_headers(self) -> dict:
        """Returns the authorization headers, fetching a new token only if necessary."""
        if self._requires_new_token():
            self.authenticate()

        return {
            "Authorization": f"AnaplanAuthToken {self._cached_token}",
            "Content-Type": "application/json",
        }

    @abstractmethod
    def authenticate(self) -> None:
        """
        Implementation-specific logic to fetch a token from Anaplan
        and update self._cached_token and self._token_timestamp.
        """
        pass


# =========================================================================================


class BasicAuthenticator(Authenticator):
    def __init__(self, email: str, pwd: str, verify_ssl: bool = True):
        super().__init__()  # Initialize the base class token caching
        self.email = email
        self.pwd = pwd
        self.verify_ssl = verify_ssl

    @retry_network_errors()
    def authenticate(self) -> None:
        """Fetches a new token using Email and Password."""
        try:
            response = httpx.post(
                self.AUTH_URL, auth=(self.email, self.pwd), verify=self.verify_ssl
            )
            response.raise_for_status()
            json_payload = response.json()

            if json_payload.get("status") != "SUCCESS":
                err_msg = json_payload.get("statusMessage", "Unknown Error")
                raise AnaplanConnectionError(f"Anaplan Auth Failed: {err_msg}")

            self._cached_token = json_payload["tokenInfo"]["tokenValue"]
            self._token_timestamp = time.time()

        except httpx.HTTPError as e:
            raise AnaplanConnectionError(f"Basic Authentication failed: {str(e)}") from e


# =========================================================================================


class CertificateAuthenticator(Authenticator):
    def __init__(self, cert_path: str, cert_password: str = None, verify_ssl: bool = True):
        super().__init__()
        self.cert_path = cert_path
        self.cert_password = cert_password
        self.verify_ssl = verify_ssl

    @retry_network_errors()
    def authenticate(self) -> None:
        """Fetches a new token using Anaplan's custom RSA-SHA512 handshake."""
        try:
            # 1. Read the raw PEM file
            with open(self.cert_path, "rb") as f:
                pem_data = f.read()

            # 2. Extract Public Certificate string (Anaplan requires it without headers or newlines)
            pem_text = pem_data.decode("utf-8")
            if "-----BEGIN CERTIFICATE-----" not in pem_text:
                raise ValueError("No public certificate found in the PEM file.")

            cert_body = pem_text.split("-----BEGIN CERTIFICATE-----")[1].split(
                "-----END CERTIFICATE-----"
            )[0]
            pub_cert_string = cert_body.replace("\n", "").replace("\r", "")

            # 3. Load the Private Key to sign the payload
            pwd_bytes = self.cert_password.encode("utf-8") if self.cert_password else None
            private_key = serialization.load_pem_private_key(pem_data, password=pwd_bytes)

            # 4. Generate a 100-byte random message and encode it
            random_bytes = os.urandom(100)
            encoded_data = base64.b64encode(random_bytes).decode("utf-8")

            # 5. Sign the message using RSA-SHA512
            signature = private_key.sign(random_bytes, padding.PKCS1v15(), hashes.SHA512())
            encoded_signed_data = base64.b64encode(signature).decode("utf-8")

            # 6. Build Anaplan's highly specific JSON request
            headers = {
                "Authorization": f"CACertificate {pub_cert_string}",
                "Content-Type": "application/json",
            }
            payload = {"encodedData": encoded_data, "encodedSignedData": encoded_signed_data}

            # Send a standard POST request
            response = httpx.post(
                self.AUTH_URL, headers=headers, json=payload, verify=self.verify_ssl
            )
            response.raise_for_status()
            json_payload = response.json()

            if json_payload.get("status") != "SUCCESS":
                err_msg = json_payload.get("statusMessage", "Unknown Error")
                raise AnaplanConnectionError(f"Anaplan Auth Failed: {err_msg}")

            self._cached_token = json_payload["tokenInfo"]["tokenValue"]
            self._token_timestamp = time.time()

        except Exception as e:
            raise AnaplanConnectionError(f"Certificate Authentication failed: {str(e)}") from e
