import httpx
import logging
import time
from functools import wraps
from anaplan_orm.exceptions import AnaplanConnectionError

logger = logging.getLogger(__name__)

def retry_network_errors(max_retries: int = 3, base_delay: float = 1.0):
    """
    A decorator that catches transient httpx network errors and retries the function 
    using exponential backoff. It dynamically unwraps custom AnaplanConnectionErrors 
    to inspect the underlying cause.
    """
    if max_retries <= 0:
        raise ValueError("max_retries must be greater than 0")

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            client_instance = args[0]

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                
                except Exception as wrapper_error:
                    # Unwrap the error to find the true HTTP cause
                    http_error = wrapper_error.__cause__ if isinstance(wrapper_error, AnaplanConnectionError) else wrapper_error
                    
                    if not isinstance(http_error, httpx.HTTPError):
                        # It's a standard Python bug (like a KeyError), fail immediately!
                        raise wrapper_error
                        
                    # The rest of the logic uses the unwrapped 'http_error'
                    if attempt == max_retries:
                        logger.error(f"Max retries ({max_retries}) reached. Failing permanently.")
                        raise wrapper_error # Raise the custom wrapped error so the user gets a nice message
                    
                    if isinstance(http_error, httpx.HTTPStatusError):
                        status = http_error.response.status_code
                        
                        # Intercept Token Expiry (401)
                        if status == 401:
                            logger.warning(f"⚠️ Token expired mid-flight (Attempt {attempt + 1}). Forcing token refresh...")
                            client_instance.authenticator.clear_token()
                            time.sleep(1)
                            continue
                        
                        # Permanent client errors - Do not retry
                        if status in [400, 403, 404]:
                            raise wrapper_error
                        
                        # Only retry on specific Anaplan server gateways/timeouts
                        if status not in [502, 503, 504]:
                            raise wrapper_error
                    
                    # Exponential backoff
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Network error encountered ({str(http_error)}). Retrying in {delay} seconds...")
                    time.sleep(delay)

        return wrapper
    return decorator