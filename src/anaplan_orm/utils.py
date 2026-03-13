import httpx
import logging
import time
from functools import wraps

logger = logging.getLogger(__name__)

def retry_network_errors(max_retries: int = 3, base_delay: float = 1.0):
    """
    A decorator that catches transient httpx network errors and retries the function 
    using exponential backoff.
    """
    if max_retries <= 0:
        raise ValueError("max_retries must be greater than 0")

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):

            for attempt in range(max_retries + 1):
                try:
                    # Attempt to execute the method
                    return func(*args, **kwargs)
                
                except httpx.HTTPError as e:
                    if attempt == max_retries:
                        logger.error(f"Max retries ({max_retries}) reached. Failing permanently.")
                        raise
                    
                    # Check for specific HTTP Status Codes
                    if isinstance(e, httpx.HTTPStatusError):
                        status = e.response.status_code
                        
                        # Permanent errors - Does not retry
                        if status in [400, 401, 403, 404]:
                            raise 
                        
                        if status not in [502, 503, 504]:
                            raise
                    
                    # Error must be either a 50x error OR network timeout.
                    # Calculate the backoff delay exponentially
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Network error encountered ({str(e)}). Retrying in {delay} seconds...")
                    time.sleep(delay)

        return wrapper
    return decorator