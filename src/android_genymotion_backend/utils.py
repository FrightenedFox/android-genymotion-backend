import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def custom_requests(
    total_retries=3, backoff_factor=0.3, status_forcelist=None, allowed_methods=None, connect_timeout=5, read_timeout=15
):
    """
    Creates and returns a custom requests session with retry and timeout configurations.

    Parameters:
    - total_retries (int): Total number of retries to allow.
    - backoff_factor (float): A backoff factor to apply between retries. e.g., 0.3 would result in delays of
                              0.3s, 0.6s, 1.2s, etc.
    - status_forcelist (list): A list of HTTP status codes that will trigger a retry (e.g., [429, 500, 502, 503, 504]).
    - allowed_methods (list): A list of HTTP methods to retry on (e.g., ["HEAD", "GET", "POST", "PUT", "DELETE"]).
    - connect_timeout (float): The connection timeout in seconds.
    - read_timeout (float): The read timeout in seconds.

    Returns:
    - A configured `requests.Session` object.
    """
    # Default status codes to retry on if none are provided
    if status_forcelist is None:
        status_forcelist = [429, 500, 502, 503, 504]

    # Default allowed methods to retry if none are provided
    if allowed_methods is None:
        allowed_methods = ["HEAD", "GET", "POST", "PUT", "DELETE", "OPTIONS", "TRACE"]

    # Configure retry strategy
    retry_strategy = Retry(
        total=total_retries,
        status_forcelist=status_forcelist,
        backoff_factor=backoff_factor,
        allowed_methods=allowed_methods,
        raise_on_status=False,
    )

    # Configure the adapter with the retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)

    # Create a new session
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    # Set default timeout for all requests made with this session
    session.request = lambda method, url, **kwargs: requests.Session.request(
        session, method, url, timeout=(connect_timeout, read_timeout), **kwargs
    )

    return session
