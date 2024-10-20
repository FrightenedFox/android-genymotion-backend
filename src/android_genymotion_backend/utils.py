import logging
from typing import Optional

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import requests
from requests.auth import HTTPBasicAuth


def genymotion_request(
    address: str,
    instance_id: str,
    method: str,
    endpoint: str,
    data=None,
    params=None,
    verify_ssl=True,
    files=None,
    stream=False,
    logger: Optional[logging.Logger] = None,
):
    """
    Makes an authenticated request to the Genymotion API.

    Args:
        address (str): The secure address (domain name).
        instance_id (str): The EC2 instance ID (used as password).
        method (str): HTTP method ('GET', 'POST', 'PUT', 'DELETE').
        endpoint (str): API endpoint (e.g., '/android/shell').
        data (dict): JSON data to send in the body of the request.
        params (dict): Query parameters.
        verify_ssl (bool): Whether to verify SSL certificates.
        files: Files to send in the request.
        stream (bool): Whether to stream the response.
        logger (Optional[logging.Logger]): Logger object.

    Returns:
        Response object.
    """
    url = f"https://{address}/api/v1{endpoint}"
    auth = HTTPBasicAuth("genymotion", instance_id)  # Password is the instance ID

    headers = {}
    if data is not None:
        headers["Content-Type"] = "application/json"

    if logger:
        logger.info(f"Making request to Genymotion API: {method} {url}")
    response = requests.request(
        method=method,
        url=url,
        auth=auth,
        json=data,
        params=params,
        verify=verify_ssl,
        files=files,
        stream=stream,
        headers=headers,
    )

    response.raise_for_status()  # Raise an exception for HTTP errors
    return response


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


def execute_shell_command(
    address: str,
    instance_id: str,
    commands: str | list[str],
    logger: Optional[logging.Logger] = None,
verify_ssl: bool = True,
) -> Response:
    """
    Executes a shell command on the device via the Genymotion API.

    Args:
        address (str): The secure address.
        instance_id (str): The instance ID.
        commands (str or list[str]): The shell command(s) to execute.
        logger (Optional[logging.Logger]): Logger object.
        verify_ssl (bool): Whether to verify SSL certificates.
    """
    endpoint = "/android/shell"
    data = {"commands": commands if isinstance(commands, list) else [commands], "timeout_in_seconds": 10}

    if logger:
        logger.info(f"Executing shell command on {address}: {commands}")
    response = genymotion_request(
        address=address, instance_id=instance_id, method="POST", endpoint=endpoint, data=data, verify_ssl=verify_ssl
    )
    return response
