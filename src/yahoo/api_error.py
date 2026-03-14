import sys
from typing import Any
import requests

def handle_api_error(response: requests.Response, context: str = "") -> None:
    """Check a requests.Response and cleanly exit on 401/403/404 with friendly errors.

    Args:
        response: The requests.Response object to check.
        context: Optional string describing what was being fetched (e.g., 'league metadata').
    """
    ctx_str = f" while fetching {context}" if context else ""
    
    if response.status_code == 401:
        print(f"ERROR: Unauthorized (401){ctx_str}.")
        print("Your Yahoo OAuth token may be invalid or expired. Run setup_oauth.py to refresh.")
        sys.exit(1)
        
    elif response.status_code == 403:
        print(f"ERROR: Forbidden (403){ctx_str}.")
        print("You do not have access to this resource.")
        print("Double check your league key, season year, and ensure your account has permissions.")
        sys.exit(1)
        
    elif response.status_code == 404:
        print(f"ERROR: Not Found (404){ctx_str}.")
        print("The requested resource does not exist.")
        print("Double check your league key or season year.")
        sys.exit(1)
        
    # For any other 4xx or 5xx errors, fall back to the standard exception
    response.raise_for_status()
