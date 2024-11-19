import getpass
import boto3
import threading
from functools import lru_cache
from typing import Dict, Any

# Thread-local storage for boto3 sessions and clients
_thread_local = threading.local()

def _get_session(region_name: str = None) -> boto3.Session:
    """Get or create a thread-local boto3 session."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = boto3.Session(region_name=region_name)
    return _thread_local.session

@lru_cache(maxsize=32)
def _get_rds_client(region_name: str = None) -> boto3.client:
    """Get or create a cached RDS client.
    
    The lru_cache ensures we reuse clients across different calls in the same thread,
    while the thread_local session ensures thread safety.
    """
    session = _get_session(region_name)
    return session.client(service_name="rds", region_name=region_name)

def get_aws_connection_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get AWS connection parameters with caching for better memory management
    in multithreaded environments.
    """
    params = params.copy()  # Don't modify the original dict
    
    enabled = params.pop("use_iam_auth", None)
    if not enabled:
        return params
    
    region_name = params.pop("region_name", None)
    rds_client = _get_rds_client(region_name)

    hostname = params.get("host", "localhost")

    params["password"] = rds_client.generate_db_auth_token(
        DBHostname=hostname,
        Port=params.get("port", 5432),
        DBUsername=params.get("user") or getpass.getuser(),
    )

    return params