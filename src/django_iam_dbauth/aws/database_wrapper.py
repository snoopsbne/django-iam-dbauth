import getpass
import boto3
import threading
import time
from functools import lru_cache
from typing import Dict, Any, Tuple

# Thread-local storage for boto3 sessions and clients
_thread_local = threading.local()

# 15 minutes in seconds (AWS RDS auth tokens expire after 15 minutes)
TOKEN_EXPIRATION_TIME = 10 * 60

def _get_session(region_name: str = None) -> boto3.Session:
    """Get or create a thread-local boto3 session."""
    if not hasattr(_thread_local, 'session'):
        _thread_local.session = boto3.Session(region_name=region_name)
    return _thread_local.session

@lru_cache(maxsize=32)
def _get_rds_client(region_name: str = None) -> boto3.client:
    """Get or create a cached RDS client."""
    session = _get_session(region_name)
    return session.client(service_name="rds", region_name=region_name)

class TokenCache:
    def __init__(self):
        self._cache: Dict[Tuple, Tuple[str, float]] = {}
        self._lock = threading.Lock()

    def get_token(self, key: Tuple) -> str | None:
        """Get a cached token if it's still valid."""
        with self._lock:
            if key in self._cache:
                token, expiration = self._cache[key]
                if time.time() < expiration:
                    return token
                del self._cache[key]
            return None

    def set_token(self, key: Tuple, token: str):
        """Cache a token with expiration time."""
        with self._lock:
            self._cache[key] = (token, time.time() + TOKEN_EXPIRATION_TIME - 30)  # 30s buffer

# Global token cache instance
_token_cache = TokenCache()

def get_aws_connection_params(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Get AWS connection parameters with caching for better memory management
    in multithreaded environments. Includes token expiration handling.
    """
    params = params.copy()
    
    enabled = params.pop("use_iam_auth", None)
    if not enabled:
        return params
    
    region_name = params.pop("region_name", None)
    hostname = params.get("host", "localhost")
    port = params.get("port", 5432)
    username = params.get("user") or getpass.getuser()

    # Create a cache key from the connection parameters
    cache_key = (hostname, port, username, region_name)
    
    # Try to get cached token
    cached_token = _token_cache.get_token(cache_key)
    if cached_token:
        params["password"] = cached_token
        return params

    # Generate new token if cached token not found or expired
    rds_client = _get_rds_client(region_name)
    token = rds_client.generate_db_auth_token(
        DBHostname=hostname,
        Port=port,
        DBUsername=username,
    )
    
    # Cache the new token
    _token_cache.set_token(cache_key, token)
    params["password"] = token

    return params