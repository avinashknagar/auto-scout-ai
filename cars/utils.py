import time
import functools
import logging
import os

import requests


class AuthExpiredError(Exception):
    """Raised when API returns 401/403, indicating token/session expired."""
    pass


def retry_with_backoff(max_retries=3, backoff_base=2.0,
                       retriable_statuses=(429, 500, 502, 503, 504)):
    """Decorator for methods that make HTTP requests."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__qualname__)
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.HTTPError as e:
                    status = e.response.status_code if e.response is not None else None
                    if status in (401, 403):
                        logger.error(f"Auth failed (HTTP {status}). Token likely expired.")
                        raise AuthExpiredError(
                            f"HTTP {status}: update tokens.yaml"
                        ) from e
                    if status in retriable_statuses and attempt < max_retries:
                        wait = backoff_base ** attempt
                        logger.warning(
                            f"HTTP {status}, retrying in {wait:.0f}s "
                            f"(attempt {attempt + 1}/{max_retries})"
                        )
                        time.sleep(wait)
                        continue
                    raise
                except requests.exceptions.ConnectionError:
                    if attempt < max_retries:
                        wait = backoff_base ** attempt
                        logger.warning(f"Connection error, retrying in {wait:.0f}s")
                        time.sleep(wait)
                        continue
                    raise
                except requests.exceptions.Timeout:
                    if attempt < max_retries:
                        wait = backoff_base ** attempt
                        logger.warning(f"Timeout, retrying in {wait:.0f}s")
                        time.sleep(wait)
                        continue
                    raise
        return wrapper
    return decorator


class RateLimiter:
    """Ensures minimum delay between calls."""

    def __init__(self, min_delay: float):
        self.min_delay = min_delay
        self._last_call = 0.0

    def wait(self):
        now = time.monotonic()
        elapsed = now - self._last_call
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
        self._last_call = time.monotonic()


def setup_logging(config: dict):
    """Configure logging from config."""
    level = getattr(logging, config["logging"].get("level", "INFO").upper(), logging.INFO)
    handlers = [logging.StreamHandler()]

    log_file = config["logging"].get("file")
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=handlers,
    )
