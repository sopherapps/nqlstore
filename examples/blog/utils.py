"""Some random utilities for the app"""

from datetime import datetime


def current_timestamp() -> str:
    """Gets the current timestamp as an timezone naive ISO format string

    Returns:
        string of the current datetime
    """
    return datetime.now().isoformat()
