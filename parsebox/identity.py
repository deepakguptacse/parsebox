"""Filesystem-based user identity for parsebox.

Generates a UUID on first run and persists it at ~/.parsebox/.user_id.
No login, no accounts -- just a persistent local identifier.
"""

import uuid
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

_USER_ID_FILE = Path.home() / ".parsebox" / ".user_id"


def get_or_create_user_id() -> str:
    """Get existing user ID from disk or create a new one."""
    if _USER_ID_FILE.exists():
        user_id = _USER_ID_FILE.read_text().strip()
        if user_id:
            logger.debug("Returning existing user ID: %s", user_id)
            return user_id

    new_id = str(uuid.uuid4())
    _USER_ID_FILE.parent.mkdir(parents=True, exist_ok=True)
    _USER_ID_FILE.write_text(new_id)
    logger.info("Created new user ID: %s", new_id)
    return new_id
