"""Fernet credential decryption for cleanup route."""

from __future__ import annotations

import json
import logging
from base64 import urlsafe_b64encode
from hashlib import sha256

from cryptography.fernet import Fernet

logger = logging.getLogger("etl.encryption")


def _derive_fernet_key(key: str) -> bytes:
    """Derive Fernet key from hex (64 chars) or use as-is if already base64."""
    key = key.strip()
    if len(key) == 64 and all(c in "0123456789abcdefABCDEF" for c in key):
        raw = bytes.fromhex(key)
        if len(raw) != 32:
            raise ValueError("ENCRYPTION_KEY hex must decode to 32 bytes")
        return urlsafe_b64encode(raw)
    if len(key) == 44:
        return key.encode()
    raise ValueError("ENCRYPTION_KEY must be 64 hex chars or 44-char base64 Fernet key")


def decrypt_credentials(encrypted_value: str, key: str) -> dict:
    """
    Decrypt a Fernet-encrypted JSON string.
    key: hex-encoded 32-byte key from ENCRYPTION_KEY env var.
    Returns dict of credential fields.
    Never logs the decrypted value or any field named 'password'.
    """
    if not encrypted_value or not key:
        raise ValueError("encrypted_value and key are required")
    fernet = Fernet(_derive_fernet_key(key))
    decrypted = fernet.decrypt(encrypted_value.encode() if isinstance(encrypted_value, str) else encrypted_value)
    data = json.loads(decrypted.decode())
    if not isinstance(data, dict):
        raise ValueError("Decrypted value must be a JSON object")
    return data
