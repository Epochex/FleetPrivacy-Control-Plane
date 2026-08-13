import hashlib
import hmac

from fastapi import Header, HTTPException

from privacy_cloud.config import get_settings


def hash_subject(value: str) -> str:
    normalized = value.strip().lower().encode("utf-8")
    return hashlib.sha256(normalized).hexdigest()


async def tenant_context(
    x_tenant_id: str = Header(min_length=2, max_length=64),
    x_api_key: str = Header(),
) -> str:
    if not hmac.compare_digest(x_api_key, get_settings().api_key):
        raise HTTPException(status_code=401, detail="invalid API key")
    return x_tenant_id
