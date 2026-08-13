from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from privacy_cloud.config import Settings


def encode_artifact(document: dict[str, Any]) -> tuple[bytes, str]:
    content = json.dumps(
        document,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return content, hashlib.sha256(content).hexdigest()


@dataclass(frozen=True)
class ArtifactReference:
    uri: str
    sha256: str


class ArtifactStore(Protocol):
    async def put_json(
        self,
        *,
        tenant_id: str,
        request_id: str,
        document: dict[str, Any],
    ) -> ArtifactReference: ...

    async def presign_get(self, uri: str, expires_seconds: int) -> str | None: ...


class LocalArtifactStore:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    async def put_json(
        self,
        *,
        tenant_id: str,
        request_id: str,
        document: dict[str, Any],
    ) -> ArtifactReference:
        content, digest = encode_artifact(document)
        path = self.root / tenant_id / f"{request_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".tmp")
        await asyncio.to_thread(temporary.write_bytes, content)
        await asyncio.to_thread(temporary.replace, path)
        return ArtifactReference(uri=str(path), sha256=digest)

    async def presign_get(self, uri: str, expires_seconds: int) -> str | None:
        del uri, expires_seconds
        return None


class S3ArtifactStore:
    def __init__(
        self,
        *,
        client: Any,
        bucket: str,
        kms_key_id: str,
        key_prefix: str = "privacy-artifacts",
    ) -> None:
        if not bucket or not kms_key_id:
            raise ValueError("S3 bucket and KMS key ID are required")
        self.client = client
        self.bucket = bucket
        self.kms_key_id = kms_key_id
        self.key_prefix = key_prefix.strip("/")

    async def put_json(
        self,
        *,
        tenant_id: str,
        request_id: str,
        document: dict[str, Any],
    ) -> ArtifactReference:
        content, digest = encode_artifact(document)
        key = f"{self.key_prefix}/{tenant_id}/{request_id}.json"
        await asyncio.to_thread(
            self.client.put_object,
            Bucket=self.bucket,
            Key=key,
            Body=content,
            ContentType="application/json",
            ServerSideEncryption="aws:kms",
            SSEKMSKeyId=self.kms_key_id,
            Metadata={"sha256": digest},
        )
        return ArtifactReference(uri=f"s3://{self.bucket}/{key}", sha256=digest)

    async def presign_get(self, uri: str, expires_seconds: int) -> str | None:
        prefix = f"s3://{self.bucket}/"
        if not uri.startswith(prefix):
            raise ValueError("artifact URI does not belong to the configured bucket")
        key = uri.removeprefix(prefix)
        return await asyncio.to_thread(
            self.client.generate_presigned_url,
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_seconds,
        )


def build_artifact_store(settings: Settings, *, client: Any | None = None) -> ArtifactStore:
    if settings.artifact_backend == "local":
        return LocalArtifactStore(settings.artifact_dir)
    if settings.artifact_backend != "s3":
        raise ValueError(f"unsupported artifact backend: {settings.artifact_backend}")
    if client is None:
        import boto3

        client = boto3.client(
            "s3",
            region_name=settings.aws_region,
            endpoint_url=settings.aws_endpoint_url or None,
        )
    return S3ArtifactStore(
        client=client,
        bucket=settings.s3_bucket,
        kms_key_id=settings.s3_kms_key_id,
        key_prefix=settings.s3_prefix,
    )
