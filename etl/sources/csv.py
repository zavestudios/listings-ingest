import csv
import os
import tempfile
from urllib.parse import urlparse


def _download_s3(s3_url: str) -> str:
    import boto3

    parsed = urlparse(s3_url)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    endpoint = os.environ.get("MINIO_ENDPOINT")
    access_key = os.environ.get("MINIO_ACCESS_KEY")
    secret_key = os.environ.get("MINIO_SECRET_KEY")
    if not all([endpoint, access_key, secret_key]):
        raise EnvironmentError(
            "MINIO_ENDPOINT, MINIO_ACCESS_KEY, and MINIO_SECRET_KEY must be set for s3:// paths"
        )

    import botocore

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        verify=False,
        config=botocore.config.Config(s3={"addressing_style": "path"}),
    )

    tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
    client.download_fileobj(bucket, key, tmp)
    tmp.close()
    return tmp.name


def read_listings(path: str) -> list[dict]:
    tmp_path = None
    if path.startswith("s3://"):
        tmp_path = _download_s3(path)
        path = tmp_path

    try:
        with open(path, newline="", encoding="utf-8") as handle:
            return list(csv.DictReader(handle))
    finally:
        if tmp_path:
            os.unlink(tmp_path)
