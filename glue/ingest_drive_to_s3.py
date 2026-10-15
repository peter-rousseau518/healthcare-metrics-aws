"""
ingest_drive_to_s3.py  (AWS Glue Python shell job)

Copies the CMS CSV files from a Google Drive folder into the lakehouse
landing zone:  s3://<bucket>/landing/<table>/release=<YYYY-MM>/<file name>

- Authenticates to Google as a service account whose JSON key is stored in
  AWS Secrets Manager (ADR-003). The key never touches disk or logs.
- Routes each file to its table using glue/tables.json (file-name patterns).
- Skips files whose Drive MD5 matches the copy already in S3, so scheduled
  runs only move new or changed files (ADR-004).
- Fails the job if any expected table has no file, so the workflow stops.

Job arguments: --bucket --folder_id --release --secret_id
"""
import io
import json
import logging
import re
import sys
import tempfile

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger("ingest")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def get_args(names):
    try:
        from awsglue.utils import getResolvedOptions
        return getResolvedOptions(sys.argv, names)
    except ImportError:  # local testing
        args = {}
        for n in names:
            args[n] = sys.argv[sys.argv.index(f"--{n}") + 1]
        return args


def drive_client(secret_id):
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    secret = boto3.client("secretsmanager").get_secret_value(SecretId=secret_id)["SecretString"]
    info = json.loads(secret)
    if "private_key" not in info:
        raise RuntimeError(f"Secret {secret_id} does not contain a service account key yet")
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_folder(drive, folder_id):
    files, token = [], None
    while True:
        resp = drive.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            fields="nextPageToken, files(id, name, mimeType, size, md5Checksum, modifiedTime)",
            pageSize=100, pageToken=token,
            supportsAllDrives=True, includeItemsFromAllDrives=True,
        ).execute()
        files += resp.get("files", [])
        token = resp.get("nextPageToken")
        if not token:
            return files


def match_table(name, tables):
    for t in tables:
        if re.match(t["pattern"], name):
            return t["name"]
    return None


def already_landed(s3, bucket, key, md5):
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        return head.get("Metadata", {}).get("drive-md5") == md5
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise


def download(drive, file_id, fh):
    from googleapiclient.http import MediaIoBaseDownload
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    downloader = MediaIoBaseDownload(fh, request, chunksize=32 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()


def main():
    args = get_args(["bucket", "folder_id", "release", "secret_id"])
    bucket, release = args["bucket"], args["release"]
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", release):
        raise ValueError(f"--release must be YYYY-MM, got {release}")

    s3 = boto3.client("s3")
    tables = json.loads(s3.get_object(Bucket=bucket, Key="scripts/glue/tables.json")["Body"].read())["tables"]
    drive = drive_client(args["secret_id"])
    files = list_folder(drive, args["folder_id"])
    log.info("Found %d items in the Drive folder", len(files))

    seen, uploaded, skipped, ignored = set(), 0, 0, 0
    for f in sorted(files, key=lambda x: x["name"]):
        table = match_table(f["name"], tables)
        if not table:
            log.info("IGNORE  %s (not a listed source table)", f["name"])
            ignored += 1
            continue
        seen.add(table)
        key = f"landing/{table}/release={release}/{f['name']}"
        md5 = f.get("md5Checksum", "")
        if md5 and already_landed(s3, bucket, key, md5):
            log.info("SKIP    %s (unchanged)", f["name"])
            skipped += 1
            continue
        with tempfile.TemporaryFile() as tmp:
            download(drive, f["id"], tmp)
            size_mb = tmp.tell() / 1_048_576
            tmp.seek(0)
            s3.upload_fileobj(tmp, bucket, key, ExtraArgs={
                "ContentType": "text/csv",
                "Metadata": {"drive-md5": md5, "drive-file-id": f["id"],
                             "drive-modified-time": f.get("modifiedTime", ""), "source-system": "google-drive"},
            })
        log.info("UPLOAD  %s -> s3://%s/%s (%.1f MB)", f["name"], bucket, key, size_mb)
        uploaded += 1

    missing = sorted({t["name"] for t in tables} - seen)
    log.info("Done: %d uploaded, %d skipped, %d ignored", uploaded, skipped, ignored)
    if missing:
        raise RuntimeError(f"No file found for {len(missing)} table(s): {', '.join(missing)}")


if __name__ == "__main__":
    main()
