#!/usr/bin/env python3
"""
Standalone script to clean up empty JSONL files from S3.
Based on the battle-tested code from target-s3-jsonl.
"""

import json
import logging
import sys
from typing import Dict, Any, Generator, List, Callable, Optional

import backoff
import boto3
from botocore.exceptions import ClientError
from botocore.client import BaseClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
LOGGER = logging.getLogger(__name__)


def _log_backoff_attempt(details: Dict) -> None:
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


def _retry_pattern() -> Callable:
    def giveup_on_unretryable_errors(e):
        """Don't retry on errors that won't be fixed by retrying"""
        if isinstance(e, ClientError):
            error_code = e.response.get('Error', {}).get('Code', '')
            # NoSuchKey: file is gone, no point retrying
            # MalformedXML: object key has fundamental issues, retrying won't help
            return error_code in ['NoSuchKey', 'MalformedXML']
        return False

    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=_log_backoff_attempt,
        giveup=giveup_on_unretryable_errors,
        factor=10)


def has_record_entries(content: bytes) -> bool:
    """
    Check if JSONL content contains any entries with type="RECORD"
    Optimized to check only the second line since RECORD entries are always on the second line.

    Args:
        content: Raw bytes content of the JSONL file

    Returns:
        True if file contains at least one RECORD entry, False otherwise
    """
    try:
        # Decode to text
        text_content = content.decode('utf-8')
        lines = text_content.strip().split('\n')

        # Check only the second line (index 1) since RECORD entries are always there
        if len(lines) < 2:
            return False  # No second line means no RECORD entries

        second_line = lines[1].strip()
        if not second_line:
            return False  # Empty second line means no RECORD entries

        try:
            record = json.loads(second_line)
            return record.get('type') == 'RECORD'
        except json.JSONDecodeError:
            LOGGER.warning(f"Failed to parse JSON on second line: {second_line[:100]}...")
            return False

    except Exception as e:
        LOGGER.error(f"Error processing file content: {str(e)}")
        return True  # Conservative approach: keep file if we can't process it


@_retry_pattern()
def list_s3_objects(client: BaseClient, bucket: str, prefix: str = '') -> Generator[Dict[str, Any], None, None]:
    """
    List all objects in S3 bucket with given prefix recursively

    Args:
        client: S3 client
        bucket: S3 bucket name
        prefix: Key prefix to filter objects

    Yields:
        Dict containing object metadata
    """
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                yield obj


@_retry_pattern()
def get_s3_object(client: BaseClient, bucket: str, key: str) -> bytes:
    """
    Download S3 object content

    Args:
        client: S3 client
        bucket: S3 bucket name
        key: S3 object key

    Returns:
        Object content as bytes
    """
    response = client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read()


@_retry_pattern()
def delete_s3_objects_batch(client: BaseClient, bucket: str, keys: List[str]) -> int:
    """
    Delete multiple S3 objects in batch (called with 100 objects at a time to isolate potential errors)

    Args:
        client: S3 client
        bucket: S3 bucket name
        keys: List of S3 object keys to delete

    Returns:
        Number of objects successfully deleted
    """
    if not keys:
        return 0

    try:
        response = client.delete_objects(
            Bucket=bucket,
            Delete={
                'Objects': [{'Key': key} for key in keys],
                'Quiet': False  # We want to see both successes and failures
            }
        )

        deleted_count = len(response.get('Deleted', []))

        # Log any errors
        if 'Errors' in response:
            for error in response['Errors']:
                LOGGER.error(f"Failed to delete s3://{bucket}/{error['Key']}: {error['Message']}")

        if deleted_count > 0:
            LOGGER.info(f"Batch deleted {deleted_count} objects from s3://{bucket}/")

        return deleted_count

    except ClientError as e:
        # If batch delete fails, fall back to individual deletes to isolate the problematic key
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'MalformedXML':
            LOGGER.warning(f"Batch delete failed with MalformedXML, falling back to individual deletes for {len(keys)} objects")
            return delete_s3_objects_individually(client, bucket, keys)
        else:
            raise


@_retry_pattern()
def delete_s3_objects_individually(client: BaseClient, bucket: str, keys: List[str]) -> int:
    """
    Delete S3 objects one by one as fallback when batch delete fails

    Args:
        client: S3 client
        bucket: S3 bucket name
        keys: List of S3 object keys to delete

    Returns:
        Number of objects successfully deleted
    """
    deleted_count = 0

    for key in keys:
        try:
            LOGGER.info(f"Deleting individual object: s3://{bucket}/{key}")
            client.delete_object(Bucket=bucket, Key=key)
            deleted_count += 1
            LOGGER.debug(f"Deleted individual object: s3://{bucket}/{key}")
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'NoSuchKey':
                LOGGER.debug(f"Object s3://{bucket}/{key} already deleted")
                deleted_count += 1  # Count as successful since goal is achieved
            else:
                LOGGER.error(f"Failed to delete s3://{bucket}/{key}: {str(e)}")
        except Exception as e:
            LOGGER.error(f"Unexpected error deleting s3://{bucket}/{key}: {str(e)}")

    if deleted_count > 0:
        LOGGER.info(f"Individually deleted {deleted_count} objects from s3://{bucket}/")

    return deleted_count


def is_jsonl_file(key: str) -> bool:
    """
    Check if the S3 key represents a JSONL file

    Args:
        key: S3 object key

    Returns:
        True if file appears to be JSONL format
    """
    # Check for JSONL file extension only
    return key.lower().endswith('.jsonl')


def should_process_key(key: str, main_prefix: str, additional_prefixes: Optional[List[str]] = None) -> bool:
    """
    Check if a key should be processed based on prefix filters.
    For paths like output/2025/09/01/some_prefix/file.jsonl, we check if "some_prefix"
    is in the additional_prefixes list.

    Args:
        key: S3 object key
        main_prefix: Main prefix that key must start with (e.g., "output/2025/09/")
        additional_prefixes: Optional list of additional prefixes to filter by within each day

    Returns:
        True if key should be processed, False otherwise
    """
    # Key must start with main prefix
    if not key.startswith(main_prefix):
        return False

    # If no additional prefixes specified, process all files under main prefix
    if not additional_prefixes:
        return True

    # Extract the part after the main prefix
    relative_path = key[len(main_prefix):]

    # For a path like "01/some_prefix/file.jsonl", we want to check "some_prefix"
    # Split by '/' to get path components
    path_parts = relative_path.split('/')

    # We expect at least: day/prefix/file.jsonl (3 parts minimum)
    if len(path_parts) < 3:
        return False

    # The prefix we're checking is the second part (after the day)
    # path_parts[0] is the day (e.g., "01")
    # path_parts[1] is the prefix we want to check (e.g., "some_prefix")
    prefix_to_check = path_parts[1]

    # Check if this prefix is in our allowed list
    return prefix_to_check in additional_prefixes


def cleanup_empty_jsonl_files(bucket: str, prefix: str, dry_run: bool = False,
                              additional_prefixes: Optional[List[str]] = None,
                              dry_run_output_file: Optional[str] = None) -> Dict[str, int]:
    """
    Main function to clean up JSONL files without RECORD entries

    Args:
        bucket: S3 bucket name
        prefix: S3 prefix to search under
        dry_run: If True, only list files that would be deleted without actually deleting
        additional_prefixes: Optional list of additional prefixes to filter by (within main prefix)
        dry_run_output_file: Optional file path to write dry run results to

    Returns:
        Dict with cleanup statistics
    """
    # Create S3 client
    client = boto3.client('s3')

    # Open output file if needed (opened once and kept open for efficiency)
    dry_run_file_handle = None

    try:
        LOGGER.info(f"Starting cleanup of JSONL files in s3://{bucket}/{prefix}")
        if additional_prefixes:
            LOGGER.info(f"Filtering to prefixes within each day: {', '.join(additional_prefixes)}")
        if dry_run:
            LOGGER.info("DRY RUN MODE - No files will be deleted")
            if dry_run_output_file:
                LOGGER.info(f"Dry run results will be written to: {dry_run_output_file}")
                # Open file for writing (will overwrite if exists)
                dry_run_file_handle = open(dry_run_output_file, 'w')

        files_processed = 0
        files_without_records = 0
        files_deleted = 0
        files_to_delete = []  # Collect files for batch deletion

        # List all objects in the bucket with the given prefix
        for obj in list_s3_objects(client, bucket, prefix):
            key = obj['Key']
            size = obj['Size']

            # Skip if not a JSONL file
            if not is_jsonl_file(key):
                continue

            # Skip if key doesn't match additional prefix filters
            if not should_process_key(key, prefix, additional_prefixes):
                LOGGER.debug(f"Skipping {key} - doesn't match prefix filters")
                continue

            files_processed += 1
            LOGGER.debug(f"Processing {key} ({size} bytes)")

            try:
                # Download and immediately check file content
                if not has_record_entries(get_s3_object(client, bucket, key)):
                    files_without_records += 1

                    if dry_run:
                        LOGGER.info(f"[DRY RUN] Would delete: s3://{bucket}/{key}")
                        if dry_run_file_handle:
                            dry_run_file_handle.write(f"s3://{bucket}/{key}\n")
                            dry_run_file_handle.flush()  # Ensure it's written immediately
                    else:
                        LOGGER.info(f"Queueing for deletion: s3://{bucket}/{key}")
                        files_to_delete.append(key)

                        # Batch delete when we hit 100 files (conservative limits to isolate malformed XML errors)
                        if len(files_to_delete) >= 100:
                            deleted_count = delete_s3_objects_batch(client, bucket, files_to_delete)
                            files_deleted += deleted_count
                            files_to_delete = []  # Reset for next batch

            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'NoSuchKey':
                    LOGGER.warning(f"File s3://{bucket}/{key} no longer exists (likely deleted by concurrent process)")
                    continue
                else:
                    LOGGER.error(f"AWS error processing s3://{bucket}/{key}: {str(e)}")
                    continue
            except Exception as e:
                LOGGER.error(f"Error processing s3://{bucket}/{key}: {str(e)}")
                continue

        # Delete any remaining files in final batch
        if not dry_run and files_to_delete:
            deleted_count = delete_s3_objects_batch(client, bucket, files_to_delete)
            files_deleted += deleted_count

        # Close the dry run output file if it was opened
        if dry_run_file_handle:
            dry_run_file_handle.close()
            LOGGER.info(f"Wrote {files_without_records} file paths to {dry_run_output_file}")

        LOGGER.info(f"Cleanup completed:")
        LOGGER.info(f"  Files processed: {files_processed}")
        LOGGER.info(f"  Files without RECORD entries: {files_without_records}")
        if not dry_run:
            LOGGER.info(f"  Files deleted: {files_deleted}")
        else:
            LOGGER.info(f"  Files that would be deleted: {files_without_records}")

        return {
            'files_processed': files_processed,
            'files_without_records': files_without_records,
            'files_deleted': files_deleted if not dry_run else 0
        }

    except Exception as e:
        LOGGER.error(f"Cleanup failed: {str(e)}")
        raise
    finally:
        # Ensure file is closed even if an error occurs
        if dry_run_file_handle:
            dry_run_file_handle.close()


def delete_from_file(file_path: str, dry_run: bool = False) -> Dict[str, int]:
    """
    Delete S3 objects listed in a file

    Args:
        file_path: Path to file containing S3 paths to delete (one per line)
        dry_run: If True, only show what would be deleted without actually deleting

    Returns:
        Dict with deletion statistics
    """
    client = boto3.client('s3')

    files_processed = 0
    files_deleted = 0
    files_to_delete = []
    current_bucket = None

    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line or not line.startswith('s3://'):
                    continue

                files_processed += 1

                # Parse S3 path
                path_parts = line[5:].split('/', 1)  # Remove 's3://' prefix
                if len(path_parts) != 2:
                    LOGGER.warning(f"Invalid S3 path format: {line}")
                    continue

                bucket = path_parts[0]
                key = path_parts[1]

                if current_bucket is None:
                    current_bucket = bucket
                elif current_bucket != bucket:
                    # Process accumulated files for current bucket before switching
                    if not dry_run and files_to_delete:
                        deleted_count = delete_s3_objects_batch(client, current_bucket, files_to_delete)
                        files_deleted += deleted_count
                        files_to_delete = []
                    current_bucket = bucket

                if dry_run:
                    LOGGER.info(f"[DRY RUN] Would delete: {line}")
                else:
                    files_to_delete.append(key)

                    # Batch delete when we hit 100 files
                    if len(files_to_delete) >= 100:
                        deleted_count = delete_s3_objects_batch(client, bucket, files_to_delete)
                        files_deleted += deleted_count
                        files_to_delete = []

        # Delete any remaining files in final batch
        if not dry_run and files_to_delete and current_bucket:
            deleted_count = delete_s3_objects_batch(client, current_bucket, files_to_delete)
            files_deleted += deleted_count

        LOGGER.info(f"Deletion completed:")
        LOGGER.info(f"  Files processed from list: {files_processed}")
        if dry_run:
            LOGGER.info(f"  Files that would be deleted: {files_processed}")
        else:
            LOGGER.info(f"  Files deleted: {files_deleted}")

        return {
            'files_processed': files_processed,
            'files_deleted': files_deleted if not dry_run else 0
        }

    except FileNotFoundError:
        LOGGER.error(f"File not found: {file_path}")
        raise
    except Exception as e:
        LOGGER.error(f"Error processing file list: {str(e)}")
        raise


def main():
    """Main entry point for the script"""
    import argparse

    parser = argparse.ArgumentParser(description='Clean up empty JSONL files from S3')

    # Mode selection
    parser.add_argument('--delete-from-file', type=str, default=None,
                        help='Delete S3 objects listed in this file (one S3 path per line). When used, --bucket and --prefix are not required.')

    # Standard mode arguments
    parser.add_argument('--bucket', help='S3 bucket name (required unless using --delete-from-file)')
    parser.add_argument('--prefix', help='S3 prefix to search under (required unless using --delete-from-file)')
    parser.add_argument('--additional-prefixes', type=str, default=None,
                        help='Comma-separated list of prefixes to filter within each day directory (e.g., "prefix1,prefix2")')
    parser.add_argument('--dry-run', action='store_true', help='List files that would be deleted without actually deleting')
    parser.add_argument('--dry-run-output', type=str, default=None,
                        help='File path to write dry run results to (one S3 path per line)')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    args = parser.parse_args()

    if args.debug:
        LOGGER.setLevel(logging.DEBUG)

    # Determine which mode to run in
    if args.delete_from_file:
        # Delete from file mode
        LOGGER.info(f"Running in delete-from-file mode using: {args.delete_from_file}")

        try:
            result = delete_from_file(
                file_path=args.delete_from_file,
                dry_run=args.dry_run
            )
            sys.exit(0)
        except Exception as e:
            LOGGER.error(f"Script failed: {str(e)}")
            sys.exit(1)

    else:
        # Standard scan-and-delete mode
        if not args.bucket or not args.prefix:
            parser.error("--bucket and --prefix are required when not using --delete-from-file")

        # Parse additional prefixes if provided
        additional_prefixes = None
        if args.additional_prefixes:
            additional_prefixes = [p.strip() for p in args.additional_prefixes.split(',') if p.strip()]
            if additional_prefixes:
                LOGGER.info(f"Additional prefixes to filter: {additional_prefixes}")

        try:
            result = cleanup_empty_jsonl_files(
                bucket=args.bucket,
                prefix=args.prefix,
                dry_run=args.dry_run,
                additional_prefixes=additional_prefixes,
                dry_run_output_file=args.dry_run_output
            )
            sys.exit(0)
        except Exception as e:
            LOGGER.error(f"Script failed: {str(e)}")
            sys.exit(1)


if __name__ == '__main__':
    main()