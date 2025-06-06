#!/usr/bin/env python3

import json
from typing import Dict, Any, Generator, List, Callable

import backoff
import boto3
from botocore.exceptions import ClientError
from botocore.client import BaseClient

from .snowflake import PathComponents
from target._logger import get_logger

LOGGER = get_logger()


def _log_backoff_attempt(details: Dict) -> None:
    LOGGER.info("Error detected communicating with Amazon, triggering backoff: %d try", details.get("tries"))


def _retry_pattern() -> Callable:
    return backoff.on_exception(
        backoff.expo,
        ClientError,
        max_tries=5,
        on_backoff=_log_backoff_attempt,
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
    Delete multiple S3 objects in batch (up to 1000 at a time)
    
    Args:
        client: S3 client
        bucket: S3 bucket name
        keys: List of S3 object keys to delete
        
    Returns:
        Number of objects successfully deleted
    """
    if not keys:
        return 0
    
    delete_objects = [{'Key': key} for key in keys]
    
    response = client.delete_objects(
        Bucket=bucket,
        Delete={'Objects': delete_objects}
    )
    
    deleted_count = len(response.get('Deleted', []))
    
    # Log any errors
    if 'Errors' in response:
        for error in response['Errors']:
            LOGGER.error(f"Failed to delete s3://{bucket}/{error['Key']}: {error['Message']}")
    
    if deleted_count > 0:
        LOGGER.info(f"Batch deleted {deleted_count} objects from s3://{bucket}/")
    
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


def cleanup_empty_jsonl_files(bucket: str, client: BaseClient, path_components: PathComponents) -> Dict[str, int]:
    """
    Main function to clean up JSONL files without RECORD entries
    
    Args:
        bucket: S3 bucket name
        client: S3 client to use for operations
        path_components: Path components to construct the S3 prefix (org_id/source/repo_id)
        
    Returns:
        Dict with cleanup statistics: {'files_processed': int, 'files_without_records': int, 'files_deleted': int}
    """
    
    prefix = f"{path_components.org_id}/{path_components.source}/{path_components.repo_id}"
    
    try:
        
        LOGGER.info(f"Starting cleanup of JSONL files in s3://{bucket}/{prefix}")
        
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
                
            files_processed += 1
            LOGGER.debug(f"Processing {key} ({size} bytes)")
            
            try:
                # Download and immediately check file content, don't hold reference
                if not has_record_entries(get_s3_object(client, bucket, key)):
                    files_without_records += 1
                    LOGGER.info(f"File s3://{bucket}/{key} has no RECORD entries")
                    
                    files_to_delete.append(key)
                    
                    # Batch delete when we hit 1000 files (S3 limit)
                    if len(files_to_delete) >= 1000:
                        deleted_count = delete_s3_objects_batch(client, bucket, files_to_delete)
                        files_deleted += deleted_count
                        files_to_delete = []  # Reset for next batch
                        
            except Exception as e:
                LOGGER.error(f"Error processing s3://{bucket}/{key}: {str(e)}")
                continue
        
        # Delete any remaining files in final batch
        if files_to_delete:
            deleted_count = delete_s3_objects_batch(client, bucket, files_to_delete)
            files_deleted += deleted_count
        
        LOGGER.info(f"Cleanup completed:")
        LOGGER.info(f"  Files processed: {files_processed}")
        LOGGER.info(f"  Files without RECORD entries: {files_without_records}")
        LOGGER.info(f"  Files deleted: {files_deleted}")
        
        return {
            'files_processed': files_processed,
            'files_without_records': files_without_records,
            'files_deleted': files_deleted
        }
            
    except Exception as e:
        LOGGER.error(f"Cleanup failed: {str(e)}")
        raise
