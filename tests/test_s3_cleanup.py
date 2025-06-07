import json
import sys
import os
import unittest
from unittest.mock import Mock, patch
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from target_s3_json.s3_cleanup import (
    has_record_entries,
    is_jsonl_file,
    cleanup_empty_jsonl_files,
    delete_s3_objects_batch,
    delete_s3_objects_individually
)
from target_s3_json.snowflake import PathComponents


class TestRecordDetection(unittest.TestCase):
    """Test the RECORD entry detection functionality"""
    
    def test_has_record_entries_with_record_on_second_line(self):
        """Test detecting files with RECORD entries on second line"""
        content = b'''{"type": "STATE", "value": {"currently_syncing": null}}
{"type": "RECORD", "stream": "test", "record": {"id": 1}}
{"type": "STATE", "value": {"currently_syncing": null}}'''
        
        assert has_record_entries(content) is True
    
    def test_has_record_entries_without_records(self):
        """Test detecting files without RECORD entries"""
        content = b'''{"type": "STATE", "value": {"currently_syncing": null}}
{"type": "SCHEMA", "stream": "test", "schema": {}}
{"type": "STATE", "value": {"currently_syncing": null}}'''
        
        assert has_record_entries(content) is False
    
    def test_has_record_entries_empty_file(self):
        """Test empty files"""
        content = b''
        assert has_record_entries(content) is False
    
    def test_has_record_entries_single_line(self):
        """Test files with only one line"""
        content = b'{"type": "STATE", "value": {"currently_syncing": null}}'
        assert has_record_entries(content) is False
    
    def test_has_record_entries_empty_second_line(self):
        """Test files with empty second line"""
        content = b'''{"type": "STATE", "value": {"currently_syncing": null}}

{"type": "SCHEMA", "stream": "test", "schema": {}}'''
        assert has_record_entries(content) is False
    
    def test_has_record_entries_invalid_json_second_line(self):
        """Test files with invalid JSON on second line"""
        content = b'''{"type": "STATE", "value": {"currently_syncing": null}}
invalid json line
{"type": "SCHEMA", "stream": "test", "schema": {}}'''
        
        assert has_record_entries(content) is False


class TestFileTypeDetection(unittest.TestCase):
    """Test JSONL file type detection"""
    
    def test_is_jsonl_file_jsonl_extension(self):
        """Test .jsonl files are detected"""  
        assert is_jsonl_file('test.jsonl') is True
        assert is_jsonl_file('path/to/test.jsonl') is True
    
    def test_is_jsonl_file_case_insensitive(self):
        """Test case insensitive detection"""
        assert is_jsonl_file('test.JSONL') is True
        assert is_jsonl_file('path/to/TEST.Jsonl') is True
    
    def test_is_jsonl_file_non_jsonl(self):
        """Test non-JSONL files are not detected"""
        assert is_jsonl_file('test.json') is False  # Only .jsonl now
        assert is_jsonl_file('test.txt') is False
        assert is_jsonl_file('test.csv') is False
        assert is_jsonl_file('test.xml') is False
        assert is_jsonl_file('test.json.gz') is False  # No compressed files
        assert is_jsonl_file('test.jsonl.gz') is False


class TestBatchDelete(unittest.TestCase):
    """Test batch delete functionality"""
    
    def test_delete_s3_objects_batch_success(self):
        """Test successful batch delete"""
        mock_client = Mock()
        mock_client.delete_objects.return_value = {
            'Deleted': [
                {'Key': 'file1.jsonl'},
                {'Key': 'file2.jsonl'}
            ]
        }
        
        result = delete_s3_objects_batch(mock_client, 'test-bucket', ['file1.jsonl', 'file2.jsonl'])
        
        assert result == 2
        mock_client.delete_objects.assert_called_once_with(
            Bucket='test-bucket',
            Delete={
                'Objects': [{'Key': 'file1.jsonl'}, {'Key': 'file2.jsonl'}],
                'Quiet': False
            }
        )
    
    def test_delete_s3_objects_batch_empty_list(self):
        """Test batch delete with empty list"""
        mock_client = Mock()
        
        result = delete_s3_objects_batch(mock_client, 'test-bucket', [])
        
        assert result == 0
        mock_client.delete_objects.assert_not_called()
    
    def test_delete_s3_objects_batch_with_errors(self):
        """Test batch delete with some errors"""
        mock_client = Mock()
        mock_client.delete_objects.return_value = {
            'Deleted': [{'Key': 'file1.jsonl'}],
            'Errors': [
                {'Key': 'file2.jsonl', 'Message': 'Access denied'}
            ]
        }
        
        result = delete_s3_objects_batch(mock_client, 'test-bucket', ['file1.jsonl', 'file2.jsonl'])
        
        assert result == 1  # Only one successfully deleted

    @patch('target_s3_json.s3_cleanup.delete_s3_objects_individually')
    def test_delete_s3_objects_batch_malformed_xml_fallback(self, mock_individual_delete):
        """Test batch delete falls back to individual deletes on MalformedXML error"""
        from botocore.exceptions import ClientError
        
        mock_client = Mock()
        mock_client.delete_objects.side_effect = ClientError(
            error_response={'Error': {'Code': 'MalformedXML', 'Message': 'XML malformed'}},
            operation_name='DeleteObjects'
        )
        
        mock_individual_delete.return_value = 2
        
        result = delete_s3_objects_batch(mock_client, 'test-bucket', ['file1.jsonl', 'file2.jsonl'])
        
        assert result == 2
        mock_individual_delete.assert_called_once_with(mock_client, 'test-bucket', ['file1.jsonl', 'file2.jsonl'])
    
    def test_batch_size_conservative_for_xml_limits(self):
        """Test that we use conservative batch sizes to avoid XML payload limits"""
        # This test documents that we use 100 objects per batch instead of 1000
        # to avoid XML payload size issues with long S3 object keys
        
        bucket = 'test-bucket'
        path_components = PathComponents(
            org_id='long-org-id-that-takes-space',
            source='github', 
            repo_id='long-repo-id-that-also-takes-space'
        )
        
        mock_client = Mock()
        
        # Create 150 mock objects with long keys (similar to real scenario)
        long_keys = []
        for i in range(150):
            long_key = f"{path_components.org_id}/{path_components.source}/{path_components.repo_id}/2025/01/01/123456/very_long_filename_that_simulates_real_world_scenario_part_{i:05d}.jsonl"
            long_keys.append({'Key': long_key, 'Size': 100})
        
        # Mock paginated response
        mock_client.get_paginator.return_value.paginate.return_value = [
            {'Contents': long_keys}
        ]
        
        # Mock all files as having no RECORD entries
        def mock_get_object(Bucket, Key):
            mock_body = Mock()
            mock_body.read.return_value = b'{"type": "STATE", "value": {}}\n{"type": "SCHEMA", "stream": "test"}'
            return {'Body': mock_body}
        
        mock_client.get_object.side_effect = mock_get_object
        mock_client.delete_objects.return_value = {'Deleted': [{'Key': 'dummy'}] * 100}  # Mock successful batch delete
        
        # Run cleanup
        result = cleanup_empty_jsonl_files(bucket, mock_client, path_components)
        
        # Should process all 150 files
        assert result['files_processed'] == 150
        assert result['files_without_records'] == 150
        
        # Should call batch delete twice: once at 100 files, once for remaining 50
        assert mock_client.delete_objects.call_count == 2


class TestIndividualDelete(unittest.TestCase):
    """Test individual delete functionality"""
    
    def test_delete_s3_objects_individually_success(self):
        """Test successful individual deletes"""
        mock_client = Mock()
        mock_client.delete_object.return_value = {}
        
        result = delete_s3_objects_individually(mock_client, 'test-bucket', ['file1.jsonl', 'file2.jsonl'])
        
        assert result == 2
        assert mock_client.delete_object.call_count == 2
        mock_client.delete_object.assert_any_call(Bucket='test-bucket', Key='file1.jsonl')
        mock_client.delete_object.assert_any_call(Bucket='test-bucket', Key='file2.jsonl')
    
    def test_delete_s3_objects_individually_no_such_key(self):
        """Test individual delete when files don't exist"""
        from botocore.exceptions import ClientError
        
        mock_client = Mock()
        mock_client.delete_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
            operation_name='DeleteObject'
        )
        
        result = delete_s3_objects_individually(mock_client, 'test-bucket', ['file1.jsonl'])
        
        assert result == 1  # Still count as successful since goal is achieved
    
    def test_delete_s3_objects_individually_malformed_xml(self):
        """Test individual delete when key causes MalformedXML - should not retry"""
        from botocore.exceptions import ClientError
        
        mock_client = Mock()
        mock_client.delete_object.side_effect = ClientError(
            error_response={'Error': {'Code': 'MalformedXML', 'Message': 'XML malformed'}},
            operation_name='DeleteObject'
        )
        
        result = delete_s3_objects_individually(mock_client, 'test-bucket', ['problematic-key.jsonl'])
        
        # Should not count as successful since the key has fundamental issues
        assert result == 0
        # Should only call delete_object once (no retries for MalformedXML)
        assert mock_client.delete_object.call_count == 1
    
    def test_delete_s3_objects_individually_mixed_results(self):
        """Test individual delete with mixed success/failure"""
        from botocore.exceptions import ClientError
        
        mock_client = Mock()
        
        def side_effect(Bucket, Key):
            if Key == 'file1.jsonl':
                return {}  # Success
            elif Key == 'file2.jsonl':
                raise ClientError(
                    error_response={'Error': {'Code': 'NoSuchKey', 'Message': 'Key not found'}},
                    operation_name='DeleteObject'
                )
            else:
                raise ClientError(
                    error_response={'Error': {'Code': 'AccessDenied', 'Message': 'Access denied'}},
                    operation_name='DeleteObject'  
                )
        
        mock_client.delete_object.side_effect = side_effect
        
        result = delete_s3_objects_individually(mock_client, 'test-bucket', ['file1.jsonl', 'file2.jsonl', 'file3.jsonl'])
        
        assert result == 2  # file1 success + file2 NoSuchKey (counted as success)


class TestCleanupIntegration(unittest.TestCase):
    """Integration tests for the cleanup functionality"""
    
    def test_cleanup_with_path_components(self):
        """Test cleanup using PathComponents"""
        # Setup bucket and client
        bucket = 'test-bucket'
        path_components = PathComponents(
            org_id='e459f0ee-9ed2-4232-bada-dc4c05bdfd10',
            source='github', 
            repo_id='bb33070e-98db-4780-9ce1-36427cc73662'
        )
        
        mock_client = Mock()
        
        # Mock S3 objects - only .jsonl files now
        mock_client.get_paginator.return_value.paginate.return_value = [
            {
                'Contents': [
                    {'Key': 'e459f0ee-9ed2-4232-bada-dc4c05bdfd10/github/bb33070e-98db-4780-9ce1-36427cc73662/file1.jsonl', 'Size': 100},
                    {'Key': 'e459f0ee-9ed2-4232-bada-dc4c05bdfd10/github/bb33070e-98db-4780-9ce1-36427cc73662/file2.jsonl', 'Size': 200}
                ]
            }
        ]
        
        # Mock file contents - one with RECORD on second line, one without
        def mock_get_object(Bucket, Key):
            mock_body = Mock()
            if 'file1.jsonl' in Key:
                # File with RECORD on second line
                mock_body.read.return_value = \
                    b'{"type": "STATE", "value": {}}\n{"type": "RECORD", "record": {"id": 1}}'
            else:
                # File without RECORD entries
                mock_body.read.return_value = \
                    b'{"type": "STATE", "value": {}}\n{"type": "SCHEMA", "stream": "test"}'
            return {'Body': mock_body}
        
        mock_client.get_object.side_effect = mock_get_object
        mock_client.delete_objects.return_value = {'Deleted': [{'Key': 'file2.jsonl'}]}
        
        # Run cleanup
        result = cleanup_empty_jsonl_files(bucket, mock_client, path_components)
        
        # Verify results
        assert result['files_processed'] == 2
        assert result['files_without_records'] == 1
        assert result['files_deleted'] == 1
        
        # Verify batch delete was called
        mock_client.delete_objects.assert_called_once()
    
    def test_cleanup_with_empty_bucket(self):
        """Test cleanup with empty bucket name"""
        bucket = ''  # Empty bucket name
        path_components = PathComponents(
            org_id='test-org',
            source='github', 
            repo_id='test-repo'
        )
        mock_client = Mock()
        mock_client.get_paginator.return_value.paginate.return_value = []
        
        # Run cleanup
        result = cleanup_empty_jsonl_files(bucket, mock_client, path_components)
        
        # Should return empty results since no objects found
        assert result == {'files_processed': 0, 'files_without_records': 0, 'files_deleted': 0}


if __name__ == '__main__':
    unittest.main()