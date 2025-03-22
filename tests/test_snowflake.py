import pytest
import json
import re
from pathlib import Path
from unittest.mock import patch, MagicMock
from target_s3_json.snowflake import parse_path_template, PathComponents, SnowflakeStage

def test_parse_path_template_valid():
    """Test parsing a valid path template."""
    path = "my-org/github/edb7cb5f-ccea-4cfe-b9ad-5e81956f1349"
    result = parse_path_template(path)
    
    assert isinstance(result, PathComponents)
    assert result.org_id == "my-org"
    assert result.source == "github"
    assert result.repo_id == "edb7cb5f-ccea-4cfe-b9ad-5e81956f1349"

def test_parse_path_template_with_additional_paths():
    """Test parsing a path template with additional path components."""
    path = "my-org/github/edb7cb5f-ccea-4cfe-b9ad-5e81956f1349/some/other/path"
    result = parse_path_template(path)
    
    assert isinstance(result, PathComponents)
    assert result.org_id == "my-org"
    assert result.source == "github"
    assert result.repo_id == "edb7cb5f-ccea-4cfe-b9ad-5e81956f1349"

def test_parse_path_template_minimal():
    """Test parsing a path template with exactly 3 components."""
    path = "org/src/notarepo"
    result = parse_path_template(path)
    
    assert isinstance(result, PathComponents)
    assert result.org_id == "org"
    assert result.source == "src"
    assert result.repo_id == ""

def test_parse_path_template_invalid():
    """Test parsing an invalid path template with insufficient components."""
    path = "my-org/github"
    
    with pytest.raises(ValueError) as exc_info:
        parse_path_template(path)
    
    assert "Path template must have at least 3 components" in str(exc_info.value)
    assert "got: my-org/github" in str(exc_info.value)

def test_parse_path_template_empty():
    """Test parsing an empty path template."""
    path = ""
    
    with pytest.raises(ValueError) as exc_info:
        parse_path_template(path)
    
    assert "Path template must have at least 3 components" in str(exc_info.value)
    assert "got: " in str(exc_info.value)

def test_parse_path_template_path_templates():
    """Test parsing path templates."""
    # Read path_templates from JSON file
    test_data_path = Path(__file__).parent / "resources" / "path_templates.json"
    with open(test_data_path, 'r') as f:
        path_templates_json = json.load(f)
    
    # UUID pattern for validation
    uuid_pattern = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    
    for path_obj in path_templates_json:
        result = parse_path_template(path_obj["path"])
        
        assert isinstance(result, PathComponents)
        # Verify the UUID format for org_id
        assert len(result.org_id) == 36  # UUID length
        assert uuid_pattern.match(result.org_id), f"Invalid UUID format for org_id: {result.org_id}"       
        
        # Verify the repo_id is either a UUID or a date template
        if result.source == "azuretickets":
            # For azuretickets, repo_id should be a date template
            assert result.repo_id == "", f"Expected empty repo_id for azuretickets, got: {result.repo_id}"
        else:
            # For other sources, repo_id should be a UUID
            assert len(result.repo_id) == 36, f"Invalid UUID length for repo_id: {result.repo_id}"
            assert uuid_pattern.match(result.repo_id), f"Invalid UUID format for repo_id: {result.repo_id}" 

def test_enable_directory_on_stage():
    """Test that enable_directory_on_stage generates the correct SQL query."""
    # Create a PathComponents with a known UUID and source
    components = PathComponents(
        org_id="e459f0ee-9ed2-4232-bada-dc4c05bdfd10",
        source="github",
        repo_id="f159f0ee-9ed2-4232-bada-dc4c05bdfd10"
    )
    
    # Create SnowflakeStage instance
    stage = SnowflakeStage(components)
    
    # Mock execute_query to capture the query
    with patch.object(stage, 'execute_query') as mock_execute:
        # Call the function
        stage.enable_directory_on_stage()
        
        # Verify execute_query was called with the correct query
        mock_execute.assert_called_once_with(
            "ALTER STAGE T_E459F0EE9ED24232BADADC4C05BDFD10_GITHUB.S3_STAGE SET DIRECTORY = (ENABLE = TRUE);"
        ) 