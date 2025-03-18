import os
import snowflake.connector
from typing import Dict, Any, List, NamedTuple
from target._logger import get_logger

LOGGER = get_logger()

class SnowflakeError(Exception):
    """Custom exception for Snowflake-related errors."""
    def __init__(self, message: str, exit_code: int = 2):
        self.message = message
        self.exit_code = exit_code
        super().__init__(self.message)

class PathComponents(NamedTuple):
    """Components extracted from a path template."""
    org_id: str
    source: str
    repo_id: str

class SnowflakeStage:
    """Handles Snowflake stage operations and connections."""
    
    def __init__(self, path_components: PathComponents):
        """
        Initialize SnowflakeStage with path components.
        
        Args:
            path_components: PathComponents containing org_id, source, and repo_id
        """
        self.path_components = path_components
        self._connection_params = self._get_connection_params()
        self.stage_name = self._create_stage_name()
    
    @staticmethod
    def _get_connection_params() -> Dict[str, str]:
        """Get Snowflake connection parameters from environment variables."""
        required_params = [
            'USERNAME', 'PASSWORD', 'ACCOUNT', 
            'WAREHOUSE', 'DATABASE'
        ]
        
        params = {}
        for param in required_params:
            env_var = f'SNOWFLAKE_{param}'
            value = os.environ.get(env_var)
            if not value:
                raise ValueError(f"Missing required environment variable: {env_var}")
            params[param.lower()] = value
        
        return params
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a Snowflake query using stored connection parameters."""
        conn = snowflake.connector.connect(**self._connection_params)
        
        try:
            cur = conn.cursor(snowflake.connector.DictCursor)
            cur.execute(query)
            results = cur.fetchall()
            return results
        finally:
            cur.close()
            conn.close()
    
    def _create_stage_name(self) -> str:
        """Create a Snowflake stage name in the format T_ORGID_SOURCE.S3_STAGE."""
        clean_org_id = self.path_components.org_id.replace('-', '').upper()
        clean_source = self.path_components.source.upper()
        return f"T_{clean_org_id}_{clean_source}.S3_STAGE"
    
    def create_s3_stage(self, s3_bucket: str) -> None:
        """Create a Snowflake stage for S3 integration if it doesn't exist."""
        query = f"""
        CREATE STAGE IF NOT EXISTS {self.stage_name}
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{s3_bucket}/{self.path_components.org_id}/{self.path_components.source}'
        DIRECTORY = (
            ENABLE = TRUE, 
            REFRESH_ON_CREATE = FALSE
        )
        FILE_FORMAT = (TYPE = JSON);
        """
        self.execute_query(query)
        LOGGER.info(f"Created or verified stage: {self.stage_name}")
    
    def refresh_directory(self) -> None:
        """Refresh a Snowflake directory for the stage."""
        query = f"""
        ALTER STAGE {self.stage_name} REFRESH SUBPATH = '{self.path_components.repo_id}';
        """
        self.execute_query(query)
        LOGGER.info(f"Refreshed directory {self.stage_name} with repo {self.path_components.repo_id}")

def parse_path_template(path_template: str) -> PathComponents:
    """
    Parse a path template to extract organization ID, source, and repository ID.
    
    Args:
        path_template: String template like "orgid/source/repoid/..."
        
    Returns:
        PathComponents containing org_id, source, and repo_id
    """
    parts = path_template.split('/')
    if len(parts) < 3:
        raise ValueError(f"Path template must have at least 3 components (org_id/source/repo_id), got: {path_template}")
    
    return PathComponents(
        org_id=parts[0],
        source=parts[1],
        repo_id=parts[2]
    )

