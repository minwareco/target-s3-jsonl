"""
This module handles Snowflake integration for the target-s3-jsonl Singer target.
It provides functionality to:
1. Parse S3 path templates into components (org_id, source, repo_id)
2. Create and Snowflake schemas for the source if necessary 
3. Create snowflake stage pointing as the s3 bucket if necessary
4. Refresh Snowflake stage directory with S3 contents using the repo_id (if we have one) as the subpath (incremental loading)

The module supports two formats of path in the path_template:
- org_id/source/repo_id (e.g., GitHub, GitLab)
- org_id/source/date (e.g., Azure Tickets)
"""

import os
import snowflake.connector
from typing import Dict, Any, List, NamedTuple
from target._logger import get_logger
import re

LOGGER = get_logger()

class PathComponents(NamedTuple):
    """Components extracted from a path template."""
    org_id: str
    source: str
    repo_id: str

class SnowflakeStage:
    """Handles Snowflake stage operations and connections."""    
    
    # UUID pattern for validation
    UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.I)
    
    def __init__(self, path_components: PathComponents):
        """
        Initialize SnowflakeStage with path components.
        
        Args:
            path_components: PathComponents containing org_id, source, and repo_id
        """
        self._conn = None  # Connection will be created on first use
        self.path_components = path_components
        self._connection_params = self._get_connection_params()
        self.schema_name = self._create_schema_name()
        self.stage_name = self._create_stage_name()
    
    @staticmethod
    def _get_connection_params() -> Dict[str, str]:
        """Get Snowflake connection parameters from environment variables."""
        # Map environment variable names to Snowflake connector parameter names
        param_mapping = {
            'USERNAME': 'user',
            'PASSWORD': 'password',
            'ACCOUNT': 'account',
            'WAREHOUSE': 'warehouse',
            'DATABASE': 'database',
            'ROLE': 'role'
        }
        
        params = {}
        for env_param, connector_param in param_mapping.items():
            env_var = f'SNOWFLAKE_{env_param}'
            value = os.environ.get(env_var)
            if not value:
                raise ValueError(f"Missing required environment variable: {env_var}")
            params[connector_param] = value
            
            # Log connection parameters (except password)
            if connector_param != 'password':
                LOGGER.info(f"Snowflake connection parameter {connector_param}: {value}")
        
        return params
    
    def _get_connection(self):
        """Get or create Snowflake connection."""
        if not self._conn:
            self._conn = snowflake.connector.connect(**self._connection_params)
        return self._conn
    
    def execute_query(self, query: str) -> List[Dict[str, Any]]:
        """Execute a Snowflake query using the shared connection."""
        conn = self._get_connection()
        cur = conn.cursor(snowflake.connector.DictCursor)
        try:
            cur.execute(query)
            return cur.fetchall()
        finally:
            cur.close()
    
    def _clean_identifier(self, value: str) -> str:
        """Clean an identifier by removing hyphens and converting to uppercase."""
        return value.replace('-', '').upper()
    
    def _create_stage_name(self) -> str:
        """Create a Snowflake stage name in the format S3_STAGE"""
        return "S3_STAGE"
    
    def _create_schema_name(self) -> str:
        """Create a Snowflake schema name in the format T_ORGID_SOURCE."""
        return f"T_{self._clean_identifier(self.path_components.org_id)}_{self._clean_identifier(self.path_components.source)}"
    
    def create_schema(self) -> None:
        """Create a Snowflake schema if it doesn't exist."""
        query = f"""
        CREATE SCHEMA IF NOT EXISTS {self.schema_name};
        """
        self.execute_query(query)
        LOGGER.info(f"Created or verified schema: {self.schema_name}")
    
    def create_s3_stage(self, s3_bucket: str) -> None:
        """Create a Snowflake stage for S3 integration if it doesn't exist."""
        # First create the schema if it doesn't exist
        self.create_schema()
        
        query = f"""
        CREATE STAGE IF NOT EXISTS {self.schema_name}.{self.stage_name}
        STORAGE_INTEGRATION = s3_int
        URL = 's3://{s3_bucket}/{self.path_components.org_id}/{self.path_components.source}'
        DIRECTORY = (
            ENABLE = TRUE, 
            REFRESH_ON_CREATE = FALSE
        )
        FILE_FORMAT = (TYPE = JSON);
        """
        self.execute_query(query)
        LOGGER.info(f"Created or verified stage: {self.schema_name}.{self.stage_name}")

    def enable_directory_on_stage(self) -> None:
        """Enable directory on the stage."""
        query = f"ALTER STAGE {self.schema_name}.{self.stage_name} SET DIRECTORY = (ENABLE = TRUE);"
        self.execute_query(query)
        LOGGER.info(f"Enabled directory on stage: {self.schema_name}.{self.stage_name}")
    def refresh_directory(self) -> None:
        """
        Refresh a Snowflake directory for the stage.
        For paths with repo_ids (e.g., GitHub), refreshes with that specific subpath/repo.
        For datepaths with dates (e.g., Azure Tickets), refreshes the entire stage.
        """
        stage_ref = f"{self.schema_name}.{self.stage_name}"
        
        # Build the refresh query based on repo_id presence
        if self.path_components.repo_id:
            subpath = f"SUBPATH = '{self.path_components.repo_id}'"
            LOGGER.info(f"Refreshing {stage_ref} with repo {self.path_components.repo_id}")
        else:
            subpath = ""
            LOGGER.info(f"Refreshing entire {stage_ref}")
            
        query = f"ALTER STAGE {stage_ref} REFRESH {subpath};"
        self.execute_query(query)
    
    def close(self):
        """Close the Snowflake connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

def parse_path_template(path_template: str) -> PathComponents:
    """
    Parse a path template to extract org_id, source, and repo_id.
    
    Args:
        path_template: String like "org_id/source/repo_id/..."
        
    Returns:
        PathComponents containing org_id, source, and repo_id (
        If the repo_id is a UUID, it is used as the repo_id, otherwise it is an empty string
    """
    parts = path_template.split('/')
    if len(parts) < 3:
        raise ValueError(f"Path template must have at least 3 components (org_id/source/repo_id), got: {path_template}")
    
    return PathComponents(
        org_id=parts[0],
        source=parts[1],
        repo_id=parts[2] if SnowflakeStage.UUID_PATTERN.match(parts[2]) else ""
    )

