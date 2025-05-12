"""
version_manager.py
------------------
Provides utilities for generating migration file versions, extracting version numbers, and ordering migration files.
"""

import time
import re
from typing import List, Any

class VersionManager:
    """
    Handles versioning logic for migration files, including version extraction and ordering.
    """

    def __init__(self) -> None:
        """
        Initialize the VersionManager.
        """
        pass

    def generate_file_version(self, file_object: object) -> str:
        """
        Generate a versioned filename for a SQL migration file using the current timestamp.

        Args:
            file_object (Path): A Path object representing the SQL file.
        Returns:
            str: Versioned filename in the format 'V<timestamp>__<filename>'.
        Raises:
            ValueError: If the file is not a .sql file.
        """
        if file_object.suffix != '.sql':
            raise ValueError("File type must be sql")

        return f'V{time.time()}__{file_object.name}'

    def extract_version(self, file_name: str) -> float:
        """
        Extract the version number from a migration filename.

        Args:
            file_name (str): The migration filename.
        Returns:
            float: The extracted version number.
        Raises:
            ValueError: If the filename does not match the expected pattern.
        """
        match = re.match(r'V(\d+(?:\.\d+)?)__.*\.sql', file_name)
        if not match:
            raise ValueError("Invalid migration file format")
        return float(match.group(1))

    def order_migrations(self, migration_list: List[object]) -> List[object]:
        """
        Return migrations in ascending order based on version number.

        Args:
            migration_list (List[Path]): List of Path objects for migration files.
        Returns:
            List[Path]: Sorted list of Path objects.
        """
        return sorted(migration_list, key=lambda x: self.extract_version(x.name))
