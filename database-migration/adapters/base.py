"""
base.py
-------
Defines the Adapter abstract base class for database-specific migration operations.
"""

from abc import ABC, abstractmethod
from typing import Dict, List

class Adapter(ABC):
    """
    Abstract base class for database adapters. Defines the required interface for migration registry operations.
    """
    @abstractmethod
    def initialize_registry(self, cursor):
        """
        Initialize the schema_migrations table in the database.

        Args:
            cursor: Database cursor object.
        """
        pass

    @abstractmethod
    def record_migration(self, cursor, migration: Dict[str, any], execution_time: str, status: str, applied_by='system') -> None:
        """
        Record a migration as applied in the schema_migrations table.

        Args:
            cursor: Database cursor object.
            migration (dict): Migration metadata.
            execution_time (str): Time taken to apply the migration.
            status (str): Status of the migration.
            applied_by (str): User or system applying the migration.
        """
        pass

    @abstractmethod
    def get_applied_migrations(self, cursor):
        """
        Retrieve applied migrations from the schema_migrations table.

        Args:
            cursor: Database cursor object.
        """
        pass
