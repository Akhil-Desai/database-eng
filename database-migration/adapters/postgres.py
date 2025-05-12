"""
postgres.py
-----------
Implements the Adapter interface for PostgreSQL, providing methods to initialize the migration registry, record migrations, and retrieve applied migrations.
"""

from .base import Adapter
from typing import List,Dict

class posgrestSQL(Adapter):
    """
    PostgreSQL adapter for migration registry operations.
    """
    def __init__(self):
        """
        Initialize the posgrestSQL adapter with SQL templates for migration operations.
        """
        self.TEMPLATES = {
            'initialize_table': """
                        CREATE TABLE IF NOT EXISTS
                        schema_migrations (
                            id SERIAL PRIMARY KEY,
                            version VARCHAR(50) NOT NULL,
                            description VARCHAR(200),
                            filename VARCHAR(255) NOT NULL,
                            checksum VARCHAR(64) NOT NULL,
                            executed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                            execution_time INTEGER,
                            status VARCHAR(20) NOT NULL,
                            applied_by VARCHAR(100),
                            UNIQUE(version)
                            );
                        """,

            'record_migration': """
                                INSERT INTO schema_migrations
                                (version,description, filename,checksum,execution_time,status,applied_by)
                                VALUES(%s, %s, %s, %s, %s, %s, %s)
                                """,

            'get_applied_migrations': """SELECT version,checksum
                               FROM schema_migrations
                               ORDER BY version
                               """,
        }


    def initialize_registry(self, cursor):
        """
        Create the schema_migrations table in the PostgreSQL database if it does not exist.
        Args:
            cursor: PostgreSQL database cursor.
        """
        cursor.execute(self.TEMPLATES['initialize_registry'])


    def record_migration(self, cursor, migration: Dict[str, any], execution_time: str, status: str, applied_by='system') -> None:
        """
        Record a migration as applied in the schema_migrations table.
        Args:
            cursor: PostgreSQL database cursor.
            migration (dict): Migration metadata.
            execution_time (str): Time taken to apply the migration.
            status (str): Status of the migration.
            applied_by (str): User or system applying the migration.
        """
        cursor.execute(self.TEMPLATES["record_migration"],
                       (migration['version'],
                        migration['description'],
                        migration['filename'],
                        migration['checksum'],
                        execution_time,
                        status,
                        applied_by,))

    def get_applied_migrations(self, cursor):
        """
        Retrieve applied migrations from the schema_migrations table.
        Args:
            cursor: PostgreSQL database cursor.
        """
        cursor.execute(self.TEMPLATES["get_applied_migrations"])
