"""
migration_runner.py
-------------------
Coordinates the process of determining which migrations need to be applied and executes them against the database.
"""

from typing import Dict, List
from .migration_scanner import MigrationScanner
from .migration_registry import MigrationRegistry
import time

class MigrationRunner:
    """
    Handles the discovery, filtering, and application of migration files to the database.
    """
    def __init__(self, migration_dir, db_config) -> None:
        """
        Initialize the MigrationRunner.

        Args:
            migration_dir (str): Directory containing migration files.
            db_config (dict): Database configuration dictionary.
        """
        self.migration_dir = migration_dir
        self.migration_registry = MigrationRegistry(db_config)

    def get_applied_migrations(self) -> dict:
        """
        Retrieve a dictionary of applied migrations and their checksums from the database.

        Returns:
            dict: Mapping of migration version to checksum.
        """
        conn = self.migration_registry._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("""SELECT version,checksum
                           FROM schema_migrations
                           ORDER BY version
                           """)
            return {row[0]: row[1] for row in cursor.fetchall()}
        finally:
            cursor.close()
            conn.close()

    def get_migrations_to_apply(self) -> List[Dict[str,any]]:
        """
        Determine which migrations need to be applied by comparing migration files to the registry.
        Also checks for checksum mismatches in already-applied migrations.

        Returns:
            List[dict]: List of migration metadata dicts to apply.
        Raises:
            ValueError: If a migration file has been changed after being applied, or if no migration files are found.
        """
        scanner = MigrationScanner(self.migration_dir)
        migration_files = scanner.discover_migrations()
        if not migration_files:
            raise ValueError("No migration files found")
        applied_migrations = self.get_applied_migrations()
        apply_migrations = []
        for migration in migration_files:
            version,checksum = migration["version"], migration['checksum']
            if version in applied_migrations:
                if checksum != applied_migrations[version]:
                    raise ValueError(f'Applied Migration {version} has been changed')
            else:
                apply_migrations.append(migration)
        return apply_migrations

    def run_migrations(self) -> None:
        """
        Apply all pending migrations to the database in order.
        Records each migration in the registry after successful application.
        Raises:
            ValueError: If there are no migrations to apply.
        """
        to_apply = self.get_migrations_to_apply()
        if not to_apply:
            raise ValueError("No migrations to apply")
        conn = self.migration_registry._get_connection()
        conn.autocommit = False
        try:
            cursor = conn.cursor()
            for migration in to_apply:
                print(f"Applying migration {migration['version']}...")
                try:
                    with open(migration["path"], 'r') as file:
                        sql_statements = file.read()
                    start = time.time()
                    cursor.execute(sql_statements)
                    end = time.time()
                    conn.commit()
                    self.migration_registry.record_migration(migration,(end - start),"Applied")
                except Exception as e:
                    conn.rollback()
                    print(f'Error {e} when applying migration {migration['version']}')
                    raise
        finally:
            cursor.close()
            conn.close()
