"""
migration_rollback.py
---------------------
Provides functionality to rollback database migrations to a specified target version. This includes reversing the effects of applied migrations and cleaning up the migration registry to reflect the rollback state.
"""
from pathlib import Path
import re
from .migration_registry import MigrationRegistry
from .version_manager import VersionManager
from .migration_scanner import MigrationScanner

class MigrationRollback():
    """
    Handles the rollback process for database migrations.
    This class is responsible for reversing migrations that were applied after a specified target version and updating the migration registry accordingly.
    """
    def __init__(self, migration_dir: str, migration_registry: MigrationRegistry, rollback_target: object) -> None:
        """
        Initialize the MigrationRollback instance.

        Args:
            migration_dir (str): Directory containing the down (rollback) migration files.
            migration_registry (MigrationRegistry): Instance of MigrationRegistry for database operations.
            rollback_target (object): Target migration object to rollback to (should be a file object).
        """
        self.version_manager = VersionManager()
        self.migration_dir = migration_dir
        self.migration_registry = migration_registry
        self.rollback_target = rollback_target
        self.migration_scanner = MigrationScanner(migration_dir)

    def clean_registry(self) -> None:
        """
        Remove migration records from the registry for all migrations with a version greater than the rollback target.
        This ensures the registry accurately reflects the current state after rollback.
        """
        file_version = self.version_manager.extract_version(self.rollback_target.name)
        conn = None
        cursor = None
        try:
            conn = self.migration_registry._get_connection()
            cursor = conn.cursor()

            cursor.execute("DELETE FROM schema_migrations WHERE version > ?", (file_version))

            conn.commit()
        except Exception as e:
            print("Error", e)
            if conn: conn.rollback()

        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()

    def rollback(self):
        """
        Rollback all migrations with a version greater than the rollback target version.
        This executes the corresponding down (rollback) SQL scripts in reverse order and cleans the migration registry.
        """
        file_version = self.version_manager.extract_version(self.rollback_target.name)
        migration_down_files = self.migration_scanner.discover_migrations()

        migration_down_files = [m for m in migration_down_files if m["version"] > file_version]
        conn = None
        cursor = None
        try:
            conn = self.migration_registry._get_connection()
            cursor = conn.cursor()
            for migration in reversed(migration_down_files):
                path = migration["path"]

                try:
                    with open(path, 'r') as file:
                        sql_statements = file.read()
                        cursor.execute(sql_statements)
                        conn.commit()
                except Exception as e:
                    print("Error", e)
                    conn.rollback()
                    raise

            self.clean_registry()

        except Exception as e:
            print("Error", e)
            conn.rollback()

        finally:
            cursor.close()
            conn.close()
