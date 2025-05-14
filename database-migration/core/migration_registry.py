"""
migration_registry.py
---------------------
Handles the migration registry, including initialization and recording of applied migrations for different database backends (PostgreSQL, MySQL).
"""
import sqlite3
import psycopg2
import mysql.connector
from typing import Dict
from ..adapters.mysql import mySQL
from ..adapters.postgres import posgrestSQL

class MigrationRegistry:
    """
    Manages the schema_migrations registry table and records migration application events.
    Supports PostgreSQL and MySQL backends.
    """
    def __init__(self,db_config) -> None:
        """
        Initialize the MigrationRegistry with the given database configuration.

        Args:
            db_config (dict): Database configuration dictionary.
        """
        self.db_config = db_config
        self.db_type = db_config.get('type', 'postgresql')
        self.mySQL_adapter = mySQL()
        self.postgrest_adpater = posgrestSQL()

    def initialize(self) -> None:
        """
        Initialize the schema_migrations table in the target database.
        Calls the appropriate adapter for the configured backend.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if self.db_type == 'postgresql':
            try:
                self.postgrest_adpater.initialize_registry(cursor)
            except psycopg2.error as e:
                print("Error", e)
                raise
        if self.db_type == 'mysql':
            try:
                self.mySQL.intialize_registry(cursor)
            except Exception as e:
                print("Error", e)
                raise

        conn.commit()
        cursor.close()
        conn.close()

    def record_migration(self, migration: Dict[str,any], execution_time: str, status: str, applied_by='system') -> None:
        """
        Record a migration as applied in the schema_migrations table.

        Args:
            migration (dict): Migration metadata.
            execution_time (str): Time taken to apply the migration.
            status (str): Status of the migration (e.g., 'Applied').
            applied_by (str): User or system applying the migration.
        """
        conn = self._get_connection()
        cursor = conn.cursor()

        if self.db_type == 'postgresql':
            try:
                self.postgrest_adpater.record_migration(cursor, migration, execution_time, status, applied_by)
            except Exception as e:
                print("Error", e)
                raise
        if self.db_type == 'mysql':
            try:
                self.mySQL_adapter.record_migration(cursor,migration, execution_time, status, applied_by )
            except Exception as e:
                print("Error", e)
                raise

        conn.commit()
        cursor.close()
        conn.close()

    def _get_connection(self):
        """
        Create and return a new database connection for the configured backend.
        """
        if self.db_type == 'postgresql':
            db_config_no_type = {k:v for k,v in self.db_config.items() if k != "type"}
            return psycopg2.connect(**db_config_no_type)
        elif self.db_type == 'mysql':
            db_config_no_type = {k:v for k,v in self.db_config.items() if k != "type"}
            return mysql.connector.connect(**self.db_config)
