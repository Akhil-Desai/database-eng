"""
conftest.py
-----------
Pytest fixtures for database configuration and cleaning the test database before each test.
"""

import pytest
import psycopg2
import os
from dotenv import load_dotenv
from core.migration_registry import MigrationRegistry

load_dotenv()

DB_PASS = os.getenv("DB_PASSWORD")


@pytest.fixture
def db_config():
    """
    Fixture providing database configuration for tests.
    Returns:
        dict: Database configuration dictionary.
    """
    return {
        'type': 'postgresql',
        'dbname': 'postgres',
        'user': "akhil",
        'password': DB_PASS,
        'port':"5432",
    }

@pytest.fixture(scope="function")
def clean_db(db_config):
    """
    Fixture to ensure the test database is clean before each test.
    Drops user, product, and migration tables if they exist, then re-initializes the registry after the test.
    """
    registry = MigrationRegistry(db_config)
    conn = None
    cursor = None
    try:
        conn = registry._get_connection()
        cursor = conn.cursor()
        # Drop tables created by migrations (add others if needed)
        # Use IF EXISTS for safety, order matters for foreign keys
        cursor.execute("DROP TABLE IF EXISTS users;")
        cursor.execute("DROP TABLE IF EXISTS products;")
        # Drop the migrations table itself
        cursor.execute("DROP TABLE IF EXISTS schema_migrations;")
        conn.commit()
        print("\nCleaned database tables for test.")
    except Exception as e:
        # Ignore errors if tables don't exist (e.g., first run)
        print(f"\nDB cleanup notice: {e}")
        if conn:
            conn.rollback() # Rollback any partial changes from cleanup attempt
    finally:
        if cursor: cursor.close()
        if conn: conn.close()

    yield

    registry.initialize()
