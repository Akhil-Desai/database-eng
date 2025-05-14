"""
test_migrations.py
------------------
Unit tests for migration registry, version management, and migration runner functionality.
"""

from pathlib import Path
import re
from core.migration_runner import MigrationRunner
from core.version_manager import VersionManager
from core.migration_registry import MigrationRegistry


def test_create_migration_table(db_config, clean_db):
    """
    Test that the migration registry table is created successfully.
    """
    migration_registry = MigrationRegistry(db_config)

    assert migration_registry.initialize() == "Applied"


def test_version_generation(clean_db):
    """
    Test that a versioned filename is generated for a SQL file.
    """
    version_manager = VersionManager()

    new_file = Path("file1.sql")

    new_file_with_version = version_manager.generate_file_version(new_file)

    match = re.match(r'V(\d+(?:\.\d+)?)__.*\.sql', new_file_with_version)

    assert match is not None


def test_extract_version(clean_db):
    """
    Test that the version number can be extracted from a versioned filename.
    """
    version_manager = VersionManager()

    new_file = Path("file1.sql")

    new_file_with_version = version_manager.generate_file_version(new_file)

    extracted_version = version_manager.extract_version(new_file_with_version)

    assert type(extracted_version) == float


def test_find_migrations(db_config, clean_db):
    """
    Test that the migration runner can find migrations to apply.
    """

    migration_registry = MigrationRegistry(db_config)

    migration_runner = MigrationRunner(migration_registry=migration_registry, migration_dir="migrations")

    migration_files = migration_runner.get_migrations_to_apply()

    for migration in migration_files:
        print(f"Migration to apply: {migration}")

    assert migration_files is not None


def test_apply_migrations(db_config, clean_db):
    """
    Test that all pending migrations are applied successfully.
    """
    migration_registry = MigrationRegistry(db_config)

    migration_runner = MigrationRunner(migration_dir="migrations", migration_registry = migration_registry)

    assert migration_runner.run_migrations() == "Migrations Applied"
