from pathlib import Path
import re
from core.migration_runner import MigrationRunner
from core.version_manager import VersionManager
from core.migration_registry import MigrationRegistry


def test_create_migration_table(db_config,clean_db):

    migration_registry = MigrationRegistry(db_config)

    assert migration_registry.initialize() == "Applied"

def test_version_generation():
    version_manager = VersionManager()

    new_file = Path("file1.sql")

    new_file_with_version = version_manager.generate_file_version(new_file)

    match = re.match(r'V(\d+(?:\.\d+)?)__.*\.sql',new_file_with_version)

    assert match is not None

def test_extract_version():
    version_manager = VersionManager()

    new_file = Path("file1.sql")

    new_file_with_version = version_manager.generate_file_version(new_file)

    extracted_version = version_manager.extract_version(new_file_with_version)

    assert type(extracted_version) == float


def test_find_migrations(db_config,clean_db):
    migration_runner = MigrationRunner(migration_dir="migrations",db_config=db_config)

    migration_files = migration_runner.get_migrations_to_apply()

    for migration in migration_files:
        print(f"Migration to apply: {migration}")

    assert migration_files is not None

def test_apply_migrations(db_config, clean_db):
    migration_runner = MigrationRunner(migration_dir="migrations",db_config=db_config)

    assert migration_runner.run_migrations() == "Migrations Applied"
