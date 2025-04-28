import os
import re
import hashlib
from pathlib import Path
from typing import List, Dict, Any
from .version_manager import VersionManager


class MigrationScanner:

    def __init__(self, migration_dir: str) -> None:
        self.migration_dir = migration_dir
        self.version_manager = VersionManager()

    def discover_migrations(self) -> List[Dict[str,Any]]:

        migrations = []

        migration_files = Path(self.migration_dir).glob('V*.*__*.sql')

        sorted_files = self.version_manager.order_migrations(migration_files)

        for file_path in sorted_files:
            #Extract Version and description

            match = re.match(r'V(\d+(?:\.\d+)?)__(.+)\.sql', file_path.name)
            if not match:
                continue

            version,description = match.groups()

            with open(file_path, 'rb') as f:
                content = f.read()
                checksum = hashlib.md5(content).hexdigest()

            migrations.append({
                'version': f"V{version}",
                'description': description.replace('_', ''),
                "filename": file_path.name,
                "path": str(file_path),
                'checksum': checksum,
            })

        #Migrations are returned in sorted order DO NOT RESORT!
        return migrations
