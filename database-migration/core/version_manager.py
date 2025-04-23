import time
import re
from typing import List,Any

class VersionManager:

    def __init__(self) -> None:
        pass

    def generate_file_version(self,file_object: object) -> str:
        """
        Version's strictly SQL files

        Parameters
        ----------
        file_object: object
            A Path Object
        """
        if file_object.suffix != 'sql':
            raise ValueError("File type must be sql")

        return f'V{time.time()}__{file_object.name}.{file_object.suffix}'

    def extract_version(self, file_name: str) -> float:
        """
        Extracts the version number from inputed filename

        Parameters
        ----------
        file_name: str
            A filename that should be extracted through a Path object
        """

        match = re.match(r'V(\d+(?:\.\d+)?)__.*\.sql',file_name)
        if not match:
            raise ValueError("Invalid migration file format")
        return float(match.group(1))


    def order_migrations(self,migration_list: List[object]) -> List[object]:
        """
        Returns migration's in ascending order based on version

        Parameters
        ----------
        migration_list: List[object]
            A list of Path Objects
        """

        return sorted(migration_list, key=lambda x: self.extract_version(x.name))
