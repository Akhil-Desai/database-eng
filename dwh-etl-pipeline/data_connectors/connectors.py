#We can connect to an API, Database, File Reader
import httpx
import logging
import csv
import aiofiles
import asyncpg
from typing import *
from utils.db_utils import get_connection

class DataConnectors:

    def __init__(self):
        pass

    async def api_connector(
        self,
        endpoint: str,
        offset: tuple[str,int],
        limit: tuple[str,int],
        _max_limit: int = 100,
        _timeout: int = 10,
    ) -> Tuple[Optional[Any], int]:

        """
        Fetches data from an API endpoint with pagination support.

        Args:
            endpoint (str): The API URL.
            offset (Tuple[str, int]): (parameter name, value) for offset.
            limit (Tuple[str, int]): (parameter name, value) for limit.
            max_limit (int): Maximum number of records to fetch per request.
            timeout (int): Timeout for the request in seconds.

        Returns:
            Tuple[Optional[Any], int]: (data, next_offset)
        """

        params = {offset[0]: offset[1], limit[0]: min(limit[1], _max_limit)}

        try:
            async with httpx.AsyncClient() as client:
                res = await client.get(endpoint, params, timeout=_timeout)
            res.raise_for_status()
            try:
                json_data = res.json()
                return json_data, (offset[1] + limit[1])
            except ValueError as ve:
                logging.warning(f'Invalid JSON returned from API, returning as text...{ve}')
                text_data = res.text
                return text_data, (offset[1] + limit[1])

        except Exception as e:
            logging.exception(f'Error fetching from API: {e}')
            return

    async def csv_connector(
        self,
        file_path: str,
        offset: int,
        limit: int,
        _max_limit: int = 100
    ) -> Tuple[List[Dict], Optional[int]]:

        """
        Fetches data from a CSV with pagination support

        Args:
            file_path(str): The file path
            offset(int): offset
            limit(int): limit of rows to read
            max_limit(int): internal argument for nax limit

        Returns:
            List[Dict], Optional[Any]: A List of dicts which represents the row data, and a pointer
        """
        csv_data = []
        limit = min(_max_limit,limit)
        try:
            async with aiofiles.open(file_path, 'r', newline='') as csvfile:
                content = await csvfile.read()
                reader = csv.DictReader(content.splitlines())
                for _ in range(offset):
                    next(reader, None)
                for i,row in enumerate(reader):
                    if i >= limit: break
                    csv_data.append(row)

            return csv_data,(offset + limit)
        except Exception as e:
            logging.exception(f'Error reading from csv {e}')
            return [], None


    async def db_connector(
        self,
        selected_table: Tuple[str,int,int],
        db_config
    ):
        """
        Fetches data from selected tables in a database -- supports mysql and postgres

        Args:
            selected_table ( Tuple[str,int,int] ):  a table and each of its limits and offsets respectively
            _max_workers(int): Internal argument to cap max threads spun up to read multiple tables

        Returns:
            List of fetched data and its offset
        """
        table_name,limit,offset = selected_table

        try:
            conn = asyncpg.connect(**db_config)
            rows = await conn.fetch(f'SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}')
            await conn.close()

            return rows, limit + offset

        except Exception as e:
            logging.exception(f'error retrieving from {table_name}')
            return [],None
