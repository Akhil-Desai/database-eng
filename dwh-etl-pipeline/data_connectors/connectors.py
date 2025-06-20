#We can connect to an API, Database, File Reader
import requests
import logging
import csv
from typing import *

class DataConnectors:

    def __init__(self,data_config):
        self.config = data_config

    def api_connector(
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
            res = requests.get(endpoint, params, timeout=_timeout)
            json_data = res.json()
            if res.status_code in [500,503,403,404,401,400]:
                logging.error(f'Bad status code: {res.status_code}')
                raise ValueError("JSON object not returned")

            return json_data, (offset[1] + limit[1])
        except Exception as e:
            logging.warning(f'Invalid JSON returned from API, trying as text...{e}')
            try:
                #I want to test this and see what it returns, Looks like it structures it as a dict based on docs
                text_data = res.text
                return text_data, (offset[1] + limit[1])
            except Exception as e2:
                logging.error(f'Error fetching as text {e2}')
                return

    def csv_connector(
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
            with open(file_path, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)

                for _ in range(offset):
                    next(reader, None)

                for i,row in enumerate(reader):
                    if i >= limit: break

                    csv_data.append(row)

            return csv_data,(offset + limit)
        except Exception as e:
            logging.error(f'Error reading from csv {e}')
            return [], None


    def db_connector():
        #This in itself might have multiple db configs -- have to adress this
        pass
