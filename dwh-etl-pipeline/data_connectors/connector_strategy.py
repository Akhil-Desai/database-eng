from connectors import DataConnectors
from enum import Enum

class Strategies(Enum):
    API = "API"
    CSV = "CSV"
    DB = "DB"

class ConnectorStrategy(DataConnectors):

    def __init__(self):
        self.strategy = None

    def set_strategy(self, strat: Strategies):
        if strat == Strategies.API:
            self.strategy = self.api_connector
        elif strat == Strategies.CSV:
            self.strategy = self.csv_connector
        elif strat == Strategies.DB:
            self.strategy = self.db_connector
        else:
            raise ValueError("Unknown strategy")

    def execute(self, *args, **kwargs):
        if not self.strategy:
            raise Exception("Strategy not set")

        return self.strategy(*args, **kwargs)
