import unittest
from unittest.mock import MagicMock
from data_connectors import connector_strategy as cs


class TestConnectorMethods(unittest.TestCase):

    def setUp(self):
        self.selector = cs.ConnectorStrategy()

    def test_strategy_selection(self):
        #Mock what the connectors to check if selecting strategy works
        mock_api_connector = MagicMock(return_value={"data": "api"})
        mock_csv_connector = MagicMock(return_value={"data": "csv"})
        mock_db_connector = MagicMock(return_value={"data": "db"})

        self.selector.set_strategy(cs.Strategies.API)
        self.selector.strategy = mock_api_connector
        api_result = self.selector.execute("example/endpoint.com", ("offset",1), ("limit",1))
        self.assertEqual({"data":"api"}, api_result)

        self.selector.set_strategy(cs.Strategies.CSV)
        self.selector.strategy = mock_csv_connector
        csv_result = self.selector.execute("example/filepath",1,1)
        self.assertEqual({"data":"csv"}, csv_result)

        self.selector.set_strategy(cs.Strategies.DB)
        self.selector.strategy = mock_db_connector
        db_result = self.selector.execute(("example_table,1,10"), "dummy_config")
        self.assertEqual({"data":"db"},db_result)

        self.selector.clear_strategy()


    def test_api_connector(self):
        #Make a request to an endpoint that supports pagination
        pass

    def tearDown(self):
        self.selector = None
