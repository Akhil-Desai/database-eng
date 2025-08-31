import unittest
from unittest.mock import AsyncMock
from data_connectors import connector_strategy as cs


class TestConnectorMethods(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        self.selector = cs.ConnectorStrategy()

    async def test_strategy_selection(self):
        #Mock what the connectors to check if selecting strategy works

        mock_api_connector = AsyncMock(return_value={"data": "api"})
        mock_csv_connector = AsyncMock(return_value={"data": "csv"})
        mock_db_connector = AsyncMock(return_value={"data": "db"})

        self.selector.set_strategy(cs.Strategies.API)
        self.selector.strategy = mock_api_connector
        api_result = await self.selector.execute("example/endpoint.com", ("offset",1), ("limit",1))
        self.assertEqual({"data":"api"}, api_result)

        self.selector.set_strategy(cs.Strategies.CSV)
        self.selector.strategy = mock_csv_connector
        csv_result = await self.selector.execute("example/filepath",1,1)
        self.assertEqual({"data":"csv"}, csv_result)

        self.selector.set_strategy(cs.Strategies.DB)
        self.selector.strategy = mock_db_connector
        db_result = await self.selector.execute(("example_table,1,10"), "dummy_config")
        self.assertEqual({"data":"db"},db_result)



    async def test_api_connector(self):


        self.selector.set_strategy(cs.Strategies.API)

        self.assertIsNotNone(self.selector.strategy)

        api_end_point = "https://jsonplaceholder.typicode.com/posts"

        result = await self.selector.execute(api_end_point, ("_start", 0), ("_limit",5))

        self.assertIsInstance(result[0], list)
        self.assertGreater(len(result[0]),0)
        self.assertIn("title", result[0][0])
        self.assertNotEqual(result[2], 500)




    async def test_csv_connector(self):


        self.selector.set_strategy(cs.Strategies.CSV)
        file_path = 'test_files/test_data.csv'

        result = await self.selector.execute(file_path,0,3)

        self.assertIsInstance(result[0], list)
        self.assertGreater(len(result[0]), 0)
        self.assertEqual(result[1],3)

    def tearDown(self):
        self.selector = None
