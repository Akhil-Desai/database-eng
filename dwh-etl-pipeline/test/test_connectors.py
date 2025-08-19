import pytest
from unittest.mock import MagicMock
from data_connectors import connector_strategy as strategy



def test_strategy(monkeypatch):

    strat = strategy.ConnectorStrategy()

    fake_result = {"data":"test"}

    monkeypatch.setattr(
        "data_connectors.connector_strategy.DataConnectors.api_connector",
        MagicMock(return_value={"data": "lol"})
    )

    strat.set_strategy(strategy.Strategies.API)
    result = strat.execute("example.com", ("offset", 2), ("limit", 2))
    assert result != fake_result
