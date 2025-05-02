import pytest
from unittest.mock import patch
import lib.Config.config as config

# 설정 값을 직접 주입
config.Config._config = {
    "color_log": False,
    "API_KEYS": {"Fred": "test-api-key"},
    "symbol_size": 10,
    "articles": {"size": 1, "retry": 1},
    "save_method": {"save_to_file": True, "save_to_DB": True},
    "database": {"url": "sqlite:///:memory:"},
}


@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    from main import main

    main()
    mock_run.assert_called_once()
