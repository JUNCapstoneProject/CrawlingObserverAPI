import os
import pytest
from unittest.mock import patch
import lib.Config.config as config

# 절대 경로로 지정
test_config_path = os.path.abspath("test_settings.yaml")

patcher = patch.object(config.Config, "_config_path", test_config_path)
patcher.start()

from main import main


@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    main()
    mock_run.assert_called_once()


patcher.stop()
