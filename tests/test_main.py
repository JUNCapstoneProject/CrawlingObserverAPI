import os
import pytest
from unittest.mock import patch
import lib.Config.config as config

# 절대 경로 지정 (파일이 있는 곳 기준)
test_config_path = os.path.join(os.path.dirname(__file__), "test_settings.yaml")

# 경로 패치 먼저
patcher = patch.object(config.Config, "_config_path", test_config_path)
patcher.start()

from main import main  # 패치 후에 import!


@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    main()
    mock_run.assert_called_once()


patcher.stop()
