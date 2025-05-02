import os
import pytest
from unittest.mock import patch
import lib.Config.config as config

# 절대 경로 지정
test_config_path = os.path.join(os.path.dirname(__file__), "test_settings.yaml")


@pytest.fixture(autouse=True)
def patch_config_path():
    with patch.object(config.Config, "_config_path", test_config_path):
        yield  # 테스트 실행
        # 자동으로 patch 해제됨


@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    from main import main  # patch 이후에 import 해야 반영됨

    main()
    mock_run.assert_called_once()
