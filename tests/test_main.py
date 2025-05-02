import pytest
from unittest.mock import patch
import lib.Config.config as config


@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    # 테스트용 설정 파일 경로로 임시 변경
    test_config_path = "test_settings.yaml"
    with patch.object(config.Config, "_config_path", test_config_path):
        from main import main

        main()
        mock_run.assert_called_once()
