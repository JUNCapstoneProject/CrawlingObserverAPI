import os
import pytest
from unittest.mock import patch
import lib.Config.config as config

# 테스트 설정 파일 경로
TEST_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "test_settings.yaml")


@pytest.fixture(autouse=True)
def patch_config_path():
    """Config 클래스의 설정 파일 경로를 테스트용으로 패치"""
    with patch.object(config.Config, "_config_path", TEST_CONFIG_PATH):
        yield  # 테스트 실행
        # patch는 컨텍스트 종료 시 자동 해제


@patch("main.run")
def test_main_run_called(mock_run):
    """main 함수 실행 시 run 함수가 호출되는지 테스트"""
    from main import main  # patch 이후에 import 해야 반영됨

    main()
    mock_run.assert_called_once()
