import pytest
from unittest.mock import patch
import lib.Config.config as config

# 설정 파일 경로를 테스트 설정으로 먼저 교체
test_config_path = "test_settings.yaml"

# 이걸 먼저 실행해줘야 합니다
patcher = patch.object(config.Config, "_config_path", test_config_path)
patcher.start()

# 이제 main을 import (이 시점엔 이미 패치 적용됨)
from main import main


@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    main()
    mock_run.assert_called_once()


# 테스트 끝난 후 패치 해제
patcher.stop()
