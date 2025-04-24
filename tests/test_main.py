# test_main.py

import pytest
from unittest.mock import patch


# main.py에서 run을 호출하는 main 함수를 테스트
# 경로는 'main.py'에서 import한 위치 기준임
@patch("lib.Crawling.run")
def test_main_run_called(mock_run):
    # 테스트 대상 함수 import
    from main import main

    main()

    # run() 함수가 1번 호출됐는지 확인
    mock_run.assert_called_once()
