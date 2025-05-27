import yaml
import os
from pathlib import Path
from typing import Any


class Config:
    """설정 파일 관리 클래스"""

    _config = {}
    _config_path = Path(__file__).resolve().parents[2] / "settings.yaml"
    _last_mtime = None

    @classmethod
    def init(cls):
        """설정을 초기화합니다."""
        cls._load(force=True)

    @classmethod
    def _load(cls, force: bool = False):
        """설정 파일을 로드합니다.

        Args:
            force (bool): 강제로 로드할지 여부.
        """
        mtime = os.path.getmtime(cls._config_path)
        if not force and cls._last_mtime == mtime:
            return  # 설정 파일에 변경 없음 → 로드 생략

        cls._last_mtime = mtime
        with open(cls._config_path, "r", encoding="utf-8") as f:
            cls._config = yaml.safe_load(f)

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        """설정 값을 가져옵니다.

        Args:
            key (str): 설정 키 (예: 'database.host').
            default (Any): 키가 없을 경우 반환할 기본값.

        Returns:
            Any: 설정 값 또는 기본값.
        """
        cls._load()  # 변경 감지 후 필요 시 재로드

        if key == "symbol_size.total":
            return 200

        keys = key.split(".")
        value = cls._config
        for k in keys:
            if not isinstance(value, dict) or k not in value:
                return default
            value = value[k]
        return value
