import yaml
import os
from pathlib import Path
from typing import Any


class Config:
    _config = {}
    _config_path = Path(__file__).resolve().parents[2] / "settings.yaml"
    _last_mtime = None

    @classmethod
    def init(cls):
        cls._load(force=True)

    @classmethod
    def _load(cls, force=False):
        mtime = os.path.getmtime(cls._config_path)
        if not force and cls._last_mtime == mtime:
            return  # 변경 없음 → skip
        cls._last_mtime = mtime
        with open(cls._config_path, "r", encoding="utf-8") as f:
            cls._config = yaml.safe_load(f)

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        cls._load()  # 변경 감지 후 필요시 reload

        keys = key.split(".")
        value = cls._config
        for k in keys:
            if not isinstance(value, dict) or k not in value:
                return default
            value = value[k]
        return value
