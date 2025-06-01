import os
import sys
import threading
import logging
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler
from rich.logging import RichHandler
from rich.console import Console

from lib.Config.config import Config


class CustomLogger(logging.Logger):

    def __init__(self, name: str):
        super().__init__(name)

        self.error_count = 0
        self.warning_count = 0
        self.backup_count = 24
        self.is_test = Config.get("is_test.toggle", False)
        self.rotation_interval = Config.get("log_rotation", 1)
        self.include_traceback = Config.get("log_include_traceback", True)

        self._setup_handlers()
        self._inject_count_filter()

    def _setup_handlers(self):
        for h in self.handlers[:]:  # 안전한 핸들러 제거
            self.removeHandler(h)

        formatter = logging.Formatter(
            ">>\n[%(filename)s : %(funcName)s() : %(lineno)s]\n[%(asctime)s] [%(levelname)-7s] %(name)-24s - %(message)s\n<<",
            "%m-%d %H:%M",
        )
        console_formatter = logging.Formatter(
            "%(name)-24s >> %(message)s",
            "%m-%d %H:%M:%S",
        )

        os.makedirs("logs/errors", exist_ok=True)
        os.makedirs(os.path.join("logs", self.name), exist_ok=True)

        # 콘솔 출력 (rich)
        console_handler = RichHandler(markup=True, show_path=False)
        if self.is_test:
            console_handler.setLevel(logging.DEBUG)
        else:
            console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(console_formatter)
        self.addHandler(console_handler)

        # 공통 로그 파일 핸들러
        common_path = os.path.join("logs", "log_common.log")
        common_handler = TimedRotatingFileHandler(
            common_path,
            when="H",
            interval=self.rotation_interval,
            backupCount=self.backup_count,
            encoding="utf-8",
        )
        common_handler.setLevel(logging.INFO)
        common_handler.setFormatter(formatter)
        self.addHandler(common_handler)

        # 개별 로그 파일 핸들러
        indiv_path = os.path.join("logs", self.name, "log_indiv.log")
        indiv_handler = TimedRotatingFileHandler(
            indiv_path,
            when="H",
            interval=self.rotation_interval,
            backupCount=self.backup_count,
            encoding="utf-8",
        )
        indiv_handler.setLevel(logging.DEBUG)
        indiv_handler.setFormatter(formatter)
        self.addHandler(indiv_handler)

        # 에러 로그 파일 핸들러
        error_path = os.path.join("logs", "errors", "log_error.log")
        error_handler = TimedRotatingFileHandler(
            error_path,
            when="H",
            interval=self.rotation_interval,
            backupCount=self.backup_count,
            encoding="utf-8",
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        self.addHandler(error_handler)

    def _inject_count_filter(self):
        def count_filter(record):
            if record.levelno == logging.ERROR:
                self.error_count += 1
            elif record.levelno == logging.WARNING:
                self.warning_count += 1
            return True

        self.addFilter(count_filter)

    def log_summary(self):
        timestamp = datetime.now().strftime("%m-%d %H:%M:%S")
        msg = f"로그 저장 완료-`logs` WARNING: {self.warning_count}개, ERROR: {self.error_count}개"

        if self.error_count > 0:
            color = "bold red"
        elif self.warning_count > 0:
            color = "bold yellow"
        else:
            color = "grey62"

        Console().print(f"[{timestamp}] {self.name:<24} >> {msg}", style=color)

        self.error_count = 0
        self.warning_count = 0

    def register_global_hooks(self):
        def handle_exception(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            self.error(
                "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
            )

        def handle_thread_exception(args):
            self.error(
                f"Uncaught thread exception: {args.thread.name}",
                exc_info=(args.exc_type, args.exc_value, args.exc_traceback),
            )

        sys.excepthook = handle_exception
        if hasattr(threading, "excepthook"):
            threading.excepthook = handle_thread_exception


def get_logger(name: str) -> CustomLogger:
    logger = CustomLogger(name)
    return logger
