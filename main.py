from threading import Thread


# Import local modules
from lib.Crawling import run as run_crawling  # 크롤링 실행 함수
from lib.Config.config import Config  # 설정 로드
from lib.Distributor.notifier import run as run_notifier  # notifier
from lib.Logger.logger import get_logger


def run():
    Config.init()
    threads = []

    logger = get_logger("Main")
    logger.register_global_hooks()

    if Config.get("run_condition.crawler", True):
        threads.append(Thread(target=run_crawling, name="CrawlerThread"))

    if Config.get("run_condition.notifier", True):
        threads.append(Thread(target=run_notifier, name="NotifierThread"))

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def main():
    run()


if __name__ == "__main__":
    main()
