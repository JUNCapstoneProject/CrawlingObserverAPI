from threading import Thread


# Import local modules
from lib.Crawling import run as run_crawling  # 크롤링 실행 함수
from lib.Config.config import Config  # 설정 로드
from lib.Exceptions.traceback import log_traceback  # 트레이스백 처리 함수
from lib.Distributor.notifier import run as run_notifier  # notifier


def run():
    Config.init()

    threads = []

    if Config.get("run_condition.crawler", True):
        threads.append(Thread(target=run_crawling, name="CrawlerThread"))

    if Config.get("run_condition.notifier", True):
        threads.append(Thread(target=run_notifier, name="NotifierThread"))

    for t in threads:
        t.start()

    for t in threads:
        t.join()


def main():
    try:
        run()
    except Exception as e:
        log_traceback(e)


if __name__ == "__main__":
    main()
