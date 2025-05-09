# Import local modules
from lib.Crawling import run  # 크롤링 실행 함수
from lib.Config.config import Config  # 설정 로드
from lib.Exceptions.traceback import log_traceback  # 트레이스백 처리 함수

# from Utill.Socket.Server import SocketServer  # 소켓 서버 (필요 시 활성화)


def main():
    try:
        # Config 초기화
        Config.init()

        # 크롤러 실행
        run()

        # 소켓 서버 실행 (외부 요청 대기) - 필요 시 활성화
        # server = SocketServer()
        # server.run()

    except Exception as e:
        # 트레이스백 처리 함수 호출
        log_traceback(e)


if __name__ == "__main__":
    main()
