from lib.Crawling import run  # 크롤링 실행 함수

# from Utill.Socket.Server import SocketServer  # 소켓 서버

from lib.Config.config import Config  # 설정 로드


def main():
    # 크롤러 실행
    run()

    # # 소켓 서버 실행 (외부 요청 대기)
    # server = SocketServer()
    # server.run()

    # config load
    Config.init()


if __name__ == "__main__":
    main()
