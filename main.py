import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from lib.Crawling import run  # 크롤링 실행 함수
# from Utill.Socket.Server import SocketServer  # 소켓 서버

def main():
    # 크롤러 실행
    run()
    
    # # 소켓 서버 실행 (외부 요청 대기)
    # server = SocketServer()
    # server.run()

if __name__ == "__main__":
    main()
