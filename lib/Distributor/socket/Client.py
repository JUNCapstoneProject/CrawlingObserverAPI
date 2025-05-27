import socket
import copy
import json
import zlib

# Bridge
from lib.Distributor.socket.Interface import SocketInterface
from lib.Distributor.socket.messages.request import requests_message
from lib.Logger.logger import Logger


class SocketClient(SocketInterface):
    def __init__(self):
        self.requests_message = copy.deepcopy(requests_message)
        self.logger = Logger(self.__class__.__name__)

    # FIXME : 구현하기
    @staticmethod
    def resolve_addr(message):
        return "StockAnalysisAPI_service", 4006

    def request_tcp(self, item):
        """
        item을 입력으로 받아 request_message를 만들어 요청하고,
        data만 반환
        """
        self.requests_message["body"]["item"] = item

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.settimeout(10)  # 🔸 블로킹 방지용 타임아웃 추가

        addr, port = self.resolve_addr(self.requests_message)
        self.logger.log("DEBUG", f"[SOCKET] 연결 시도 → {addr}:{port}")
        client_socket.connect((addr, port))
        self.logger.log("DEBUG", "[SOCKET] 연결 성공")

        try:
            datagram = zlib.compress(json.dumps(self.requests_message).encode())
            self.logger.log(
                "DEBUG", f"[SOCKET] 요청 데이터 크기: {len(datagram)} bytes"
            )
            self.logger.log("DEBUG", "[SOCKET] 요청 전송 중...")
            client_socket.sendall(datagram)
            self.logger.log("DEBUG", "[SOCKET] 요청 전송 완료, 응답 대기 중...")

            data = client_socket.recv(self.SOCKET_BYTE)
            self.logger.log("DEBUG", f"[SOCKET] 응답 수신 완료 ({len(data)} bytes)")

            message = json.loads(data.decode())
            self.logger.log(
                "DEBUG", f"[SOCKET] 응답 메시지 파싱 성공: {message.get('status_code')}"
            )
            return message

        except socket.timeout:
            self.logger.log("ERROR", "[SOCKET] 타임아웃: 서버로부터 응답이 없습니다.")
            raise
        except Exception as e:
            self.logger.log("ERROR", f"[SOCKET] 예외 발생: {e}")
            raise
        finally:
            client_socket.close()
            self.logger.log("DEBUG", "[SOCKET] 소켓 연결 종료")
