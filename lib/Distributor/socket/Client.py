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
        self.CONNECT_TIMEOUT = 5
        self.RECV_TIMEOUT = 10
        self.SOCKET_BYTE = 8192

    @staticmethod
    def resolve_addr(message):
        return "StockAnalysisAPI_service", 4006

    def request_tcp(self, item):
        self.requests_message["body"]["item"] = item

        # 압축 테스트
        try:
            json_payload = json.dumps(self.requests_message)
            compressed = zlib.compress(json_payload.encode("utf-8"))
            self.logger.log("DEBUG", f"[TEST] 압축 크기: {len(compressed)} bytes")

            decompressed = zlib.decompress(compressed).decode("utf-8")
            parsed = json.loads(decompressed)
            self.logger.log(
                "DEBUG", f"[TEST] 압축 해제 성공: keys = {list(parsed.keys())}"
            )
        except Exception as e:
            self.logger.log("ERROR", f"[TEST] 압축 테스트 실패: {e}")
            raise

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr, port = self.resolve_addr(self.requests_message)

        try:
            # 연결 시 타임아웃
            client_socket.settimeout(self.CONNECT_TIMEOUT)
            self.logger.log(
                "DEBUG",
                f"[CONNECT] Connecting to {addr}:{port} (timeout={self.CONNECT_TIMEOUT}s)",
            )
            try:
                client_socket.connect((addr, port))
            except socket.timeout:
                self.logger.log("ERROR", "[CONNECT] 연결 타임아웃 발생")
                raise
            except Exception as e:
                self.logger.log("ERROR", f"[CONNECT] 연결 예외 발생: {e}")
                raise

            # 응답 대기 타임아웃 설정
            client_socket.settimeout(self.RECV_TIMEOUT)
            self.logger.log(
                "DEBUG", f"[SEND] 데이터 전송 (recv_timeout={self.RECV_TIMEOUT}s)"
            )
            client_socket.sendall(compressed)

            try:
                data = client_socket.recv(self.SOCKET_BYTE)
                message = json.loads(data.decode())
                self.logger.log("DEBUG", "[RECV] 응답 수신 완료")
                return message
            except socket.timeout:
                self.logger.log("ERROR", "[RECV] 응답 수신 타임아웃 발생")
                raise
            except Exception as e:
                self.logger.log("ERROR", f"[RECV] 응답 수신 예외 발생: {e}")
                raise

        finally:
            client_socket.close()
            self.logger.log("DEBUG", "[SOCKET] 소켓 종료")
