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

    @staticmethod
    def resolve_addr(message):
        return "StockAnalysisAPI_service", 4006

    def request_tcp(self, item):
        """
        item을 입력으로 받아 request_message를 만들어 요청하고,
        data만 반환
        """
        self.requests_message["body"]["item"] = item

        # 🔍 압축 테스트 (로깅 포함)
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

        # 소켓 통신
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr, port = self.resolve_addr(self.requests_message)
        client_socket.connect((addr, port))

        try:
            client_socket.sendall(compressed)
            data = client_socket.recv(self.SOCKET_BYTE)
            message = json.loads(data.decode())
            return message

        finally:
            client_socket.close()
