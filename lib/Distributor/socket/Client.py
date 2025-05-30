import socket
import copy
import json
import base64
import zstandard as zstd

# Bridge
from lib.Distributor.socket.Interface import SocketInterface
from lib.Distributor.socket.messages.request import requests_message
from lib.Logger.logger import Logger


class SocketClient(SocketInterface):
    def __init__(self):
        self.requests_message = copy.deepcopy(requests_message)
        self.logger = Logger(self.__class__.__name__)
        self.cctx = zstd.ZstdCompressor(level=9)

    @staticmethod
    def resolve_addr(message=None):
        return "StockAnalysisAPI_service", 4006

    def request_tcp(self, item):
        """
        item을 입력으로 받아 request_message를 만들어 요청하고,
        data만 반환
        """
        self.requests_message["body"]["item"] = item

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr, port = self.resolve_addr()
        self.logger.log("DEBUG", f"Connecting to {addr}:{port}")
        client_socket.connect((addr, port))
        try:
            datagram = self.cctx.compress(json.dumps(self.requests_message).encode())
            datagram = base64.b64encode(datagram) + b"<END>"
            self.logger.log("DEBUG", f"datagram len : {len(datagram)}")
            client_socket.sendall(datagram)
            self.logger.log("DEBUG", "Datagram sent, waiting for response...")
            data = client_socket.recv(1024)
            self.logger.log("DEBUG", f"Received data: {data[:100]}...")  # 일부만 출력
            message = json.loads(data.decode())
            self.logger.log("DEBUG", f"Decoded message: {message}")
            return message

        except Exception as e:
            self.logger.log("ERROR", f"TCP 요청 중 오류: {e}")
            raise
        finally:
            client_socket.close()
            self.logger.log("DEBUG", "Socket closed")
