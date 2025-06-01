import socket
import copy
import json
import base64
import zstandard as zstd

# Bridge
from lib.Distributor.socket.Interface import SocketInterface
from lib.Distributor.socket.messages.request import requests_message
from lib.Logger.logger import get_logger


class SocketClient(SocketInterface):
    def __init__(self):
        self.requests_message = copy.deepcopy(requests_message)
        self.logger = get_logger(self.__class__.__name__)
        self.cctx = zstd.ZstdCompressor(level=9)

    @staticmethod
    def resolve_addr(message=None):
        return "msiwol.iptime.org", 4006

    def request_tcp(self, item):
        """
        item을 입력으로 받아 request_message를 만들어 요청하고,
        data만 반환
        """
        self.requests_message["body"]["item"] = item

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr, port = self.resolve_addr()
        self.logger.debug(f"Connecting to {addr}:{port}")
        client_socket.connect((addr, port))

        try:
            datagram = self.cctx.compress(json.dumps(self.requests_message).encode())
            datagram = base64.b64encode(datagram) + b"<END>"

            client_socket.sendall(datagram)

            data = client_socket.recv(1024)

            message = json.loads(data.decode())
            return message

        except Exception as e:
            self.logger.error(f"TCP 오류: {e}")
            raise
        finally:
            client_socket.close()
            self.logger.debug("Socket closed")
