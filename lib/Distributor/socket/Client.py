import socket
import copy
import json
import zlib

# Bridge
from lib.Distributor.socket.Client import SocketInterface
from lib.Distributor.socket.messages.request import requests_message


class SocketClient(SocketInterface):
    def __init__(self):
        self.requests_message = copy.deepcopy(requests_message)

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
        addr, port = self.resolve_addr(self.requests_message)
        client_socket.connect((addr, port))
        try:
            datagram = zlib.compress(json.dumps(self.requests_message).encode())
            client_socket.sendall(datagram)
            data = client_socket.recv(self.SOCKET_BYTE)
            message = json.loads(data.decode())
            return message

        finally:
            client_socket.close()
