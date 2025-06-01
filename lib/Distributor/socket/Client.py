import socket
import copy
import json
import base64
import zstandard as zstd

# Bridge
from lib.Distributor.socket.Interface import SocketInterface
from lib.Logger.logger import get_logger


class SocketClient(SocketInterface):
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.cctx = zstd.ZstdCompressor(level=9)

    @staticmethod
    def resolve_addr(message=None):
        return "msiwol.iptime.org", 4006

    def request_tcp(self, requests_message):
        """
        item을 입력으로 받아 request_message를 만들어 요청하고,
        정상적인 JSON 응답을 반환
        """

        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        addr, port = self.resolve_addr()
        self.logger.debug(f"Connecting to {addr}:{port}")
        client_socket.connect((addr, port))

        try:
            # 1. 요청 메시지 압축 + 인코딩 + 종료 토큰
            datagram = self.cctx.compress(json.dumps(requests_message).encode())
            datagram = base64.b64encode(datagram) + b"<END>"
            self.logger.debug(f"Datagram len: {len(datagram)}")

            client_socket.sendall(datagram)
            self.logger.debug("Datagram sent, waiting for response...")

            # 2. 응답 수신 (반복하여 <END>까지 수신)
            buffer = b""
            while True:
                chunk = client_socket.recv(1024)
                if not chunk:
                    break
                buffer += chunk
                if b"<END>" in buffer:
                    buffer = buffer.replace(b"<END>", b"")
                    break

            if not buffer:
                raise ValueError("서버에서 응답이 없습니다 (빈 응답)")

            try:
                message_str = buffer.decode(errors="replace").strip()
                self.logger.debug("Received response successfully.")
                message = json.loads(message_str)
                return message

            except json.JSONDecodeError as je:
                preview = buffer[:100] if len(buffer) >= 100 else buffer
                self.logger.error(
                    f"JSON 파싱 실패: {je} → 수신 데이터 일부: {preview!r}"
                )
                raise ValueError("유효하지 않은 JSON 응답입니다.")

        except Exception as e:
            self.logger.error(f"TCP 오류: {e}")
            raise

        finally:
            client_socket.close()
            self.logger.debug("Socket closed")
