import socket
import struct
import threading

API_VERSIONS_KEY = 18


def parse_request_header(data):
    if len(data) < 14:
        return None
    message_size = struct.unpack(">i", data[0:4])[0]
    api_key = struct.unpack(">h", data[4:6])[0]
    api_version = struct.unpack(">h", data[6:8])[0]
    correlation_id = struct.unpack(">i", data[8:12])[0]
    client_id_length = struct.unpack(">h", data[12:14])[0]
    header = {
        "message_size": message_size,
        "api_key": api_key,
        "api_version": api_version,
        "correlation_id": correlation_id,
        "client_id_length": client_id_length,
    }
    return header


def build_api_versions_response(correlation_id, error_code=0):
    api_versions = [
        (API_VERSIONS_KEY, 0, 4),
    ]
    response_body = struct.pack(">i", correlation_id)
    response_body += struct.pack(">h", error_code)
    response_body += struct.pack(">i", len(api_versions))
    for api_key, min_version, max_version in api_versions:
        response_body += struct.pack(">hhh", api_key, min_version, max_version)
    response_body += struct.pack(">i", 0)
    response = struct.pack(">i", len(response_body)) + response_body
    return response


class MyKafka:
    def __init__(self, host="localhost", port=9092):
        self.host = host
        self.port = port
        self.server = None

    def init_server(self):
        self.server = socket.create_server((self.host, self.port), reuse_port=True)
        return self.server.accept()

    def send(self, conn, data):
        conn.sendall(data)
        return "OK"

    def _read_message(self, conn):
        length_bytes = conn.recv(4)
        if not length_bytes or len(length_bytes) < 4:
            return None
        message_size = struct.unpack(">i", length_bytes)[0]
        data = length_bytes
        while len(data) < 4 + message_size:
            chunk = conn.recv(4 + message_size - len(data))
            if not chunk:
                return None
            data += chunk
        return data

    def _handle_request(self, data, conn):
        header = parse_request_header(data)
        if header is None:
            return False
        if header["api_key"] == API_VERSIONS_KEY:
            response = build_api_versions_response(header["correlation_id"])
        else:
            response = build_api_versions_response(
                header["correlation_id"], error_code=35
            )
        self.send(conn, response)
        return True

    def handle_client(self, conn):
        while True:
            try:
                data = self._read_message(conn)
                if data is None:
                    break
                if not self._handle_request(data, conn):
                    break
            except Exception:
                break
        conn.close()

    def serve_concurrent(self):
        while True:
            conn, _ = self.server.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn,))
            thread.start()


def main():
    print("Hello from mykafka!")

    kafka_instance = MyKafka("localhost", 9092)
    kafka_instance.init_server()
    kafka_instance.serve_concurrent()


if __name__ == "__main__":
    main()
