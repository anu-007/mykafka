import socket
import struct
import threading
import uuid

API_VERSIONS_KEY = 18
DESCRIBE_TOPIC_PARTITIONS_KEY = 75
UNKNOWN_TOPIC_OR_PARTITION = 3


def encode_varint(value):
    result = []
    while True:
        byte = value & 0x7F
        value >>= 7
        if value != 0:
            byte |= 0x80
        result.append(byte)
        if value == 0:
            break
    return bytes(result)


def decode_varint(data, offset):
    value = 0
    shift = 0
    while True:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift
        if not (byte & 0x80):
            break
        shift += 7
    return value, offset


def encode_compact_string(value):
    if value is None:
        return encode_varint(0)
    encoded = value.encode("utf-8") if isinstance(value, str) else value
    return encode_varint(len(encoded) + 1) + encoded


def decode_compact_string(data, offset):
    length, offset = decode_varint(data, offset)
    if length == 0:
        return None, offset
    actual = length - 1
    value = data[offset : offset + actual].decode("utf-8")
    offset += actual
    return value, offset


def encode_compact_array(items, encode_item):
    if items is None:
        return encode_varint(0)
    result = encode_varint(len(items) + 1)
    for item in items:
        result += encode_item(item)
    return result


def decode_compact_array(data, offset, decode_item):
    length, offset = decode_varint(data, offset)
    if length == 0:
        return None, offset
    count = length - 1
    items = []
    for _ in range(count):
        item, offset = decode_item(data, offset)
        items.append(item)
    return items, offset


def parse_request_header(data):
    if len(data) < 14:
        return None
    message_size = struct.unpack(">i", data[0:4])[0]
    api_key = struct.unpack(">h", data[4:6])[0]
    api_version = struct.unpack(">h", data[6:8])[0]
    correlation_id = struct.unpack(">i", data[8:12])[0]
    client_id_length = struct.unpack(">h", data[12:14])[0]
    offset = 14
    if client_id_length > 0:
        offset += client_id_length
    tag_count, offset = decode_varint(data, offset)
    if tag_count > 0:
        # Skip tagged header fields; assume no extra data for simplicity.
        offset += tag_count - 1
    return {
        "message_size": message_size,
        "api_key": api_key,
        "api_version": api_version,
        "correlation_id": correlation_id,
        "client_id_length": client_id_length,
        "body_offset": offset,
    }


def parse_describe_topic_partitions_request(data, offset):
    def decode_topic_name(data, offset):
        return decode_compact_string(data, offset)

    topics, offset = decode_compact_array(data, offset, decode_topic_name)
    return topics


def build_api_versions_response(correlation_id, error_code=0):
    api_versions = [
        (API_VERSIONS_KEY, 0, 4),
        (DESCRIBE_TOPIC_PARTITIONS_KEY, 0, 0),
    ]
    response_body = struct.pack(">i", correlation_id)
    response_body += struct.pack(">h", error_code)
    response_body += struct.pack(">i", len(api_versions))
    for api_key, min_version, max_version in api_versions:
        response_body += struct.pack(">hhh", api_key, min_version, max_version)
    response_body += struct.pack(">i", 0)
    response = struct.pack(">i", len(response_body)) + response_body
    return response


def build_describe_topic_partitions_response(correlation_id, topics, kafka):
    def encode_partition(partition):
        body = struct.pack(">iii", partition["index"], partition["leader_id"], partition["leader_epoch"])
        body += encode_compact_array(partition["replica_nodes"], lambda n: struct.pack(">i", n))
        body += encode_compact_array(partition["isr_nodes"], lambda n: struct.pack(">i", n))
        body += encode_compact_array(partition["eligible_leader_replicas"], lambda n: struct.pack(">i", n))
        body += encode_compact_array(partition["last_known_elr"], lambda n: struct.pack(">i", n))
        body += encode_compact_array(partition["offline_replicas"], lambda n: struct.pack(">i", n))
        body += encode_varint(0)
        return body

    def encode_topic(topic_name):
        if topic_name not in kafka.topics:
            body = struct.pack(">h", UNKNOWN_TOPIC_OR_PARTITION)
            body += encode_compact_string(topic_name)
            body += b"\x00" * 16
            body += struct.pack(">b", 0)
            body += encode_compact_array([], encode_partition)
            body += struct.pack(">i", 0)
            body += encode_varint(0)
            return body
        topic = kafka.topics[topic_name]
        body = struct.pack(">h", 0)
        body += encode_compact_string(topic["name"])
        body += topic["topic_id"]
        body += struct.pack(">b", topic["is_internal"])
        body += encode_compact_array(topic["partitions"], encode_partition)
        body += struct.pack(">i", topic["authorized_operations"])
        body += encode_varint(0)
        return body

    response_body = struct.pack(">i", correlation_id)
    response_body += struct.pack(">i", 0)
    response_body += encode_compact_array(topics, encode_topic)
    response = struct.pack(">i", len(response_body)) + response_body
    return response


class MyKafka:
    def __init__(self, host="localhost", port=9092):
        self.host = host
        self.port = port
        self.server = None
        self.topics = {}

    def add_topic(self, name, partition_count=1):
        partitions = []
        for i in range(partition_count):
            partitions.append(
                {
                    "index": i,
                    "leader_id": 1,
                    "leader_epoch": 0,
                    "replica_nodes": [1],
                    "isr_nodes": [1],
                    "eligible_leader_replicas": [],
                    "last_known_elr": [],
                    "offline_replicas": [],
                }
            )
        self.topics[name] = {
            "name": name,
            "topic_id": uuid.uuid4().bytes,
            "is_internal": 0,
            "partitions": partitions,
            "authorized_operations": 0,
        }

    def init_server(self):
        self.server = socket.create_server((self.host, self.port), reuse_port=True)

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
        elif header["api_key"] == DESCRIBE_TOPIC_PARTITIONS_KEY:
            topics = parse_describe_topic_partitions_request(data, header["body_offset"])
            if topics is None:
                topics = []
            response = build_describe_topic_partitions_response(
                header["correlation_id"], topics, self
            )
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
            try:
                conn, _ = self.server.accept()
                thread = threading.Thread(target=self.handle_client, args=(conn,))
                thread.daemon = True
                thread.start()
            except OSError:
                break


def main():
    print("Hello from mykafka!")

    kafka_instance = MyKafka("localhost", 9092)
    kafka_instance.add_topic("foo", partition_count=2)
    try:
        kafka_instance.init_server()
    except OSError as e:
        print(f"Failed to start server: {e}")
        return
    kafka_instance.serve_concurrent()


if __name__ == "__main__":
    main()
