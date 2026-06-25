import os
import socket
import struct
import threading
import uuid

API_VERSIONS_KEY = 18
DESCRIBE_TOPIC_PARTITIONS_KEY = 75
UNKNOWN_TOPIC_OR_PARTITION = 3
FETCH = 1


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
        (FETCH, 0, 16),
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


def _crc32c_table():
    table = []
    for i in range(256):
        crc = i
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0x82F63B78
            else:
                crc >>= 1
        table.append(crc)
    return table


CRC32C_TABLE = _crc32c_table()


def crc32c(data, crc=0):
    for byte in data:
        crc = CRC32C_TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return crc & 0xFFFFFFFF


def encode_record(value, offset_delta=0):
    body = struct.pack(">b", 0)
    body += encode_varint(0)
    body += encode_varint(offset_delta)
    body += encode_varint(0)
    encoded_value = value.encode("utf-8") if isinstance(value, str) else value
    body += encode_varint(len(encoded_value) + 1) + encoded_value
    body += encode_varint(1)
    return encode_varint(len(body) + 1) + body


def build_record_batch(records, base_offset=0):
    records_bytes = b"".join(records)
    count = len(records)
    post_crc = struct.pack(">h", 0)
    post_crc += struct.pack(">i", max(0, count - 1))
    post_crc += struct.pack(">q", 0)
    post_crc += struct.pack(">q", 0)
    post_crc += struct.pack(">q", -1)
    post_crc += struct.pack(">h", -1)
    post_crc += struct.pack(">i", -1)
    post_crc += struct.pack(">i", count)
    post_crc += records_bytes
    crc_data = struct.pack(">i", 0) + b"\x02" + post_crc
    crc = crc32c(crc_data)
    batch = struct.pack(">q", base_offset)
    batch += struct.pack(">i", 4 + 1 + 4 + len(post_crc))
    batch += struct.pack(">i", 0)
    batch += b"\x02"
    batch += struct.pack(">I", crc)
    batch += post_crc
    return batch


def empty_record_batch():
    return build_record_batch([])


def parse_fetch_request(data, offset):
    offset += 4  # replica_id
    offset += 4  # max_wait_ms
    offset += 4  # min_bytes
    offset += 4  # max_bytes
    offset += 1  # isolation_level
    offset += 4  # session_id
    offset += 4  # session_epoch

    def decode_partition(data, offset):
        partition = struct.unpack(">i", data[offset : offset + 4])[0]
        offset += 4
        offset += 4  # current_leader_epoch
        fetch_offset = struct.unpack(">q", data[offset : offset + 8])[0]
        offset += 8
        offset += 4  # last_fetched_epoch
        offset += 8  # log_start_offset
        offset += 4  # partition_max_bytes
        tag_count, offset = decode_varint(data, offset)
        if tag_count > 0:
            offset += tag_count - 1
        return {"partition": partition, "fetch_offset": fetch_offset}, offset

    def decode_topic(data, offset):
        topic_id = data[offset : offset + 16]
        offset += 16
        partitions, offset = decode_compact_array(data, offset, decode_partition)
        return {"topic_id": topic_id, "partitions": partitions}, offset

    topics, offset = decode_compact_array(data, offset, decode_topic)
    return topics


EMPTY_RECORDS = encode_varint(len(empty_record_batch()) + 1) + empty_record_batch()


def _encode_fetch_partition(partition_request, topic, kafka, error_code=0):
    partition_index = partition_request["partition"]
    fetch_offset = partition_request["fetch_offset"]
    records = EMPTY_RECORDS
    high_watermark = 0
    if error_code == 0:
        messages = kafka.read_messages(topic["name"], partition_index, fetch_offset)
        high_watermark = fetch_offset + len(messages)
        if messages:
            encoded = [encode_record(m.decode("utf-8"), i) for i, m in enumerate(messages)]
            batch = build_record_batch(encoded, fetch_offset)
            records = encode_varint(len(batch) + 1) + batch
    body = struct.pack(">i", partition_index)
    body += struct.pack(">h", error_code)
    body += struct.pack(">q", high_watermark)
    body += struct.pack(">q", high_watermark)
    body += struct.pack(">q", 0)
    body += encode_compact_array([], lambda _: b"")
    body += struct.pack(">i", -1)
    body += records
    body += encode_varint(0)
    return body


def _find_topic_by_id(kafka, topic_id):
    for topic in kafka.topics.values():
        if topic["topic_id"] == topic_id:
            return topic
    return None


def _encode_fetch_topic(topic_request, kafka):
    topic_id = topic_request["topic_id"]
    topic = _find_topic_by_id(kafka, topic_id)
    if topic is None:
        partitions = [
            _encode_fetch_partition(p, None, kafka, UNKNOWN_TOPIC_OR_PARTITION)
            for p in topic_request["partitions"]
        ]
    else:
        valid_partitions = {p["index"] for p in topic["partitions"]}
        partitions = []
        for p in topic_request["partitions"]:
            error_code = 0 if p["partition"] in valid_partitions else UNKNOWN_TOPIC_OR_PARTITION
            partitions.append(_encode_fetch_partition(p, topic, kafka, error_code))
    body = topic_id
    body += encode_compact_array(partitions, lambda p: p)
    body += encode_varint(0)
    return body


def build_fetch_response(correlation_id, fetch_topics, kafka):
    response_body = struct.pack(">i", correlation_id)
    response_body += struct.pack(">i", 0)
    response_body += struct.pack(">h", 0)
    response_body += struct.pack(">i", 0)
    response_body += encode_compact_array(
        fetch_topics, lambda topic: _encode_fetch_topic(topic, kafka)
    )
    response = struct.pack(">i", len(response_body)) + response_body
    return response


class MyKafka:
    def __init__(self, host="localhost", port=9092, data_dir="data"):
        self.host = host
        self.port = port
        self.server = None
        self.topics = {}
        self.data_dir = data_dir

    def _log_path(self, topic, partition):
        return os.path.join(self.data_dir, topic, str(partition), "log")

    def _ensure_log_dir(self, topic, partition):
        path = os.path.join(self.data_dir, topic, str(partition))
        os.makedirs(path, exist_ok=True)

    def add_topic(self, name, partition_count=1):
        partitions = []
        for i in range(partition_count):
            self._ensure_log_dir(name, i)
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

    def append_message(self, topic, partition, value):
        if topic not in self.topics:
            return False
        if partition < 0 or partition >= len(self.topics[topic]["partitions"]):
            return False
        self._ensure_log_dir(topic, partition)
        path = self._log_path(topic, partition)
        encoded = value.encode("utf-8") if isinstance(value, str) else value
        with open(path, "ab") as f:
            f.write(struct.pack(">i", len(encoded)))
            f.write(encoded)
        return True

    def read_messages(self, topic, partition, fetch_offset=0, max_messages=100):
        path = self._log_path(topic, partition)
        if not os.path.exists(path):
            return []
        messages = []
        current_offset = 0
        with open(path, "rb") as f:
            while current_offset < fetch_offset + max_messages:
                length_bytes = f.read(4)
                if len(length_bytes) < 4:
                    break
                length = struct.unpack(">i", length_bytes)[0]
                value = f.read(length)
                if len(value) < length:
                    break
                if current_offset >= fetch_offset:
                    messages.append(value)
                current_offset += 1
                if len(messages) >= max_messages:
                    break
        return messages

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
        elif header["api_key"] == FETCH:
            fetch_topics = parse_fetch_request(data, header["body_offset"])
            if fetch_topics is None:
                fetch_topics = []
            response = build_fetch_response(
                header["correlation_id"], fetch_topics, self
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
    kafka_instance.append_message("foo", 0, "hello")
    kafka_instance.append_message("foo", 0, "world")
    kafka_instance.append_message("foo", 1, "from partition 1")
    try:
        kafka_instance.init_server()
    except OSError as e:
        print(f"Failed to start server: {e}")
        return
    kafka_instance.serve_concurrent()


if __name__ == "__main__":
    main()
