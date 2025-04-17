from datetime import datetime

LENGTH_PACKET_TYPE = 1
LENGTH_FIELD = 2

def encode_packet_type(packet_type):
    return packet_type.to_bytes(LENGTH_PACKET_TYPE, 'big')

def encode_string(s):
    data_bytes = s.encode('utf-8')
    return len(data_bytes).to_bytes(LENGTH_FIELD, 'big') + data_bytes

def encode_strings_iterable(strings):
    data_bytes = b''
    for string in strings:
        data_bytes += encode_string(string)
    return len(data_bytes).to_bytes(LENGTH_FIELD, 'big') + data_bytes

def encode_date(d):
    return encode_string(d.isoformat())

def encode_num(n):
    s = str(n)
    return encode_string(s)

def decode_string(data_bytes):
    return data_bytes.decode('utf-8')

def decode_strings_list(data_bytes):
    offset = 0
    res = []
    while offset < len(data_bytes):
        length_elem = int.from_bytes(data_bytes[offset:offset + LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        elem = data_bytes[offset:offset + length_elem]
        res.append(decode_string(elem))
        offset += length_elem
    return res

def decode_date(data_bytes):
    s = decode_string(data_bytes)
    return datetime.fromisoformat(s).date()

def decode_int(data_bytes):
    s = decode_string(data_bytes)
    return int(s)

def decode_float(data_bytes):
    s = decode_string(data_bytes)
    return float(s)

def decode_strings_set(data_bytes):
    offset = 0
    res = set()
    while offset < len(data_bytes):
        length_elem = int.from_bytes(data_bytes[offset:offset + LENGTH_FIELD], 'big')
        offset += LENGTH_FIELD
        elem = data_bytes[offset:offset + length_elem]
        res.add(decode_string(elem))
        offset += length_elem
    return res
