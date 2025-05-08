from datetime import datetime

LENGTH_PACKET_TYPE = 1
LENGTH_FIELD = 2

def encode_packet_type(packet_type):
    return packet_type.to_bytes(LENGTH_PACKET_TYPE, 'big')

def encode_string(s):
    if s is None:
        return (0).to_bytes(LENGTH_FIELD, 'big')
    data_bytes = s.encode('utf-8')
    return len(data_bytes).to_bytes(LENGTH_FIELD, 'big') + data_bytes

def encode_strings_iterable(strings):
    if not strings:
        return (0).to_bytes(LENGTH_FIELD, 'big')
    data_bytes = b''
    for string in strings:
        data_bytes += encode_string(string)
    return len(data_bytes).to_bytes(LENGTH_FIELD, 'big') + data_bytes

def encode_date(d):
    iso = d.isoformat() if d is not None else None
    return encode_string(iso)

def encode_num(n):
    s = str(n) if n is not None else None
    return encode_string(s)

def decode_string(data_bytes):
    return data_bytes.decode('utf-8') if data_bytes else ''

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
    return datetime.fromisoformat(s).date() if s else None

def decode_int(data_bytes):
    s = decode_string(data_bytes)
    return int(s) if s else None

def decode_float(data_bytes):
    s = decode_string(data_bytes)
    return float(s) if s else None

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
