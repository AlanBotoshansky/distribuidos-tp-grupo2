LENGTH_BYTES = 3
EOF = 'EOF'

def read_bytes(socket, length):
    """Reads bytes from the socket"""
    data = bytearray()
    total_bytes_received = 0
    while total_bytes_received < length:
        bytesReceived = socket.recv(length - total_bytes_received)
        if not bytesReceived:
            raise ConnectionError("Client disconnected")
        data.extend(bytesReceived)
        total_bytes_received += len(bytesReceived)
    return data

def receive_message(socket):
    """Receives a message from the socket"""
    length_bytes = read_bytes(socket, LENGTH_BYTES)
    message_length = int.from_bytes(length_bytes, byteorder='big')
    message = read_bytes(socket, message_length)
    return message.decode('utf-8')