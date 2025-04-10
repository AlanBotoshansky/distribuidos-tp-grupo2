LENGTH_BYTES = 3
EOF = 'EOF'

def send_message(socket, message):
    encoded_message = message.encode('utf-8')
    length = len(encoded_message)
    length_bytes = length.to_bytes(LENGTH_BYTES, byteorder='big')
    socket.sendall(length_bytes + encoded_message)