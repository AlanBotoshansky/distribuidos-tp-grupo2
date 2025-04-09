LENGTH_BYTES = 2

def send_message(socket, message):
    encoded_message = message.encode('utf-8')
    length = len(encoded_message)
    length_bytes = length.to_bytes(LENGTH_BYTES, byteorder='big')
    socket.sendall(length_bytes + encoded_message)