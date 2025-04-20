import csv
import io

LENGTH_BYTES = 3
EOF = 'EOF'
LINES_SEPARATOR = ';'

def send_message(socket, message):
    """Sends a message to the socket"""
    encoded_message = message.encode('utf-8')
    length = len(encoded_message)
    length_bytes = length.to_bytes(LENGTH_BYTES, byteorder='big')
    socket.sendall(length_bytes + encoded_message)
    
def send_lines(socket, lines):
    """Sends a list of lines to the socket"""
    output = io.StringIO()
    writer = csv.writer(output, delimiter=LINES_SEPARATOR, quotechar='"', quoting=csv.QUOTE_MINIMAL)
    writer.writerow(lines)
    message = output.getvalue().rstrip('\r\n')
    send_message(socket, message)

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

def parse_lines_message(message):
    """Parses a message into a list of lines"""
    input_data = io.StringIO(message)
    reader = csv.reader(input_data, delimiter=LINES_SEPARATOR, quotechar='"')
    return next(reader)

def receive_lines_message(socket):
    """Receives a message from the socket and parses it into a list of lines"""
    message = receive_message(socket)
    return parse_lines_message(message)