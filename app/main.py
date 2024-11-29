import socket
import threading
from time import sleep

GLOBAL_KEY_VALUE_STORE = {}

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        client_thread = threading.Thread(target=handle_connection, args= (connection,))
        client_thread.start()


def parse_redis_protocol(data):
    parts = data.split(b'\r\n')
    num_arguments = int(parts[0][1:])
    i = 2
    args = []
    for _ in range(num_arguments):
        args.append(parts[i].decode('utf-8'))
        i += 2
    return args[0], args[1:]


def parse_command_and_args(command, args):
    match command:
        case 'ECHO':
            return f"+{args[0]}\r\n".encode('utf-8')
        case 'PING':
            return b"+PONG\r\n"
        case 'SET':
            if len(args) > 3 and args[2].upper() == 'PX':
                sleep(int(args[3]) / 1000.0)
            GLOBAL_KEY_VALUE_STORE[args[0]] = args[1]
            return b'+OK\r\n'
        case 'GET':
            if args[0] not in GLOBAL_KEY_VALUE_STORE:
                return b"$-1\r\n"
            value = GLOBAL_KEY_VALUE_STORE[args[0]]
            return f"${len(value)}\r\n{value}\r\n".encode('utf-8')
        case _:
            return b"-ERROR Unknown command\r\n"


def handle_connection(connection, ):
    with connection:
        data = connection.recv(8000)
        while data:
            command, args = parse_redis_protocol(data)
            response = parse_command_and_args(command,args)
            connection.sendall(response)
            data = connection.recv(8000)

if __name__ == "__main__":
    main()
