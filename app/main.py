import socket
import threading
from argparse import ArgumentParser
from time import time

GLOBAL_KEY_VALUE_STORE = {}


def main():
    directory, db_file_name = parse_command_line_args().dir, parse_command_line_args().dbfilename
    print(directory, db_file_name)
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        client_thread = threading.Thread(target=handle_connection, args= (connection,))
        client_thread.start()

def parse_command_line_args():
    parser = ArgumentParser()
    parser.add_argument("--dir", required=True, help="Path to RDB directory")
    parser.add_argument("--dbfilename", required = True, help= "Name of RDB file")
    return parser.parse_args()


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
            add_time = None
            key, value, PX = args[0], args[1], args[2]
            if len(args) > 3 and PX.upper() == 'PX':
                add_time = time() + int(PX) / 1000
            GLOBAL_KEY_VALUE_STORE[key] = (value, add_time)
            return b'+OK\r\n'
        case 'GET':
            key = args[0]
            if key not in GLOBAL_KEY_VALUE_STORE:
                return b"$-1\r\n"
            value, expiration_time = GLOBAL_KEY_VALUE_STORE[key]
            if expiration_time and expiration_time < time():
                del GLOBAL_KEY_VALUE_STORE[key]
                return b"$-1\r\n"
            return f"${len(value)}\r\n{value}\r\n".encode('utf-8')
        case _:
            return b"-ERROR Unknown command\r\n"


def handle_connection(connection, ):
    with connection:
        data = connection.recv(8000)
        while data:
            print(data)
            command, args = parse_redis_protocol(data)
            response = parse_command_and_args(command,args)
            connection.sendall(response)
            data = connection.recv(8000)

if __name__ == "__main__":
    main()
