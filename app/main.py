import socket
import threading


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        client_thread = threading.Thread(target=handle_connection, args= (connection,))
        client_thread.start()


def parse_redis_protocol(data):
    parts = data.split(b'\r\n')
    print(parts)


def handle_connection(connection):
    with connection:
        data = connection.recv(8000)
        while data:
            parse_redis_protocol(data)



if __name__ == "__main__":
    main()
