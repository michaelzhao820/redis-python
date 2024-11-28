import socket
import threading


def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        client_thread = threading.Thread(target=handle_connection, args= (connection,))
        client_thread.start()

def handle_connection(connection):
    with connection:
        while connection.recv(8000):
            connection.sendall(b"+PONG\r\n")


if __name__ == "__main__":
    main()
