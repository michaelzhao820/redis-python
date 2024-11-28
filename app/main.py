import socket

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        with connection:
            while connection.recv(8000):
                connection.sendall(b"+PONG\r\n")

if __name__ == "__main__":
    main()
