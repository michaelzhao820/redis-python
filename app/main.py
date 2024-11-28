import socket

def main():
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    while True:
        connection, _ = server_socket.accept()
        with connection:
            while True:
                data = connection.recv(1024)
                if not data:
                    break
                commands = data.decode().splitlines()
                for command in commands:
                    if command.strip().upper() == "PING":
                        connection.sendall(b"+PONG\r\n")

if __name__ == "__main__":
    main()
