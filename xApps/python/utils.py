import yaml
import socket
import ast

def read_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data

def request_ueids(slice_id, host="127.0.0.1", port=5000):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((host, port))
            client_socket.sendall(str(slice_id).encode("utf-8"))
            response = client_socket.recv(1024).decode("utf-8")
            if response != "set()":
                response = ast.literal_eval(response)
    except ConnectionRefusedError:
        print("Failed to connect to the server. Ensure the server is running.")
        response = set()
    return response
