# -*- coding: utf-8 -*-
import socket
import pickle
import threading
import pymysql
from concurrent.futures import ThreadPoolExecutor
import os

TIMEOUT = 30  # 타임아웃 설정 (초)
DEFAULT_CHUNK_SIZE = 8192
# DB 접속 설정

def handle_client(client_socket, addr):
    try:
        client_socket.settimeout(TIMEOUT)
        # 시작 신호 수신
        start_message = client_socket.recv(DEFAULT_CHUNK_SIZE)
        start_data = pickle.loads(start_message)
        folder_name = f"./saved_data/{start_data['temp_cid']}/"
        if not os.path.exists(folder_name):
            os.mkdir(folder_name)
        file_name = folder_name+start_data['file_name']
        file_size = start_data['file_size']
        chunk_size = start_data['chunk_size']
        print(f"Receiving file {file_name} of size {file_size} bytes from {addr}")
        received_size = 0
        with open(file_name, 'wb') as f:
            while received_size < file_size:
                try:
                    chunk = client_socket.recv(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    received_size += len(chunk)
                    # print(f"Received {received_size} of {file_size} bytes")
                except socket.timeout:
                    print(f"Timeout while receiving file {file_name} from {addr}. Requesting retransmission.")
                    response = {"status": "error", "message": "Timeout occurred. Please retransmit the file."}
                    serialized_response = pickle.dumps(response)
                    client_socket.sendall(serialized_response)
                    return

        if received_size == file_size:
            print(f"Saved file {file_name}")
            response = {"status": "success", "message": f"File {file_name} received"}
        else:
            print(f"Error: Received {received_size} bytes, expected {file_size} bytes")
            response = {"status": "error", "message": f"Received {received_size} bytes, expected {file_size} bytes"}

        serialized_response = pickle.dumps(response)
        client_socket.sendall(serialized_response)

    except Exception as e:
        print(f"Error handling client {addr}: {e}")
    finally:
        client_socket.close()
        
# 멀티쓰레딩 서버 설정
def start_server():
    HOST = 'localhost'  # 모든 네트워크 인터페이스에서 접속 허용
    PORT = 8888
    ADDR = (HOST, PORT)
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind(ADDR)
    server_socket.listen(5)  # 클라이언트를 최대 5개까지 받을 수 있다

    with ThreadPoolExecutor(max_workers=5) as executor:
        while True:
            client_socket, addr = server_socket.accept()
            print('Connected by', addr)
            executor.submit(handle_client, client_socket, addr)
    
    server_socket.close()

if __name__ == '__main__':
    start_server()
    
