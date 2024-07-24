# -*- coding: utf-8 -*-
import socket
import pickle
import time
import os

def send_wav_file(file_path, server_host, server_port):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist")
        return

    with open(file_path, 'rb') as f:
        wav_data = f.read()

    data_to_send = {
        'file_name': os.path.basename(file_path),
        'file_data': wav_data
    }
    
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
            client_socket.connect((server_host, server_port))
            serialized_data = pickle.dumps(data_to_send)
            client_socket.sendall(serialized_data)
            print(f"Sent file {file_path} to server")
            
            response = client_socket.recv(64)
            deserialized_response = pickle.loads(response)
            print(f"Received response from server: {deserialized_response}")

    except Exception as e:
        print(f"An error occurred: {e}")

def start_client():
    SERVER_HOST = 'localhost'  # 서버의 IP 주소 (로컬 테스트의 경우 localhost)
    SERVER_PORT = 8888         # 서버의 포트 번호
    FILE_PATH = './dataset/20240531163431.wav'  # 전송할 WAV 파일 경로

    while True:
        send_wav_file(FILE_PATH, SERVER_HOST, SERVER_PORT)
        time.sleep(1)

if __name__ == '__main__':
    start_client()
    
    
