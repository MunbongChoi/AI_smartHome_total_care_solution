# -*- coding: utf-8 -*-
import socket
import pickle
import time

def start_client():
    SERVER_HOST = '127.0.0.1'  # 서버의 IP 주소 (로컬 테스트의 경우 localhost)
    SERVER_PORT = 8888         # 서버의 포트 번호

    while True:
        # 서버에 보낼 데이터 예제
        data_to_send = [2, 'hard', -1, 45.5, 23.4]  # 센서 ID, 상황, 자동 모드, 값1, 값2
        
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((SERVER_HOST, SERVER_PORT))
                serialized_data = pickle.dumps(data_to_send)
                client_socket.sendall(serialized_data)
                print(f"Sent data to server: {data_to_send}")
                
                response = client_socket.recv(1024)
                deserialized_response = pickle.loads(response)
                print(f"Received response from server: {deserialized_response}")

        except Exception as e:
            print(f"An error occurred: {e}")
        
        # 1초 대기
        time.sleep(1)

if __name__ == '__main__':
    start_client()
