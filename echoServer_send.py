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
DB_CONFIG = {
    'host': "3.34.98.103",
    'user': "totalSOM", 
    'password': "rhdgkdrlavh4536",
    'database': "tsDB",
    'charset': 'utf8'
}

# 데이터베이스 연결 함수
def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

# 최근 레코드를 가져오는 함수
def recvDB(senserId):
    con = get_db_connection()
    cur = con.cursor()
    try:
        if senserId == 1: # 미세먼지
            sql = "SELECT * FROM airSensorInfo ORDER BY date DESC LIMIT 1"
        elif senserId == 2: # 움직임
            sql = "SELECT * FROM motionSensorInfo ORDER BY date DESC LIMIT 1"
        elif senserId == 3: # 냉난방기
            sql = "SELECT * FROM tempSensorInfo ORDER BY date DESC LIMIT 1"
        elif senserId == 4: # 비
            sql = "SELECT * FROM rainSensorInfo ORDER BY date DESC LIMIT 1"
        elif senserId == 5: # 가스
            sql = "SELECT * FROM gasSensorInfo ORDER BY date DESC LIMIT 1"
        
        cur.execute(sql)
        data = cur.fetchone()
        db_auto = data[2]
    except:
        db_auto = 99
    finally:
        con.close()
    return db_auto

def compareDB(db_auto, is_auto):
    if db_auto == 1 and is_auto == -1:
        is_auto = 1
    elif db_auto == -1 and is_auto == 1:
        is_auto = -1
    elif db_auto == 0:
        is_auto = -1
    return is_auto

def sendDB(userid, data_arr):
    con = get_db_connection()
    cur = con.cursor()
    try:
        if data_arr[0] == 1: # 미세먼지
            sql = "INSERT INTO airSensorInfo (user_id, situation, is_auto, air1_value, air2_value) VALUES (%s, %s, %s, %s, %s)"
            val = (userid, data_arr[1], data_arr[2], data_arr[3], data_arr[4])
        elif data_arr[0] == 2: # 움직임
            sql = "INSERT INTO motionSensorInfo (user_id, situation, is_auto, motion_value) VALUES (%s, %s, %s, %s)"
            val = (userid, data_arr[1], data_arr[2], data_arr[3])
        elif data_arr[0] == 3: # 냉난방기
            sql = "INSERT INTO tempSensorInfo (user_id, situation, is_auto, temp_value, humid_value) VALUES (%s, %s, %s, %s, %s)"
            val = (userid, data_arr[1], data_arr[2], data_arr[3], data_arr[4])
        elif data_arr[0] == 4: # 비
            sql = "INSERT INTO rainSensorInfo (user_id, situation, is_auto, rain_value) VALUES (%s, %s, %s, %s)"
            val = (userid, data_arr[1], data_arr[2], data_arr[3])
        elif data_arr[0] == 5: # 가스
            sql = "INSERT INTO gasSensorInfo (user_id, situation, is_auto, gas_value) VALUES (%s, %s, %s, %s)"
            val = (userid, data_arr[1], data_arr[2], data_arr[3])
        
        cur.execute(sql, val)
        con.commit()
    finally:
        con.close()

def handle_client(client_socket, addr):
    try:
        client_socket.settimeout(TIMEOUT)
        # 시작 신호 수신
        start_message = client_socket.recv(DEFAULT_CHUNK_SIZE)
        start_data = pickle.loads(start_message)
        folder_name = f"./saved_data/{start_data['temp_cid']}/"
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
    
