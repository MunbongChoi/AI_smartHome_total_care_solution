# -*- coding: utf-8 -*-
import socket
import pickle
import threading
import pymysql
from concurrent.futures import ThreadPoolExecutor

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
        data = b""
        while True:
            packet = client_socket.recv(4096)
            if not packet:
                break
            data += packet

        if data:
            data_arr = pickle.loads(data)
            print('Server received data:', data_arr)
            
            # 파일 저장
            file_name = 'saved_data/'+data_arr['file_name']
            file_data = data_arr['file_data']
            with open(file_name, 'wb') as f:
                f.write(file_data)
            print(f"Saved file {file_name}")

            response = {"status": "success", "message": f"File {file_name} received"}
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
