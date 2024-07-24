import socket
import threading
import pymysql
import time
import logging

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# DB 접속 설정
DB_CONFIG = {
    'host': "3.34.98.103",
    'user': "totalSOM", 
    'password': "rhdgkdrlavh4536",
    'database': "tsDB",
    'charset': 'utf8'
}

# 커넥션 풀을 사용하여 동시 데이터베이스 접근을 관리
db_lock = threading.Lock()

def get_db_connection():
    return pymysql.connect(**DB_CONFIG)

def autoCheck(data):
    try:
        is_auto = data[2]
        if is_auto == 1 or is_auto == 0:
            return [data[0], data[1]]
        else:
            with db_lock:
                con = get_db_connection()
                cur = con.cursor()

                if data[0] == 1:
                    sql = 'SELECT * FROM airSensorInfo ORDER BY date DESC LIMIT 2'
                elif data[0] == 2:
                    sql = 'SELECT * FROM motionSensorInfo ORDER BY date DESC LIMIT 2'
                elif data[0] == 3:
                    sql = 'SELECT * FROM tempSensorInfo ORDER BY date DESC LIMIT 2'
                elif data[0] == 4:
                    sql = 'SELECT * FROM rainSensorInfo ORDER BY date DESC LIMIT 2'
                elif data[0] == 5:
                    sql = 'SELECT * FROM gasSensorInfo ORDER BY date DESC LIMIT 2'

                cur.execute(sql)
                i = cur.fetchall()
                db_auto = i[1][2]
                con.close()

            if db_auto == 0:
                return [data[0], data[1]]
            else:
                return -1
    except Exception as e:
        logging.error(f"autoCheck error: {e}")
        return -1

def socketProgramming(host, portNum, getMsg):
    try:
        c = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        c.connect((host, portNum))
        c.send(str(getMsg[1]).encode())
        c.close()
        logging.info(f"Sent data to {host}:{portNum} - {getMsg[1]}")
    except Exception as e:
        logging.error(f"socketProgramming error: {e}")

def handle_sensor_data(sensor_id, sql, host, port):
    try:
        with db_lock:
            con = get_db_connection()
            cur = con.cursor()
            cur.execute(sql)
            data = cur.fetchone()
            con.close()

        if data:
            data_list = list(data)
            data_list[0] = sensor_id
            data = tuple(data_list)
            getMsg = autoCheck(data)
            if getMsg != -1:
                socketProgramming(host, port, getMsg)
    except Exception as e:
        logging.error(f"handle_sensor_data error: {e}")

def main_loop():
    while True:
        sensor_tasks = [
            (1, "SELECT * FROM airSensorInfo ORDER BY date DESC LIMIT 1", '192.168.1.16', 10002),
            (2, "SELECT * FROM motionSensorInfo ORDER BY date DESC LIMIT 1", '192.168.1.13', 10012),
            (3, "SELECT * FROM tempSensorInfo ORDER BY date DESC LIMIT 1", '192.168.1.18', 10022),
            (4, "SELECT * FROM rainSensorInfo ORDER BY date DESC LIMIT 1", '192.168.1.17', 10032),
            (5, "SELECT * FROM gasSensorInfo ORDER BY date DESC LIMIT 1", '192.168.1.15', 10042)
        ]

        threads = []
        for sensor_id, sql, host, port in sensor_tasks:
            t = threading.Thread(target=handle_sensor_data, args=(sensor_id, sql, host, port))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        time.sleep(3)

if __name__ == "__main__":
    main_loop()