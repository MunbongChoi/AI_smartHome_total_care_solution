import socket
import pickle
import time
import os

DEFAULT_CHUNK_SIZE = 4096  # 기본 청크 크기 설정

def calculate_chunk_size(file_size):
    """파일 크기에 따라 동적 청크 크기를 계산"""
    if file_size <= DEFAULT_CHUNK_SIZE:
        return file_size
    return min(DEFAULT_CHUNK_SIZE, file_size // 10)

def send_wav_file(file_path, server_host, server_port):
    if not os.path.exists(file_path):
        print(f"File {file_path} does not exist")
        return

    try:
        while True:  # 재전송 요청이 있을 때를 대비한 루프
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
                client_socket.connect((server_host, server_port))
                file_size = os.path.getsize(file_path)
                file_name = os.path.basename(file_path)
                chunk_size = calculate_chunk_size(file_size)
                
                # 파일 전송 시작 신호 전송
                start_message = {
                    'temp_cid' : "00000003",
                    'file_name': file_name,
                    'file_size': file_size,
                    'chunk_size': chunk_size
                }
                client_socket.sendall(pickle.dumps(start_message))
                print(f"Sent start message for file {file_path} to server with chunk size {chunk_size}")

                # 파일을 청크 단위로 읽어서 전송
                with open(file_path, 'rb') as f:
                    while True:
                        chunk = f.read(chunk_size)
                        if not chunk:
                            break
                        client_socket.sendall(chunk)

                print(f"Finished sending file {file_path} to server")

                # 수신 완료 신호 대기
                response = client_socket.recv(DEFAULT_CHUNK_SIZE)
                deserialized_response = pickle.loads(response)
                print(f"Received response from server: {deserialized_response}")

                if deserialized_response.get("status") == "success":
                    break  # 성공적으로 전송된 경우 루프 종료
                elif deserialized_response.get("status") == "error":
                    print(f"Error from server: {deserialized_response.get('message')}. Retrying...")
                    time.sleep(1)  # 재시도 전에 잠시 대기

    except Exception as e:
        print(f"An error occurred: {e}")

def start_client():
    SERVER_HOST = 'localhost'  # 서버의 IP 주소 (로컬 테스트의 경우 localhost)
    SERVER_PORT = 8888         # 서버의 포트 번호
    FILE_PATH = './dataset/240411_01.wav'  # 전송할 WAV 파일 경로

    # while True:
    send_wav_file(FILE_PATH, SERVER_HOST, SERVER_PORT)
        # time.sleep(60)  # 1분 간격으로 전송

if __name__ == '__main__':
    start_client()