import socket, pickle
import time
import gas_in
import gas_out

# HOST = '192.168.1.14'
HOST = 'localhost'
PORT = 8888

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST, PORT))

while True:
    setMsg = gas_in.inputValue()
    setMsg_string = pickle.dumps(setMsg)
    print('client setMsg : ', pickle.loads(setMsg_string))
    client_socket.send(setMsg_string)
    
    time.sleep(5)
                        
client_socket.close()                       
                        
