import socket
import threading
import json

HOST = 'localhost'
PORT = 6000

message_counter = 0
lock = threading.Lock()
clients = []

def handle_client(conn, addr):
    global message_counter
    print(f"[Sequencer] Connected by {addr}")
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break
            message = json.loads(data.decode())
            with lock:
                message_counter += 1
                message['sequence'] = message_counter
            broadcast(message)
        except:
            break
    conn.close()

def broadcast(message):
    for client in clients:
        try:
            client.sendall(json.dumps(message).encode())
        except:
            pass

def main():
    print("[Sequencer] Starting...")
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((HOST, PORT))
        s.listen()
        while True:
            conn, addr = s.accept()
            clients.append(conn)
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    main()
