import socket
import threading
import json
import time
import sys

with open('config.json') as f:
    config = json.load(f)

NODE_NAME = sys.argv[1]
NODE_HOST, NODE_PORT = config['nodes'][NODE_NAME]
SEQUENCER_HOST, SEQUENCER_PORT = config['sequencer']

received_messages = []
lock = threading.Lock()

def listen_to_nodes():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((NODE_HOST, NODE_PORT))
    server.listen()
    print(f"[{NODE_NAME}] Listening on {NODE_HOST}:{NODE_PORT}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_node_connection, args=(conn,), daemon=True).start()

def handle_node_connection(conn):
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break
            message = json.loads(data.decode())
            deliver(message)
        except:
            break
    conn.close()

def connect_to_sequencer():
    seq_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    seq_conn.connect((SEQUENCER_HOST, SEQUENCER_PORT))
    threading.Thread(target=receive_from_sequencer, args=(seq_conn,), daemon=True).start()
    return seq_conn

def receive_from_sequencer(conn):
    while True:
        try:
            data = conn.recv(1024)
            if not data:
                break
            message = json.loads(data.decode())
            forward_to_nodes(message)
            deliver(message)
        except:
            break
    conn.close()

def forward_to_nodes(message):
    for name, (host, port) in config['nodes'].items():
        if name == NODE_NAME:
            continue
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((host, port))
                s.sendall(json.dumps(message).encode())
        except:
            pass 

def deliver(message):
    with lock:
        if message not in received_messages:
            received_messages.append(message)
            received_messages.sort(key=lambda x: x['sequence'])
            print(f"[{NODE_NAME}][DELIVER] {message}")

def send_message(seq_conn):
    while True:
        msg = input(f"[{NODE_NAME}] Type message: ")
        message = {
            'sender': NODE_NAME,
            'content': msg
        }
        seq_conn.sendall(json.dumps(message).encode())

def main():
    threading.Thread(target=listen_to_nodes, daemon=True).start()
    seq_conn = connect_to_sequencer()
    send_message(seq_conn)

if __name__ == "__main__":
    main()
