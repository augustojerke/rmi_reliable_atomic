import socket
import threading
import json
from config import NODES, SEQUENCER
from log_config import setup_logger

logger = setup_logger("Sequencer")

sequence_number = 0

def handle_connection(conn):
    global sequence_number
    try:
        data = conn.recv(1024).decode()
        message = json.loads(data)
        sequence_number += 1
        message['sequence'] = sequence_number

        logger.info(f"Sequenced message {message}")

        for node_name, (host, port) in NODES.items():
            send_to_node(host, port, message)

    except Exception as e:
        logger.error(f"Error handling connection: {e}")
    finally:
        conn.close()

def send_to_node(host, port, message):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((host, port))
            s.sendall(json.dumps(message).encode())
    except Exception as e:
        logger.error(f"Failed to send to {host}:{port} - {e}")

def start_sequencer():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(SEQUENCER)
    server.listen()

    logger.info(f"Sequencer listening on {SEQUENCER}")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_connection, args=(conn,)).start()

if __name__ == "__main__":
    start_sequencer()
