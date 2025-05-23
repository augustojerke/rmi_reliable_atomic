import socket
import threading
import json
import time
import random
from config import NODES, SEQUENCER, MESSAGE_LOSS_PROBABILITY, MESSAGE_DELAY_PROBABILITY, MAX_DELAY, ACK_TIMEOUT, MAX_RETRIES
from log_config import setup_logger

import sys

NODE_NAME = sys.argv[1]
HOST, PORT = NODES[NODE_NAME]

logger = setup_logger(NODE_NAME)

delivered_messages = []
ack_received = set()

pending_acks = {}
lock = threading.Lock()


def unreliable_network_simulation():
    if random.random() < MESSAGE_LOSS_PROBABILITY:
        logger.warning("Simulating message loss")
        return False
    if random.random() < MESSAGE_DELAY_PROBABILITY:
        delay = random.uniform(1, MAX_DELAY)
        logger.warning(f"Simulating message delay of {delay:.2f} seconds")
        time.sleep(delay)
    return True


def deliver(message):
    msg_id = (message['sequence'], message['content'])
    if msg_id not in delivered_messages:
        delivered_messages.append(msg_id)
        logger.info(f"Delivered: {message}")


def send_ack(seq, addr):
    try:
        ack_message = json.dumps({"type": "ACK", "sequence": seq})
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(addr)
            s.sendall(ack_message.encode())
        logger.info(f"Sent ACK for {seq} to {addr}")
    except Exception as e:
        logger.error(f"Failed to send ACK to {addr}: {e}")


def handle_incoming(conn):
    try:
        data = conn.recv(1024).decode()
        if not data:
            return

        message = json.loads(data)

        if message.get("type") == "ACK":
            seq = message['sequence']
            with lock:
                if seq in pending_acks:
                    pending_acks[seq]['ack'] = True
                    logger.info(f"Received ACK for {seq}")
            return

        if unreliable_network_simulation():
            deliver(message)
            ack_received.add(message['sequence'])

            sender = message['sender']
            send_ack(message['sequence'], NODES[sender])

    except Exception as e:
        logger.error(f"Error handling incoming: {e}")
    finally:
        conn.close()


def listen():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    logger.info(f"{NODE_NAME} listening on {HOST}:{PORT}")

    while True:
        conn, _ = server.accept()
        threading.Thread(target=handle_incoming, args=(conn,)).start()


def broadcast(content):
    message = {
        "sender": NODE_NAME,
        "content": content
    }
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect(SEQUENCER)
            s.sendall(json.dumps(message).encode())
        logger.info(f"Sent broadcast: {content}")
    except Exception as e:
        logger.error(f"Failed to send to sequencer: {e}")


def monitor_acks(message):
    seq = message['sequence']
    retries = 0
    targets = [node for node in NODES if node != NODE_NAME]

    with lock:
        pending_acks[seq] = {'ack': False}

    while retries < MAX_RETRIES:
        time.sleep(ACK_TIMEOUT)

        with lock:
            if pending_acks[seq]['ack']:
                del pending_acks[seq]
                return

        for node in targets:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(NODES[node])
                    s.sendall(json.dumps(message).encode())
                logger.warning(f"Retransmitted to {node} (seq {seq}, attempt {retries + 1})")
            except Exception as e:
                logger.error(f"Retransmission to {node} failed: {e}")

        retries += 1

    logger.error(f"No ACK received for message {seq} after {MAX_RETRIES} attempts")
    with lock:
        del pending_acks[seq]


def heartbeat_monitor():
    while True:
        for node, (host, port) in NODES.items():
            if node == NODE_NAME:
                continue
            try:
                with socket.create_connection((host, port), timeout=2):
                    logger.info(f"Heartbeat OK with {node}")
            except Exception:
                logger.error(f"Node {node} may be down")
        time.sleep(5)


if __name__ == "__main__":
    threading.Thread(target=listen, daemon=True).start()
    threading.Thread(target=heartbeat_monitor, daemon=True).start()

    while True:
        msg = input("Enter message to broadcast (or 'exit'): ")
        if msg == "exit":
            break
        broadcast(msg)
