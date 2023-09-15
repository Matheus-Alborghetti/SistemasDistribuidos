import socket
import threading
import time

class Node:
    def __init__(self, port, neighbors):
        self.port = port
        self.neighbors = neighbors
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("localhost", port))
        self.socket.settimeout(5.0)
        
        self.is_alive = True
        self.failed_nodes = set()
        
        # Iniciar threads para enviar e receber heartbeats
        threading.Thread(target=self.send_heartbeats).start()
        threading.Thread(target=self.receive_heartbeats).start()

    def send_message(self, message, port):
        self.socket.sendto(message.encode(), ("localhost", port))

    def receive_message(self):
        try:
            data, addr = self.socket.recvfrom(1024)
            return data.decode()
        except (socket.timeout, ConnectionResetError):
            return None

    def send_heartbeats(self):
        while self.is_alive:
            for port in self.neighbors:
                self.send_message("heartbeat", port)
            time.sleep(2)

    def receive_heartbeats(self):
        while self.is_alive:
            message = self.receive_message()
            if message is None:
                # Se não houver resposta dentro do timeout, marque o nodo como falho
                for port in self.neighbors:
                    if port not in self.failed_nodes:
                        print(f"Node on port {self.port} detected failure in node on port {port}")
                        self.failed_nodes.add(port)
            elif message == "heartbeat":
                pass  # Ignorar heartbeats para este exemplo

    def stop(self):
        self.is_alive = False
        self.socket.close()

# Crie os nodos
node1 = Node(5000, [5001, 5002])
node2 = Node(5001, [5000, 5002])
node3 = Node(5002, [5000, 5001])

# Teste para detecção de falhas, pare um nodo após algum tempo e veja como os outros detectam a falha
time.sleep(10)
node1.stop()
time.sleep(2)  # pequena pausa para os outros nodos processarem a informação

# Aguardar e detectar falhas nos outros nodos
time.sleep(10)
node2.stop()
time.sleep(2)  # pequena pausa para os outros nodos processarem a informação

node3.stop()
