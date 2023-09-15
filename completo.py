import socket
import time
import hashlib
import threading

class Node:
    def __init__(self, port, secret_key):
        self.port = port
        self.tasks = []
        self.key_hash = hashlib.sha256(secret_key.encode()).hexdigest()

        # Inicializando soquete para enviar mensagens
        self.send_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        # Inicializando soquete para ouvir mensagens
        self.listen_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listen_socket.bind(("localhost", port))
        self.listen_socket.settimeout(2)

        # Iniciando o loop de ouvir mensagens em uma thread
        self.thread = threading.Thread(target=self.listen_messages)
        self.thread.start()

    def send_message(self, message, port):
        self.send_socket.sendto(message.encode(), ("localhost", port))

    def receive_message(self, sock):
        try:
            data, addr = sock.recvfrom(1024)
            return data.decode(), addr
        except socket.timeout:
            return None, None

    def add_task(self, task):
        self.tasks.append(task)

    def is_alive(self, other_port):
        try:
            self.send_message("ping", other_port)
            response, _ = self.receive_message(self.send_socket)  # Aqui usamos o send_socket para receber a resposta
            if response == "pong":
                return True
        except:
            return False

    def listen_messages(self):
        while True:
            message, addr = self.receive_message(self.listen_socket)  # Aqui usamos o listen_socket
            
            if message:  # Adicionamos esta verificação aqui
                if message == "ping":
                    self.send_message("pong", addr[1])
                elif "update:" in message:
                    _, data = message.split(":")
                    if data not in self.tasks:
                        self.add_task(data)

# ------ Melhoria 1 - Balanceamento de Carga ------
class LoadBalancer:
    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def distribute_task(self, task):
        target_node = min(self.nodes, key=lambda x: len(x.tasks))
        target_node.add_task(task)

# ------ Melhoria 2 - Tolerância a Falhas ------
def check_node_health(node, other_nodes):
    for other_node in other_nodes:
        if not node.is_alive(other_node.port):
            return False
    return True

# ------ Melhoria 3 - Consistência de Dados ------
def update_data(node, data, all_nodes):
    for other_node in all_nodes:
        if other_node != node:
            other_node.send_message(f"update:{data}", other_node.port)

# ------ Melhoria 4 - Segurança ------
def authenticate(node, secret_key):
    hash_key = hashlib.sha256(secret_key.encode()).hexdigest()
    return node.key_hash == hash_key

# Testando o sistema

# Criando nodos
node1 = Node(5000, "secret1")
node2 = Node(5001, "secret2")
node3 = Node(5002, "secret3")

# Adicionando nodos ao balanceador de carga
lb = LoadBalancer()
lb.add_node(node1)
lb.add_node(node2)
lb.add_node(node3)

# Distribuindo tarefas
lb.distribute_task("Task1")
lb.distribute_task("Task2")
lb.distribute_task("Task3")

# Verificando a saúde dos nodos
time.sleep(2) # Dar algum tempo para as threads iniciarem e se comunicarem
print(check_node_health(node1, [node2, node3]))

# Atualizando dados entre nodos
update_data(node1, "Task4", [node2, node3])

# Verificando autenticação
print(authenticate(node1, "secret1"))

