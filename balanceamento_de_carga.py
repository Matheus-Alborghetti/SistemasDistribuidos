import socket
import threading
import json

class Node:
    def __init__(self, port):
        self.tasks = []
        self.port = port
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(('localhost', port))
        self.is_alive = True
        self.thread = threading.Thread(target=self.receive_message, name="receive_message")
        self.thread.start()

    def send_message(self, message, port):
        self.socket.sendto(json.dumps(message).encode(), ("localhost", port))

    def receive_message(self):
        while self.is_alive:
            try:
                data, addr = self.socket.recvfrom(1024)
                return json.loads(data.decode())
            except socket.error:
                break  # Encerra o loop se houver um erro no socket

        # Fechando o socket após encerrar o loop
        self.socket.close()

    def add_task(self, task):
        self.tasks.append(task)
        if len(self.tasks) > 5:
            self.redistribute_tasks()

    def redistribute_tasks(self):
        while len(self.tasks) > 5:
            task_to_redistribute = self.tasks.pop()
            min_load_port = self.find_least_loaded_node()
            if min_load_port:
                self.send_message({"type": "task", "task": task_to_redistribute}, min_load_port)

    def find_least_loaded_node(self):
        # Replace 9001, 9002, 9003 with your node ports if they are different.
        node_ports = [9001, 9002, 9003]
        node_ports.remove(self.port)  # Remove current node from list
        loads = {port: self.get_node_load(port) for port in node_ports}
        return min(loads, key=loads.get)

    def get_node_load(self, port):
        self.send_message({"type": "load_request"}, port)
        response = self.receive_message()
        if response and response["type"] == "load_response":
            return response["load"]
        return float('inf')  # Return infinity if no response

    def stop(self):
        self.is_alive = False

class LoadBalancer:
    def __init__(self):
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def distribute_task(self, task):
        # Simplified logic: send task to the node with least tasks
        target_node = min(self.nodes, key=lambda x: len(x.tasks))
        target_node.add_task(task)

# Example usage:

# Create nodes and a load balancer
node1 = Node(9001)
node2 = Node(9002)
node3 = Node(9003)

lb = LoadBalancer()
lb.add_node(node1)
lb.add_node(node2)
lb.add_node(node3)

# Distribute tasks
for i in range(15):
    lb.distribute_task(f'Task {i}')

print("Node 1 tasks:", node1.tasks)
print("Node 2 tasks:", node2.tasks)
print("Node 3 tasks:", node3.tasks)

# Ensure nodes are stopped to free up resources
node1.stop()
node2.stop()
node3.stop()
