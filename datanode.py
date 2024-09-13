import socket
import threading
import os
import shutil
import time
import json
import sys 

id= int(sys.argv[1])

replicated_blocks = []
replicated_nodes = []

def check_block_exists(ip, port, block_id):
    try:
        socket_client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        socket_client.connect((ip, port))
        socket_client.send(f"CHECK_BLOCK_EXISTS@{block_id}".encode('utf-8'))
        response = socket_client.recv(1024).decode('utf-8')
        socket_client.close()
        return response == "BLOCK_EXISTS"
    except Exception as e:
        print(f"Error checking block existence: {e}")
        return False

class DataNode:
    
    def __init__(self, node_id, port, replication_factor=3):
        self.node_id = node_id
        self.port = port
        self.is_alive = True
        self.replication_factor = replication_factor
        self.data_directory = f"data_node_{node_id}"
        self.alive_nodes = []
        self.data_nodes = []
        # Create the data directory if it doesn't exist
        os.makedirs(self.data_directory, exist_ok=True)

        # Start a server to listen for requests
        self.server_thread = threading.Thread(target=self.start_server)
        self.server_thread.start()

    def start_server(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind(("172.20.10.4", self.port))
        server_socket.listen()

        print(f"[DataNode {self.node_id}] Listening on port {self.port}")

        while True:
            client_socket, addr = server_socket.accept()
            print(f"[DataNode {self.node_id}] Connection from {addr} accepted.")
            # Create a new thread that will handle the client connection
            client_thread = threading.Thread(target=self.handle_client_connection, args=(client_socket, addr))
            client_thread.start()
            
    def handle_client_connection(self, client_socket, addr):
        print(f"[DataNode {self.node_id}] Connection from {addr} accepted.")
        try:
            while True:
                request = client_socket.recv(1024).decode("utf-8")
                if not request:
                    break

                if request == "PING":
                    # Respond to the ping request
                    client_socket.send("PONG".encode("utf-8"))
                    client_socket.close()
                    self.is_alive = True  # Update DataNode status
                    print(f"[DataNode {self.node_id}] Responded to PING from NameNode")

                elif request.startswith("STORE_BLOCK"):
                    #print("DATA",data)
                    blocks = request.split("@")
                    file_name = blocks[1]
                    block_id = blocks[2]
                    block_data = blocks[3]
                    dic = blocks[4]
                    dic = json.loads(dic)
                    self.alive_nodes = dic["ports"]
                    self.data_nodes = [f"172.20.10.4:{int(i)}" for i in self.alive_nodes]
                    files = file_name+block_id
                    #print(block_id,block_data)
                    self.store_block(files, block_data,client_socket)
                    client_socket.send("ACK".encode('utf-8'))

                elif request == "Upload complete":
                    #self.replicate_data(filename, data)
                    client_socket.send("OK@".encode('utf-8'))

                elif request.startswith("DOWNLOAD"):
                    _,file_name = request.split("@")
                    block_path = os.path.join(self.data_directory, file_name)
                    with open(block_path,"r") as f:
                        content = f.read()
                        #content = [line.strip() for line in f]
                    client_socket.send(f"DOWNLOADED@{content}".encode('utf-8'))

                elif request.startswith("REPLICATE"):
                    _,file_name,content = request.split("@")
                    replicated_block_path = os.path.join(f"data_node_{self.node_id}", file_name)
                    replicated_blocks.append(file_name)
                    replicated_nodes.append(self.node_id)
                    #replicated_dic[file_name] = self.node_id
                    with open(replicated_block_path, 'w') as replicated_block_file:
                        replicated_block_file.write(content)
                    client_socket.send(f"ACK@".encode('utf-8'))
                    #print("metadata",replicated_dic)

                elif request.startswith("CHECK_BLOCK_EXISTS"):
                    _, block_id = request.split("@")
                    block_path = os.path.join(self.data_directory, block_id)
                    if os.path.exists(block_path):
                        client_socket.send("BLOCK_EXISTS".encode('utf-8'))
                    else:
                        client_socket.send("BLOCK_NOT_FOUND".encode('utf-8'))

        except Exception as e:
            print(f"An error occurred with client {addr}: {e}")
        finally:
            client_socket.close()
            print(f"[DataNode {self.node_id}] Connection from {addr} closed.")

    def store_block(self, block_id, block_data,client_socket): #block_id is the file name
        # New method to store a block of data
        block_path = os.path.join(self.data_directory, block_id)
        with open(block_path, 'w') as block_file:
            block_file.write(block_data)
        print(f"Block {block_id} stored.")

        # Replicate the data
        self.replicate_block(block_id, block_data,client_socket)

    # Inside the DataNode class
    def replicate_block(self, block_id, block_data, client_socket):
        replicated_blocks = []  # Array to store the names of replicated blocks
        replicated_nodes = []   # Array to store the node IDs where blocks are replicated

        for datanode in self.data_nodes:
            ip, port_str = datanode.split(":")
            port = int(port_str)
            if datanode != f"172.20.10.4:{self.port}":
                try:
                    replica_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    replica_socket.connect((ip, port))
                    # Check if the block already exists on the target DataNode
                    exists = check_block_exists(ip, port, block_id + "r")
                    if not exists:
                        replicated_block_id = f"{block_id}r"
                        # Send block for replication
                        replica_socket.send(f"REPLICATE@{replicated_block_id}@{block_data}".encode('utf-8'))
                        response = replica_socket.recv(1024).decode("utf-8")
                        replica_socket.close()
                        if response.startswith("ACK"):
                            # Append the file name with the replicated identifier and the node ID to the arrays
                            replicated_blocks.append(block_id + "r")
                            replicated_nodes.append(port - 5000)
                            print(f"Block {block_id} replicated to datanode {port - 5000}.")
                except Exception as e:
                    print(f"Error replicating block {block_id} to datanode {port - 5000}: {e}")
        
        # Send the arrays back to the client after replication is complete
        if replicated_blocks and replicated_nodes:
            # Convert arrays to JSON strings and concatenate with a delimiter
            data_to_send = f"REPLICATED_METADATA@{json.dumps(replicated_blocks)}@{json.dumps(replicated_nodes)}"
            client_socket.send(data_to_send.encode('utf-8'))


# Example usage:
data_node = DataNode(node_id=id, port=5000+id)

# Function to simulate a DataNode becoming unreachable (for testing purposes)
def simulate_failure():
    time.sleep(10)
    data_node.is_alive = False

# Start the simulation thread
simulate_thread = threading.Thread(target=simulate_failure)
simulate_thread.start()











