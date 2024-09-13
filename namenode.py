import socket
import threading
import time
import json

IP = "172.20.10.4"
PORT = 4456
ADDR = (IP, PORT)
SIZE = 1024
FORMAT = "utf-8"

class NameNode:
    def __init__(self):
        self.file_system = {}
        self.file_names = {}
        self.replica_block =[]
        self.replica_node =[]
        self.virtual_tree = {}
        self.ping_interval = 5
        self.running = True
        self.data_nodes = {
            1: {"ip": "172.20.10.4", "port": 5001},
            2: {"ip": "172.20.10.4", "port": 5002},
            3: {"ip": "172.20.10.4", "port": 5003}
        }
    def start(self):
        ping_thread = threading.Thread(target=self.ping_data_nodes)
        ping_thread.start()

        self.main_loop()

    def main_loop(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(ADDR)
        server.listen()
        print(f"[LISTENING] Server is listening on {IP}:{PORT}.")

        while self.running:
            conn, addr = server.accept()
            thread = threading.Thread(target=self.handle_client, args=(conn, addr))
            thread.start()
            print(f"[ACTIVE CONNECTIONS] {threading.active_count() - 1}")

    def handle_client(self, conn, addr):
        print(f"[NEW CONNECTION] {addr} connected.")
        conn.send("OK@Welcome to the Distributed File System's NameNode.".encode(FORMAT))

        while True:
            data = conn.recv(SIZE).decode(FORMAT)
            send_data = "OK@"

            if data.startswith("CREATE_FILE"):
                _, file_path = data.split("@")
                self.create_file(file_path)

            elif data == "LIST_FILES":
                send_data += str(self.list_files())

            elif data.startswith("UPLOAD_FILE"):
                _,file_path = data.split("@")
                self.create_file(file_path)
                ports = ""
                for i,j in self.data_nodes.items():
                    ports += "/"+str(i)
                text = "UPLOAD FILE CREATED" + ports
                send_data += text
            
            elif data.startswith("UPDATE_METADATA"):
                data = data.split("@")
                #print(metadata[1])
                metadata = data[1]
                self.replica_block = data[2]
                both = data[3]
                self.replica_node,file_name = both.split("/")
                metadata = eval(metadata)
                for full_name, data_node_id in metadata.items():
                    self.file_names[file_name]["blocks"].append(full_name)
                    self.file_names[file_name]["data_nodes"].append(data_node_id)
                self.update_meta_file()

            elif data.startswith("DOWNLOAD_FILE"):
                _,file_name = data.split("@")
                final_nodes = []
                final_blocks = []
                ids = []
                for key,value in self.data_nodes.items():
                    ids.append(key) #ids has the active nodes
                #print("ids",ids)
                with open ("meta.txt","r") as f:
                    for line in f:
                        if line.startswith(file_name):
                            _,blocks,nodes,replica_blocks,replica_nodes =  line.split(";")
                            #print("hi",blocks,nodes,replica_blocks,replica_nodes) #dictionaries
                            blocks = eval(blocks.replace("Blocks:","".strip()))
                            nodes = eval(nodes.replace("DataNodes:","".strip()))
                            replica_blocks = eval(replica_blocks.replace("Replicated_Blocks :","".strip()))
                            replica_nodes = eval(replica_nodes.replace("Replicated_Nodes :","".strip()))
                            #print("hi",blocks,nodes,replica_blocks,replica_nodes)
                            for i in range(len(nodes)):
                                if nodes[i] in ids:
                                    final_blocks.append(blocks[i])
                                    final_nodes.append(nodes[i])
                                else:
                                    files = blocks[i]+"r"
                                    for j in range(len(replica_blocks)):
                                        if (replica_blocks[j] == files) and replica_nodes[j] in ids:
                                            final_blocks.append(replica_blocks[j])
                                            final_nodes.append(replica_nodes[j])
                            
                #print(metadata,type(metadata))
                print("data",final_blocks,final_nodes)
                metadata = str(final_blocks)+" "+str(final_nodes)
                send_data += f"DOWNLOAD_METADATA/{metadata}/{file_name}" 

            elif data == "DOWNLOAD_DONE":
                send_data = "OK@"
            elif data == "logout":
                break

            conn.send(send_data.encode(FORMAT))

        print(f"[DISCONNECTED] {addr} disconnected")
        conn.close()

    def create_file(self, file_path):
        file_name = file_path.split("/")
        file_name= file_name[0]
        if file_path not in self.file_system:
            self.file_names[file_name] = {"blocks": [], "data_nodes": [], "replica_blocks":[], "replica_nodes":[]}
            self.update_virtual_tree(file_path)
            self.update_meta_file()
            self.update_tree_file()
            print(f"[NameNode] File {file_name} created in the file system.")
        else:
            print(f"[NameNode] File {file_name} already exists.")

    def update_virtual_tree(self, file_path):
        folders = file_path.split('/')
        current_tree = self.virtual_tree
        for folder in folders[:-1]:
            if folder not in current_tree:
                current_tree[folder] = {}
            current_tree = current_tree[folder]
        current_tree[folders[-1]] = "File"

    def update_meta_file(self):
        with open('meta.txt', 'w') as file:
            for file_name, file_data in self.file_names.items():
                file.write(f"{file_name}; Blocks: {file_data['blocks']}; DataNodes: {file_data['data_nodes']}; Replicated_Blocks : {self.replica_block} ;Replicated_Nodes : {self.replica_node}\n")
        self.replica_block = []
        self.replica_node = []

    def update_tree_file(self):
        with open('tree.txt', 'w') as file:
            self.write_tree_to_file(self.virtual_tree, file)

    def write_tree_to_file(self, tree, file, level=0):
        for key, value in tree.items():
            file.write(f"{' ' * level}{key}\n")
            if isinstance(value, dict):
                self.write_tree_to_file(value, file, level + 2)

    def list_files(self):
        # List all files in the file system
        return list(self.file_system.keys())

    def ping_data_nodes(self):
        while self.running:
            for data_node_id, data_node_info in list(self.data_nodes.items()):
                data_node_ip = data_node_info["ip"]
                data_node_port = data_node_info["port"]

                try:
                    ping_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    ping_socket.settimeout(2)  # Set a timeout for the connection attempt
                    ping_socket.connect((data_node_ip, data_node_port))
                    ping_socket.send("PING".encode(FORMAT))

                    # Receive acknowledgment from the DataNode
                    response = ping_socket.recv(SIZE).decode(FORMAT)
                    ping_socket.close()

                    if response == "PONG":
                        print(f"[NameNode] DataNode {data_node_id} is active.")
                    else:
                        print(f"[NameNode] Invalid response from DataNode {data_node_id}. Discarding.")
                        # Consider reattempting later or marking the DataNode as inactive instead of deleting
                        # del self.data_nodes[data_node_id]

                except (socket.timeout, ConnectionRefusedError):
                    print(f"[NameNode] DataNode {data_node_id} is unreachable. Marking as inactive.")
                    # Consider reattempting later or marking the DataNode as inactive instead of deleting
                    del self.data_nodes[data_node_id]
                    #print("HI",self.data_nodes)

            time.sleep(self.ping_interval)


def main():
    print("[STARTING] Server is starting")
    name_node = NameNode()
    name_node.start()

if __name__ == "__main__":
    main()

           
