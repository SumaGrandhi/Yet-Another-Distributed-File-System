import socket
import json

IP = "172.20.10.4"  # Update to the appropriate NameNode IP address
PORT = 4456
ADDR = (IP, PORT)
FORMAT = "utf-8"
SIZE = 1024
data_node_ip ="172.20.10.4"

def upload_file_to_datanodes(client,file_name, datanode_ports):
    metadata = {}
    replicated_blocks = []
    replicated_nodes = []
    # Read the file content
    with open(file_name, 'r') as f:
        content = f.readlines()

    # Split the content into blocks of 2 lines each
    blocks = [content[i:i+2] for i in range(0, len(content), 2)]
    blocks = [''.join(block) for block in blocks]  # Join the two lines into a single string for each block
    
    # Upload each block to the corresponding DataNode
    for i, block in enumerate(blocks):
        datanode_port = int(datanode_ports[i % len(datanode_ports)])   # Adjusted for zero-based indexing
 
        # Connect to the DataNode and send the block
        datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        datanode_socket.connect((data_node_ip, datanode_port))
        ports = json.dumps({"ports": datanode_ports})
        datanode_socket.send(f"STORE_BLOCK@{file_name}@{i}@{block}@{ports}".encode(FORMAT))
        metadata[file_name+str(i)] = datanode_port-5000
        response = datanode_socket.recv(SIZE).decode(FORMAT)
        if "REPLICATED_METADATA" in response:
            _, json_blocks, json_nodes = response.split("@")
            replicated_blocks.extend(json.loads(json_blocks))
            replicated_nodes.extend(json.loads(json_nodes))
    #print("final",replicated_blocks, replicated_nodes)
    for i in datanode_ports:
        datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        datanode_socket.connect((data_node_ip, i))
        datanode_socket.send(f"Upload complete".encode(FORMAT))
        response = datanode_socket.recv(SIZE).decode(FORMAT)
        datanode_socket.close()
    if response == "OK@":
        metadata = json.dumps(metadata)
        client.send(f"UPDATE_METADATA@{metadata}@{replicated_blocks}@{replicated_nodes}/{file_name}".encode(FORMAT))
    
            

def main():
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)

    while True:
        data = client.recv(SIZE).decode(FORMAT)
        cmd, msg = data.split("@")

        if cmd == "DISCONNECTED":
            print(f"[SERVER]: {msg}")
            break
        elif cmd == "OK":
            print(f"{msg}")

        command = input("> ")

        if command == "logout":
            client.send(command.encode(FORMAT))
            break

        elif command.startswith("create file"):
            file_path = command.replace("create file", "").strip()
            client.send(f"CREATE_FILE@{file_path}".encode(FORMAT))

        elif command.startswith("create directory"):
            directory_path = command.replace("create directory", "").strip()
            client.send(f"CREATE_DIRECTORY@{directory_path}".encode(FORMAT))

        elif command.startswith("upload file"):
            file_path = command.replace("upload file", "").strip()
            file_name = file_path.split("/")
            #print("File path",file_name)
            file_name = file_name[0]
            #file_path = file_name[1]
            client.send(f"UPLOAD_FILE@{file_path}".encode(FORMAT)) #passes the entire file path
            command = client.recv(SIZE).decode(FORMAT)
            #print("C",command)
            parts = command.split("/")
            alive_datanodes = len(parts)-1
            datanode_ports=[]
            for i in range(1,len(parts)):
                datanode_ports.append(5000+int(parts[i]))
            #print("DP",datanode_ports)
            upload_file_to_datanodes(client,file_name,datanode_ports)

        elif command.startswith("download file"):
            file_name = command.replace("download file", "").strip()
            client.send(f"DOWNLOAD_FILE@{file_name}".encode(FORMAT)) #passes the entire file path
            command = client.recv(SIZE).decode(FORMAT)
            #print("COM",command)
            _, data = command.split("@")
            _,metadata,file_name = data.split("/")
            metadata = metadata.rstrip(" ")
            blocks,nodes,_ = metadata.split("]")
            blocks += "]"
            nodes += "]"
            blocks = blocks.replace("Blocks:","".strip())
            nodes = nodes.replace(", DataNodes:","".strip())
            blocks = eval(blocks)
            nodes = eval(nodes)
            file = open(file_name,"w")
            for i in range (0,len(nodes)):
                port = 5000+nodes[i]
                datanode_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                datanode_socket.connect((data_node_ip, port))
                datanode_socket.send(f"DOWNLOAD@{blocks[i]}".encode(FORMAT))
                response = datanode_socket.recv(SIZE).decode(FORMAT)
                _,content = response.split("@")
                file.write(content)
                datanode_socket.close()
            file.close()
            client.send(f"DOWNLOAD_DONE".encode(FORMAT))

        elif command == "list files":
            client.send("LIST_FILES".encode(FORMAT))


    print("Disconnected from the server.")
    client.close()

if __name__ == "__main__":
    main()
