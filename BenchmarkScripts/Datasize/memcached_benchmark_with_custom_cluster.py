import subprocess
import time
import socket

START_PORT = 11211
MAX_NODES = 15
BENCHMARK_SCRIPT_PATH = './memcached_custom_sharding.py' 
DATA_SIZES = [1, 1024, 102400, 1048576]  
CLUSTER_CONFIGS = [(1, 30), (3, 10), (5, 6)]

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def start_memcached_instance(port, memory_in_gb):
    command = f"memcached -p {port} -m {memory_in_gb * 1024} -d -u root -I 1000m"
    subprocess.run(command, shell=True)
    print(f"Memcached node started at port {port} with {memory_in_gb} GB")
    time.sleep(5)  

def clear_memcached_instances():
    print("Clearing Memcached instances...")
    for port in range(START_PORT, START_PORT + MAX_NODES):
        if is_port_in_use(port):
            subprocess.run(f"kill $(lsof -t -i:{port})", shell=True) 
            time.sleep(1)
    print("Memcached instances cleared.")

def run_benchmark(cluster_config, data_sizes):
    node_amount, node_size_gb = cluster_config
    for data_size in data_sizes:
        output_file_name = f"memcached_{node_amount}x{node_size_gb}gb_{data_size}bsize.txt"
        command = f"python {BENCHMARK_SCRIPT_PATH} {node_amount} {node_size_gb} {data_size}"
        subprocess.run(command, shell=True)
        print(f"Benchmark completed for config: {node_amount} nodes, {node_size_gb}GB each, {data_size} byte size. Results in {output_file_name}")

def setup_and_run_benchmarks():
    for cluster_config in CLUSTER_CONFIGS:
        clear_memcached_instances()

        node_amount, node_size_gb = cluster_config
        for i in range(node_amount):
            start_memcached_instance(START_PORT + i, node_size_gb)

        run_benchmark(cluster_config, DATA_SIZES)

def main():
    print("Starting Memcached benchmarks...")
    setup_and_run_benchmarks()
    print("All Memcached benchmarks have been completed.")

if __name__ == "__main__":
    main()
