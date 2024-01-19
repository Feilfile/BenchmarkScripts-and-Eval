import subprocess
import time

START_PORT = 6379
MAX_NODES = 15
BENCHMARK_SCRIPT_PATH = './redis_custom_sharding.py' 
DATA_SIZES = [1, 1024, 102400, 1048576]  
CLUSTER_CONFIGS = [(1, 30), (3, 10), (5, 6)]

def start_redis_instance(port, memory_in_gb):
    config_file = f"redis_{port}.conf"
    with open(config_file, 'w') as file:
        file.write(f"port {port}\nmaxmemory {memory_in_gb * 1024}mb\nmaxmemory-policy allkeys-lru\n")
    subprocess.Popen(["redis-server", config_file], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def clear_redis_instances():
    print("Clearing Redis instances...")
    for port in range(START_PORT, START_PORT + MAX_NODES):
        subprocess.run(["redis-cli", "-p", str(port), "shutdown"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(1)
    print("Redis instances cleared.")

def run_benchmark(cluster_config, data_sizes):
    node_amount, node_size_gb = cluster_config
    for data_size in data_sizes:
        output_file_name = f"redis_{node_amount}x{node_size_gb}gb_{data_size}bsize.txt"
        command = f"python {BENCHMARK_SCRIPT_PATH} {node_amount} {node_size_gb} {data_size}"
        subprocess.run(command, shell=True)
        print(f"Benchmark completed for config: {node_amount} nodes, {node_size_gb}GB each, {data_size} byte size. Results in {output_file_name}")

def setup_and_run_benchmarks():
    for cluster_config in CLUSTER_CONFIGS:
        clear_redis_instances()
        node_amount, node_size_gb = cluster_config
        for i in range(node_amount):
            start_redis_instance(START_PORT + i, node_size_gb)
        run_benchmark(cluster_config, DATA_SIZES)

def main():
    print("Starting Redis benchmarks...")
    setup_and_run_benchmarks()
    print("All Redis benchmarks have been completed.")

if __name__ == "__main__":
    main()
