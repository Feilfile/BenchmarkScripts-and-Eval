import subprocess
import time
import os
import glob
import socket

REDIS_SERVER_PATH = 'redis-server'
REDIS_CLI_PATH = 'redis-cli'
MEMTIER_BENCHMARK_PATH = 'memtier_benchmark'

START_PORT = 7000
MAX_NODES = 15
BENCHMARK_LENGTH = 600
DATA_SIZES = [1, 1024, 102400, 1048576]
SINGLE_NODE_CONFIGS = [30]
CLUSTER_CONFIGS = [(3, 10), (5, 6)]

def is_port_in_use(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def clear_redis_instances():
    print("Clearing Redis instances...")
    for port in range(START_PORT, START_PORT + MAX_NODES):
        if is_port_in_use(port):
            subprocess.run([REDIS_CLI_PATH, "-p", str(port), "shutdown", "nosave"])
            time.sleep(1)

        files_to_remove = glob.glob(f"redis_{port}*.conf") + glob.glob(f"nodes-{port}.conf") + glob.glob(f"dump.rdb") + glob.glob("appendonly.aof")
        for file in files_to_remove:
            try:
                os.remove(file)
                print(f"Removed file: {file}")
            except OSError as e:
                print(f"Error removing file {file}: {e}")

def start_redis_instance(port, memory_in_gb, cluster_enabled=False):
    config_file = f"redis_{port}.conf"
    config_content = f"""
    port {port}
    cluster-enabled {'yes' if cluster_enabled else 'no'}
    cluster-config-file nodes-{port}.conf
    cluster-node-timeout 5000
    appendonly no
    maxmemory {memory_in_gb * 1024}mb
    maxmemory-policy allkeys-lru
    """
    with open(config_file, 'w') as file:
        file.write(config_content.strip())

    process = subprocess.Popen([REDIS_SERVER_PATH, config_file], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"Redis node created at port {port}")
    time.sleep(5)
    return process

def create_cluster(node_ports):
    node_addresses = ' '.join([f"127.0.0.1:{port}" for port in node_ports])
    print("Creating Redis cluster with nodes:", node_addresses)
    
    cluster_create_command = f"{REDIS_CLI_PATH} --cluster create {node_addresses} --cluster-replicas 0"
    process = subprocess.Popen(cluster_create_command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    output, error = process.communicate(input="yes\n")
    print("Cluster creation output:", output)
    print("Cluster creation error (if any):", error)
    print("Cluster creation command executed.")
    
    process.terminate()

    check_command = f"{REDIS_CLI_PATH} --cluster check {node_addresses}"
    check_process = subprocess.Popen(check_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    check_output, check_error = check_process.communicate()
    print("Cluster check output:", check_output)
    print("Cluster check error (if any):", check_error)

    check_process.terminate()
    time.sleep(15)

def run_benchmark(port, data_size, test_time_seconds, is_cluster, total_memory, node_count, memory_per_node):
    output_file = f"./results/redis_{node_count}x{memory_per_node}gb_{data_size}datasize.txt"
    cmd = [MEMTIER_BENCHMARK_PATH, "-p", str(port), "--test-time", str(BENCHMARK_LENGTH), "--data-size", str(data_size)]
    if is_cluster:
        cmd.append("--cluster-mode")

    print("Executing command:", ' '.join(cmd))

    with open(output_file, 'w') as file:
        subprocess.run(cmd, stdout=file, stderr=subprocess.STDOUT)
    print(f"Benchmark results written to {output_file}")

def setup_and_run_benchmarks(memory_configs, cluster=False):
    redis_processes = []

    for config in memory_configs:
        clear_redis_instances()

        if cluster:
            node_count, memory_per_node = config
            total_memory = node_count * memory_per_node
            node_ports = [START_PORT + i for i in range(node_count)]
            for port in node_ports:
                process = start_redis_instance(port, memory_per_node, cluster_enabled=True)
                redis_processes.append(process)
            create_cluster(node_ports)
        else:
            node_count = 1
            memory_per_node = config
            total_memory = memory_per_node
            process = start_redis_instance(START_PORT, memory_per_node)
            redis_processes.append(process)

        for data_size in DATA_SIZES:
            run_benchmark(START_PORT, data_size, int(BENCHMARK_LENGTH), cluster, total_memory, node_count, memory_per_node)

    for process in redis_processes:
        process.terminate()

def main():
    print("Starting non-cluster benchmarks...")

    print("Starting cluster benchmarks...")
    setup_and_run_benchmarks(CLUSTER_CONFIGS, cluster=True)

    print("All benchmarks have been completed.")

if __name__ == "__main__":
    main()
