import threading
import subprocess
import os
import sys

def run_single_benchmark(start_port, total_clients, node_size, results):
    print(f"Running single node benchmark with {total_clients} concurrent clients.")
    command = f"memtier_benchmark -p {start_port} --protocol=memcache_text -c {total_clients} -t 1 --ratio=1:10 --test-time=600"
    process = subprocess.run(command, shell=True, capture_output=True, text=True)
    results[start_port] = process.stdout
    print(process.stdout) 

def run_benchmark(port, clients, node_size, results):
    print(f"Memcached node {port - start_port + 1} with a memory of {node_size} is benchmarked with {clients} concurrent clients.")
    command = f"memtier_benchmark -p {port} --protocol=memcache_text -c {clients} -t 1 --ratio=1:10 --test-time=600"
    process = subprocess.run(command, shell=True, capture_output=True, text=True)
    results[port] = process.stdout

def run_benchmarks_in_parallel(start_port, node_amount, total_clients, node_size):
    results = {}

    if node_amount == 1:
        run_single_benchmark(start_port, total_clients, node_size, results)
        return results

    threads = []
    base_clients_per_node = total_clients // node_amount
    extra_clients = total_clients % node_amount

    for i in range(node_amount):
        port = start_port + i
        clients = base_clients_per_node + (1 if i < extra_clients else 0)
        thread = threading.Thread(target=run_benchmark, args=(port, clients, node_size, results))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    return results

def aggregate_results(results):
    aggregated_data = ""
    for port, result in results.items():
        aggregated_data += f"Results for Memcached on port {port}:\n"
        aggregated_data += result + "\n\n"
    return aggregated_data

def save_aggregated_results(data, file_path):
    with open(file_path, 'w') as file:
        file.write(data)

try:
    node_amount = int(sys.argv[1])
    cclients = int(sys.argv[2])
    node_size = sys.argv[3]
except IndexError:
    print("Usage: python script.py <nodeAmount> <cclients> <nodeSize>")
    sys.exit(1)

start_port = 11211
results_dir = "results"
output_file_name = f"memcached_{node_amount}x{node_size}gb_{cclients}cclients.txt"
aggregated_file_path = os.path.join(results_dir, output_file_name)

os.makedirs(results_dir, exist_ok=True)

benchmark_results = run_benchmarks_in_parallel(start_port, node_amount, cclients, node_size)
aggregated_data = aggregate_results(benchmark_results)
save_aggregated_results(aggregated_data, aggregated_file_path)

print(f"Benchmarks completed. Aggregated results are in '{aggregated_file_path}'.")
