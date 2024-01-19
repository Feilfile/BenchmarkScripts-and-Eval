import threading
import subprocess
import os
import sys

def run_benchmark(port, node_size, data_size, results):
    print(f"Running benchmark on Memcached node at port {port} with data size {data_size} bytes.")
    command = f"memtier_benchmark -p {port} --protocol=memcache_text --test-time=600 --data-size={data_size}"
    process = subprocess.run(command, shell=True, capture_output=True, text=True)
    results[port] = process.stdout

def run_benchmarks_in_parallel(start_port, node_amount, node_size, data_size):
    results = {}
    threads = []

    for i in range(node_amount):
        port = start_port + i
        thread = threading.Thread(target=run_benchmark, args=(port, node_size, data_size, results))
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
    node_size = sys.argv[2]
    data_size = int(sys.argv[3]) 
except IndexError:
    print("Usage: python script.py <nodeAmount> <nodeSize> <dataSize>")
    sys.exit(1)

start_port = 11211
results_dir = "results"
output_file_name = f"memcached_{node_amount}x{node_size}gb_{data_size}bsize.txt"
aggregated_file_path = os.path.join(results_dir, output_file_name)

os.makedirs(results_dir, exist_ok=True)

benchmark_results = run_benchmarks_in_parallel(start_port, node_amount, node_size, data_size)
aggregated_data = aggregate_results(benchmark_results)
save_aggregated_results(aggregated_data, aggregated_file_path)

print(f"Benchmarks completed. Aggregated results are in '{aggregated_file_path}'.")
