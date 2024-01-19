import os
import re
import csv

BENCHMARK_DIR = "./results"  # Update this path
CSV_DIR = "./csvs"
os.makedirs(CSV_DIR, exist_ok=True)

SETS_CSV = os.path.join(CSV_DIR, "sets.csv")
GETS_CSV = os.path.join(CSV_DIR, "gets.csv")
TOTALS_CSV = os.path.join(CSV_DIR, "totals.csv")

headers = ["Benchmark Method", "Amount of Nodes", "Size per Node", "Amount of CClients", 
           "Ops/sec", "Hits/sec", "Misses/sec", "Avg. Latency", "p50 Latency", "p99 Latency", 
           "p99.9 Latency", "KB/sec"]

def parse_filename(file_name):
    match = re.match(r"(\w+)_(\d+)x(\d+)gb_(\d+)cclients", file_name)
    if match:
        return match.groups()
    return None, None, None, None

def process_file(file_path):
    with open(file_path, 'r') as file:
        file_content = file.read()

        data_pattern = r"(Sets|Gets|Totals)[\s\S]+?([\d.]+)\s+(---|[\d.]+)\s+(---|[\d.]+)[\s\S]+?([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)"

        matches = re.findall(data_pattern, file_content)
        processed_data = {"Sets": [], "Gets": [], "Totals": []}

        for match in matches:
            key, values = match[0], match[1:]
            processed_values = [float(v) if v != "---" else 0 for v in values]
            processed_data[key].append(processed_values)

        return processed_data

def aggregate_data(data_list):
    aggregated_data = [0] * 8
    for data in data_list:
        for i in range(8):
            aggregated_data[i] += data[i]

    return aggregated_data

def write_to_csv(file_path, data, benchmark_info):
    with open(file_path, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(list(benchmark_info) + data)

for file_path in [SETS_CSV, GETS_CSV, TOTALS_CSV]:
    with open(file_path, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

for file_name in os.listdir(BENCHMARK_DIR):
    if file_name.endswith(".txt"):
        benchmark_info = parse_filename(file_name)
        file_path = os.path.join(BENCHMARK_DIR, file_name)

        extracted_data = process_file(file_path)
        for key in ["Sets", "Gets", "Totals"]:
            aggregated_results = aggregate_data(extracted_data[key])
            write_to_csv(os.path.join(CSV_DIR, key.lower() + ".csv"), aggregated_results, benchmark_info)

print("Benchmark results saved to CSV files.")
