import matplotlib.pyplot as plt
import argparse
import pandas as pd

import numpy as np

from collections import defaultdict
from matplotlib.ticker import StrMethodFormatter

def get_throughput(row):
    messages = min(row['pushes_ok'], row['pops_ok'])
    active_time = (row['duration_ms'] - row['warmup_ms']) / 1000
    throughput = int(messages / active_time)
    return throughput

def get_avg(df, notes, x_str, y_str):
    data = defaultdict(list)
    for index, row in df.iterrows():
        if row['notes'] != notes:
            continue
        x_entry = row[x_str]
        y_entry = row[y_str]
        data[x_entry].append(y_entry)
    x, y = [], []
    for x_entry, y_entries in data.items():
        x.append(x_entry)
        y.append(int(sum(y_entries) / len(y_entries)))
    x, y = zip(*sorted(zip(x, y)))
    return list(x), list(y)

def histogram(df):
    for _, row in df.iterrows():
        if row['notes'] != 'MPMC-vary-threads' or row['producers'] != 4:
            continue
        histogram_str = row['pop_hist_bins']
        histogram = list(map(int, histogram_str.split(";")))

        # trim trailing counts
        while histogram and histogram[-1] < 10000:
            histogram.pop()

        x = [i * 5 for i in range(len(histogram))]

        plt.figure(figsize=(8, 6))
        plt.bar(x, histogram, width=5, align="edge")
        plt.gca().yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
        plt.grid(True, axis="y", linestyle="--", alpha=0.5)
        plt.title("Consumer Latency Histogram at 4p4c")
        plt.xlabel("Latency (ns) [bucket width = 5ns]")
        plt.ylabel("Count")

        plt.tight_layout()
        plt.savefig("docs/fig/pop_hist.png", dpi=300, bbox_inches="tight")
        plt.close()
        return

def scaling_effect(df):
    plt.figure(figsize=(8, 6))
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.title("Latency vs Threads")
    plt.xlabel("Threads (producers = consumers)")
    plt.ylabel("Latency (ns)")
    threads, p50 = get_avg(df, 'MPMC-vary-threads' ,'consumers', 'pop_lat_p50_ns')
    plt.plot(threads, p50, "-o", label="p50")
    threads, p99 = get_avg(df, 'MPMC-vary-threads' ,'consumers', 'pop_lat_p99_ns')
    plt.plot(threads, p99, "-o", label="p99")
    threads, p999 = get_avg(df, 'MPMC-vary-threads' ,'consumers', 'pop_lat_p999_ns')
    plt.plot(threads, p999, "-o", label="p999")
    plt.legend()
    plt.savefig("docs/fig/latency_vs_threads.png", dpi=300, bbox_inches="tight")
    plt.close()

def capacity_effect(df):
    plt.figure(figsize=(8, 6))
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.title("Latency vs Capacity")
    plt.xlabel("Capacity")
    plt.ylabel("Latency (ns)")
    capacity, p50 = get_avg(df, 'MPMC-vary-capacity' ,'capacity', 'pop_lat_p50_ns')
    plt.plot(capacity, p50, "-o", label="p50")
    capacity, p99 = get_avg(df, 'MPMC-vary-capacity' ,'capacity', 'pop_lat_p99_ns')
    plt.plot(capacity, p99, "-o", label="p99")
    capacity, p999 = get_avg(df, 'MPMC-vary-capacity' ,'capacity', 'pop_lat_p999_ns')
    plt.plot(capacity, p999, "-o", label="p999")
    plt.xscale("log")
    plt.legend()
    plt.savefig("docs/fig/latency_vs_capacity.png", dpi=300, bbox_inches="tight")
    plt.close()

def mode_effect(df):
    plt.figure(figsize=(8, 6))
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.title("Blocking vs Non-blocking MPMC")
    plt.xlabel("Threads (producers = consumers)")
    plt.ylabel("Throughput (ops/s)")
    threads, pop_ops_per_sec = get_avg(df, 'MPMC-vary-threads' ,'consumers', 'pop_ops_per_sec')
    threads = threads[1:]
    pop_ops_per_sec = pop_ops_per_sec[1:]
    plt.plot(threads, pop_ops_per_sec, "-o", label="Blocking throughput")
    threads, pop_ops_per_sec = get_avg(df, 'MPMC-nonblocking-vary-threads' ,'consumers', 'pop_ops_per_sec')
    threads = threads[1:]
    pop_ops_per_sec = pop_ops_per_sec[1:]
    plt.plot(threads, pop_ops_per_sec, "-o", label="Non-blocking throughput")
    plt.legend()
    plt.savefig("docs/fig/mode_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()

def pinning_padding_effect(df):
    data = defaultdict(lambda: defaultdict(lambda: {'p50': [], 'p99': [], 'p999': []}))

    for _, row in df.iterrows():
        if row['notes'] != 'MPMC-vary-pinning-padding':
            continue
        data[row['pinning_on']][row['padding_on']]['p50'].append(row['pop_lat_p50_ns'])
        data[row['pinning_on']][row['padding_on']]['p99'].append(row['pop_lat_p99_ns'])
        data[row['pinning_on']][row['padding_on']]['p999'].append(row['pop_lat_p999_ns'])

    label_map = {
        (0, 0): "No pinning, No padding",
        (0, 1): "No pinning, Padding",
        (1, 0): "Pinning, No padding",
        (1, 1): "Pinning, Padding"
    }
    
    plt.figure(figsize=(8, 6))
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.title("Consumer Latency vs Pinning / Padding at 4p4c")
    plt.xlabel("Percentile")
    plt.ylabel("Latency (ns)")

    x = ["p50", "p99", "p999"]

    for pinning_on in [0, 1]:
        for padding_on in [0, 1]:
            vals = data[pinning_on][padding_on]
            y = [np.mean(vals[p]) for p in x]
            plt.plot(x, y, marker='o',
                     label=label_map[(pinning_on, padding_on)])

    plt.legend()
    plt.savefig("docs/fig/latency_vs_pinning_padding.png", dpi=300, bbox_inches="tight")
    plt.close()

def payload_effect(df):
    data = defaultdict(lambda: defaultdict(lambda: {'p50': [], 'p99': [], 'p999': []}))

    for _, row in df.iterrows():
        if row['notes'] != 'MPMC-vary-payload':
            continue
        data[row['large_payload']][row['move_only_payload']]['p50'].append(row['pop_lat_p50_ns'])
        data[row['large_payload']][row['move_only_payload']]['p99'].append(row['pop_lat_p99_ns'])
        data[row['large_payload']][row['move_only_payload']]['p999'].append(row['pop_lat_p999_ns'])

    label_map = {
        (0, 0): "Small payload, Copy",
        (0, 1): "Small payload, Move",
        (1, 0): "Large payload, Copy",
        (1, 1): "Large payload, Move"
    }
    
    plt.figure(figsize=(8, 6))
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.title("Consumer Latency vs Payload Type at 4p4c")
    plt.xlabel("Percentile")
    plt.ylabel("Latency (ns)")

    x = ["p50", "p99", "p999"]

    for large in [0, 1]:
        for move_only in [0, 1]:
            vals = data[large][move_only]
            y = [np.mean(vals[p]) for p in x]
            plt.plot(x, y, marker='o',
                     label=label_map[(large, move_only)])

    plt.legend()
    plt.savefig("docs/fig/latency_vs_payload.png", dpi=300, bbox_inches="tight")
    plt.close()

def main(results):
    # load the CSV file
    df = pd.read_csv(results)

    import os
    os.makedirs("docs/fig", exist_ok=True)

    histogram(df)
    scaling_effect(df)
    capacity_effect(df)
    mode_effect(df)
    pinning_padding_effect(df)
    payload_effect(df)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results")
    args = parser.parse_args()
    results = args.results
    main(results)
