import argparse
import os

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from collections import defaultdict
from matplotlib.ticker import StrMethodFormatter

# Group rows by x_str where notes match, return averages of y_str
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
    if not x:
        return [], []
    x, y = zip(*sorted(zip(x, y)))
    return list(x), list(y)

# Trim trailing low-count bins for plotting
def trim_histogram(histogram, r=0.005, pad=2):
    if not histogram:
        return histogram
    peak = max(histogram)
    last_idx = 0
    for i in range(len(histogram) - 1, -1, -1):
        if histogram[i] >= peak * r:
            last_idx = i
            break
    return histogram[:min(last_idx + pad + 1, len(histogram))]

# Expects notes == 'MPMC-vary-threads' and producers == 4
def histogram(df):
    for _, row in df.iterrows():
        if row['notes'] != 'MPMC-vary-threads' or row['producers'] != 4:
            continue
        histogram_str = row['pop_hist_bins']
        raw_histogram = list(map(int, histogram_str.split(";")))

        # trim for plotting
        h = trim_histogram(raw_histogram)
        x = [i * 5 for i in range(len(h))]
        y = [v / 1e6 for v in h]

        plt.figure(figsize=(8, 6))
        plt.bar(x, y, width=5, align="edge")
        plt.gca().yaxis.set_major_formatter(StrMethodFormatter("{x:,.0f}"))
        plt.grid(True, axis="y", linestyle="--", alpha=0.5)
        plt.title("Consumer Latency Histogram at 4p4c")
        plt.xlabel("Latency (ns) [bucket width = 5ns]")
        plt.ylabel("Count (millions)")

        for p, color in zip(
            ["pop_lat_p50_ns", "pop_lat_p99_ns", "pop_lat_p999_ns"],
            ["green", "orange", "red"]
        ):
            label = p.replace("pop_lat_", "").replace("_ns", "")
            plt.axvline(row[p], color=color, linestyle="--", linewidth=1.5, label=label)
        plt.legend()

        plt.tight_layout()
        plt.savefig("docs/fig/pop_hist.png", dpi=300, bbox_inches="tight")
        plt.close()
        return

# Expects notes == 'MPMC-vary-threads'
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
    plt.figtext(0.5, -0.05,
            "As concurrency rises, p50, p99, and p999 grow.",
            ha="center", fontsize=9)
    plt.savefig("docs/fig/latency_vs_threads.png", dpi=300, bbox_inches="tight")
    plt.close()

# Expects notes == 'MPMC-vary-capacity'
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

# Expects notes == 'MPMC-vary-threads' and 'MPMC-nonblocking-vary-threads'
def mode_effect(df):
    plt.figure(figsize=(8, 6))
    plt.grid(True, axis="y", linestyle="--", alpha=0.5)
    plt.title("Blocking vs Non-blocking Throughput (MPMC)")
    plt.xlabel("Threads (producers = consumers)")
    plt.ylabel("Throughput (ops/s)")

    # blocking
    threads, pop_ops_per_sec = get_avg(df, 'MPMC-vary-threads', 'consumers', 'pop_ops_per_sec')
    threads = threads[1:]
    pop_ops_per_sec = [v/1e6 for v in pop_ops_per_sec[1:]]  # scale to millions
    plt.plot(threads, pop_ops_per_sec, "-o", label="Blocking throughput")

    # non-blocking
    threads, pop_ops_per_sec = get_avg(df, 'MPMC-nonblocking-vary-threads', 'consumers', 'pop_ops_per_sec')
    threads = threads[1:]
    pop_ops_per_sec = [v/1e6 for v in pop_ops_per_sec[1:]]  # scale to millions
    plt.plot(threads, pop_ops_per_sec, "-o", label="Non-blocking throughput")

    plt.gca().yaxis.set_major_formatter(StrMethodFormatter("{x:.1f}M"))
    plt.legend()
    plt.figtext(
        0.5, -0.05,
        "Blocking calls outperform non-blocking try_* by 2-3x while tightening tail latencies.",
        ha="center", fontsize=9
    )
    plt.savefig("docs/fig/mode_comparison.png", dpi=300, bbox_inches="tight")
    plt.close()

# Expects notes == 'MPMC-vary-pinning-padding'
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
            if all(len(vals[p]) > 0 for p in x):
                y = [np.mean(vals[p]) for p in x]
                plt.plot(x, y, marker='o',
                         label=label_map[(pinning_on, padding_on)])

    plt.legend()
    plt.figtext(0.5, -0.05,
            "Pinning and cache-line padding reduce p999 by 15-45% at 4p4c.",
            ha="center", fontsize=9)
    plt.savefig("docs/fig/latency_vs_pinning_padding.png", dpi=300, bbox_inches="tight")
    plt.close()

# Expects notes == 'MPMC-vary-payload'
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
            if all(len(vals[p]) > 0 for p in x):
                y = [np.mean(vals[p]) for p in x]
                plt.plot(x, y, marker='o',
                         label=label_map[(large, move_only)])

    plt.legend()
    plt.figtext(0.5, -0.05,
            "Large copyable payloads pay for data movement; move-only payloads keep latency close to small POD.",
            ha="center", fontsize=9)
    plt.savefig("docs/fig/latency_vs_payload.png", dpi=300, bbox_inches="tight")
    plt.close()

def main(results):
    # load the CSV file
    df = pd.read_csv(results)

    os.makedirs("docs/fig", exist_ok=True)

    histogram(df)
    scaling_effect(df)
    capacity_effect(df)
    mode_effect(df)
    pinning_padding_effect(df)
    payload_effect(df)

    plt.close('all')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--results")
    args = parser.parse_args()
    results = args.results
    main(results)
