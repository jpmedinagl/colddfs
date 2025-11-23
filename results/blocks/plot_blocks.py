import pandas as pd
import matplotlib.pyplot as plt
import argparse
import glob
import os

def plot(metric):
    # Find all block_*.csv files
    files = sorted(glob.glob("block_*.csv"))

    if not files:
        print("No block_*.csv files found.")
        return

    results = {}

    # Load each CSV and extract needed columns
    for f in files:
        df = pd.read_csv(f)

        for _, row in df.iterrows():
            policy = row["policy"]
            block_size = row["block_size_bytes"]
            value = row[metric]

            if policy not in results:
                results[policy] = {}

            results[policy][block_size] = value
    
    print(results)

    # Plot
    plt.figure(figsize=(10, 6))

    for policy, data in results.items():
        block_sizes = sorted(data.keys())
        values = [data[b] for b in block_sizes]

        plt.plot(block_sizes, values, marker="o", label=policy)

    plt.xlabel("Block Size (bytes)")
    plt.ylabel(metric.replace("_", " ").title())
    plt.title(f"{metric.replace('_',' ').title()} vs Block Size")
    plt.grid(True)
    plt.xscale("log", base=2)   # optional: block sizes are powers of two

    # Format x ticks nicely as KB, MB, etc.
    plt.xticks(block_sizes, [format_bytes(b) for b in block_sizes], rotation=45)
    
    plt.legend()
    plt.tight_layout()
    # plt.show()
    plt.savefig(metric + ".png")


def format_bytes(x):
    if x >= 1024*1024:
        return f"{x // (1024*1024)} MB"
    if x >= 1024:
        return f"{x // 1024} KB"
    return f"{x} B"


if __name__ == "__main__":
    plot("write_throughput_mbps")
    # plot("total_capacity_mb")
    # plot("total_blocks")
    plot("files_created")
    plot("avg_write_latency_ms")
    plot("avg_read_latency_ms")
