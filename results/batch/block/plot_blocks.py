import pandas as pd
import matplotlib.pyplot as plt

def metric_str(metric):
    return ["Create", "Write", "Read"][metric]

def build_df(file):
    file = "avg_seq.csv"

    if not file:
        print("No summary.csv files found.")
        return

    results = {}

    # Load each CSV and extract needed columns
    df = pd.read_csv(file)

    for _, row in df.iterrows():
        block_size = row["block_size"]
        create = row["create"]
        write = row["write"]
        read = row["read"]

        results[block_size] = (create, write, read)
    
    print(results)
    
    return results 

def plot(results, metric):
    block_sizes = sorted(results.keys())

    values = [results[b][metric] for b in block_sizes]

    # Plot
    plt.figure(figsize=(10, 6))
    plt.plot(block_sizes, values, marker="o")

    plt.xlabel("Block Size (bytes)")
    plt.ylabel(metric_str(metric))
    plt.title(f"{metric_str(metric)} Time vs Block Size")
    plt.grid(True)
    plt.xscale("log", base=2)   # optional: block sizes are powers of two

    # Format x ticks nicely as KB, MB, etc.
    plt.xticks(block_sizes, [format_bytes(b) for b in block_sizes], rotation=0)
    
    # plt.legend()
    plt.tight_layout()
    # plt.show()
    plt.savefig(metric_str(metric) + "_seq.png")


def format_bytes(x):
    if x >= 1024*1024:
        return f"{x // (1024*1024)} MB"
    if x >= 1024:
        return f"{x // 1024} KB"
    return f"{x} B"


if __name__ == "__main__":
    results = build_df("summary.csv")
    plot(results, 0)
    plot(results, 1)
    plot(results, 2)
