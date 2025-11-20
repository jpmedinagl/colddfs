import os
import re
import pandas as pd
import matplotlib.pyplot as plt

# ---------------------------------------
# CONFIG
# ---------------------------------------

RESULTS_DIR = "./"     # path to your CSV folder
METRIC_TO_PLOT = "avg_write_latency_ms"   # choose any column!
# EXAMPLES:
# "avg_write_latency_ms", "load_imbalance", "load_std_dev",
# "blocks_used", "metadata_bytes", "write_count", etc.

# ---------------------------------------
# Load all CSVs and build nested dict
# ---------------------------------------

pattern = re.compile(
    r"results_fill_(\w+)_web_realistic_(\d+)nodes\.csv"
)

data = {}  # final dict: data[nodes][policy] = DataFrame

for fname in os.listdir(RESULTS_DIR):
    match = pattern.match(fname)
    if not match:
        continue

    policy = match.group(1)
    nodes = int(match.group(2))

    filepath = os.path.join(RESULTS_DIR, fname)
    df = pd.read_csv(filepath)

    if nodes not in data:
        data[nodes] = {}

    data[nodes][policy] = df

print("Loaded datasets:")
for nodes, policies in data.items():
    print(f"  {nodes} nodes: {list(policies.keys())}")

# ---------------------------------------
# Plot: fill_pct (x) vs chosen metric (y)
# For all node counts + all policies
# ---------------------------------------

def plot_metric(metric):
    plt.figure(figsize=(12, 8))

    for nodes in sorted(data.keys()):
        if nodes in [8]:
            for policy, df in data[nodes].items():
                plt.plot(
                    df["fill_pct"],
                    df[metric],
                    marker="o",
                    label=f"{policy} ({nodes} nodes)"
                )

    plt.xlabel("Fill Percentage (%)")
    plt.ylabel(metric.replace("_", " ").title())
    plt.title(f"{metric.replace('_',' ').title()} vs Fill Level")
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True)
    plt.tight_layout()
    plt.show()


# ---------------------------------------
# Run the plot
# ---------------------------------------

plot_metric(METRIC_TO_PLOT)
