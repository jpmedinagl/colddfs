import os
import re
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Configuration
results_dir = "."

# Create output directory for plots
os.makedirs("plots", exist_ok=True)

# ============================================================================
# WORKLOAD 1: Fill Levels
# ============================================================================

def plot_fill_workload():
    """Plot metrics vs fill level for all policies"""

    fill_dir = os.path.join(results_dir, "fill")
    if not os.path.exists(fill_dir):
        print("No fill/ directory found")
        return
    
    pattern = re.compile(r"results_fill_(\w+)_web_realistic_(\d+)nodes\.csv")
    
    data = {}  # data[nodes][policy] = dataframe
    
    for fname in os.listdir(fill_dir):
        match = pattern.match(fname)
        if not match:
            continue
        
        policy = match.group(1)
        nodes = int(match.group(2))
        
        filepath = os.path.join(fill_dir, fname)
        df = pd.read_csv(filepath)
        
        if nodes not in data:
            data[nodes] = {}
        
        data[nodes][policy] = df
    
    if not data:
        print("No fill workload data found")
        return
    
    # Plot different metrics
    metrics = [
        ("avg_write_latency_ms", "Average Write Latency (ms)"),
        ("load_imbalance", "Load Imbalance (CoV)"),
        ("load_std_dev", "Load Std Dev"),
    ]
    
    for metric, ylabel in metrics:
        plt.figure(figsize=(12, 8))
        
        for nodes in sorted(data.keys()):
            for policy, df in data[nodes].items():
                if metric in df.columns:
                    plt.plot(
                        df["fill_pct"],
                        df[metric],
                        marker="o",
                        label=f"{policy} ({nodes} nodes)"
                    )
        
        plt.xlabel("Fill Percentage (%)", fontsize=12)
        plt.ylabel(ylabel, fontsize=12)
        plt.title(f"{ylabel} vs Fill Level", fontsize=14, fontweight='bold')
        plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        filename = f"plots/fill_{metric}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"Saved {filename}")
        plt.close()

# ============================================================================
# WORKLOAD 2: Mixed Operations
# ============================================================================

def plot_mixed_workload():
    """Plot mixed operation workload results"""

    mixed_dir = os.path.join(results_dir, "mixed")
    if not os.path.exists(mixed_dir):
        print("No mixed/ directory found")
        return
    
    pattern = re.compile(r"results_mixed_(\w+)_web_realistic_(\d+)nodes\.csv")
    
    data = {}
    
    for fname in os.listdir(mixed_dir):
        match = pattern.match(fname)
        if not match:
            continue
        
        policy = match.group(1)
        nodes = int(match.group(2))
        
        filepath = os.path.join(mixed_dir, fname)
        df = pd.read_csv(filepath)
        
        if nodes not in data:
            data[nodes] = {}
        
        data[nodes][policy] = df
    
    if not data:
        print("No mixed workload data found")
        return
    
    # Plot write vs read latency
    plt.figure(figsize=(12, 8))
    
    for nodes in sorted(data.keys()):
        for policy, df in data[nodes].items():
            if "avg_write_latency_ms" in df.columns and "avg_read_latency_ms" in df.columns:
                plt.plot(
                    df["fill_pct"],
                    df["avg_write_latency_ms"],
                    marker="o",
                    linestyle="-",
                    label=f"{policy} Write ({nodes} nodes)"
                )
                plt.plot(
                    df["fill_pct"],
                    df["avg_read_latency_ms"],
                    marker="s",
                    linestyle="--",
                    label=f"{policy} Read ({nodes} nodes)"
                )
    
    plt.xlabel("Fill Percentage (%)", fontsize=12)
    plt.ylabel("Latency (ms)", fontsize=12)
    plt.title("Read vs Write Latency (Mixed Workload)", fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    filename = "plots/mixed_latency.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()

# ============================================================================
# WORKLOAD 3: Read-Heavy
# ============================================================================

def plot_readheavy_workload():
    """Plot read-heavy workload results"""

    readheavy_dir = os.path.join(results_dir, "readheavy")
    if not os.path.exists(readheavy_dir):
        print("No readheavy/ directory found")
        return
    
    pattern = re.compile(r"results_readheavy_(\w+)_web_realistic_(\d+)nodes\.csv")
    
    data = {}
    
    for fname in os.listdir(readheavy_dir):
        match = pattern.match(fname)
        if not match:
            continue
        
        policy = match.group(1)
        nodes = int(match.group(2))
        
        filepath = os.path.join(readheavy_dir, fname)
        df = pd.read_csv(filepath)
        
        if nodes not in data:
            data[nodes] = {}
        
        data[nodes][policy] = df
    
    if not data:
        print("No read-heavy workload data found")
        return
    
    # Plot read latency over time
    plt.figure(figsize=(12, 8))
    
    for nodes in sorted(data.keys()):
        for policy, df in data[nodes].items():
            if "avg_read_latency_ms" in df.columns:
                x = range(len(df))
                plt.plot(
                    x,
                    df["avg_read_latency_ms"],
                    marker="o",
                    label=f"{policy} ({nodes} nodes)"
                )
    
    plt.xlabel("Operation Batch", fontsize=12)
    plt.ylabel("Average Read Latency (ms)", fontsize=12)
    plt.title("Read Latency Over Time (Read-Heavy Workload)", fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    filename = "plots/readheavy_latency.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()

# ============================================================================
# WORKLOAD 4: Imbalance Test
# ============================================================================

def plot_imbalance_workload():
    """Plot imbalance creation and recovery"""

    imbalance_dir = os.path.join(results_dir, "imbalance")
    if not os.path.exists(imbalance_dir):
        print("No imbalance/ directory found")
        return
    
    pattern = re.compile(r"results_imbalance_(\w+)_web_realistic_(\d+)nodes\.csv")
    
    data = {}
    
    for fname in os.listdir(imbalance_dir):
        match = pattern.match(fname)
        if not match:
            continue
        
        policy = match.group(1)
        nodes = int(match.group(2))
        
        filepath = os.path.join(imbalance_dir, fname)
        df = pd.read_csv(filepath)
        
        if nodes not in data:
            data[nodes] = {}
        
        data[nodes][policy] = df
    
    if not data:
        print("No imbalance workload data found")
        return
    
    # Plot load imbalance over phases
    plt.figure(figsize=(12, 8))
    
    for nodes in sorted(data.keys()):
        for policy, df in data[nodes].items():
            if "load_imbalance" in df.columns:
                x = range(len(df))
                plt.plot(
                    x,
                    df["load_imbalance"],
                    marker="o",
                    label=f"{policy} ({nodes} nodes)"
                )
    
    plt.xlabel("Phase", fontsize=12)
    plt.ylabel("Load Imbalance (CoV)", fontsize=12)
    plt.title("Load Imbalance Evolution (Imbalance Test)", fontsize=14, fontweight='bold')
    plt.legend(bbox_to_anchor=(1.05, 1), loc="upper left")
    plt.grid(True, alpha=0.3)
    
    # Add phase markers
    if len(data) > 0:
        sample_df = list(list(data.values())[0].values())[0]
        if len(sample_df) >= 3:
            plt.axvline(x=1, color='red', linestyle='--', alpha=0.5, label='Truncation Start')
            plt.axvline(x=2, color='green', linestyle='--', alpha=0.5, label='New Allocations Start')
    
    plt.tight_layout()
    
    filename = "plots/imbalance_evolution.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()

# ============================================================================
# WORKLOAD 5: Node Scaling (RQ3)
# ============================================================================

def plot_node_scaling():
    """Plot scaling metrics vs number of nodes"""
    
    summary_file = "results_scaling_summary.csv"
    
    if not os.path.exists(summary_file):
        print("No scaling summary data found")
        return
    
    df = pd.read_csv(summary_file)
    
    policies = df['policy'].unique()
    
    # Plot 1: Throughput vs Nodes
    plt.figure(figsize=(10, 6))
    
    for policy in policies:
        policy_data = df[df['policy'] == policy]
        plt.plot(
            policy_data['num_nodes'],
            policy_data['throughput_mbps'],
            marker='o',
            label=policy,
            linewidth=2
        )
    
    plt.xlabel("Number of Data Nodes", fontsize=12)
    plt.ylabel("Throughput (MB/s)", fontsize=12)
    plt.title("Throughput vs Number of Nodes (RQ3)", fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    filename = "plots/scaling_throughput.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()
    
    # Plot 2: Load Imbalance vs Nodes
    plt.figure(figsize=(10, 6))
    
    for policy in policies:
        policy_data = df[df['policy'] == policy]
        plt.plot(
            policy_data['num_nodes'],
            policy_data['load_imbalance'],
            marker='o',
            label=policy,
            linewidth=2
        )
    
    plt.xlabel("Number of Data Nodes", fontsize=12)
    plt.ylabel("Load Imbalance (CoV)", fontsize=12)
    plt.title("Load Imbalance vs Number of Nodes (RQ3)", fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    filename = "plots/scaling_imbalance.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()
    
    # Plot 3: Average Latency vs Nodes
    plt.figure(figsize=(10, 6))
    
    for policy in policies:
        policy_data = df[df['policy'] == policy]
        plt.plot(
            policy_data['num_nodes'],
            policy_data['avg_write_latency_ms'],
            marker='o',
            label=policy,
            linewidth=2
        )
    
    plt.xlabel("Number of Data Nodes", fontsize=12)
    plt.ylabel("Average Write Latency (ms)", fontsize=12)
    plt.title("Latency vs Number of Nodes (RQ3)", fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    
    filename = "plots/scaling_latency.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()

# ============================================================================
# Summary Comparison
# ============================================================================

def plot_policy_comparison():
    """Create bar charts comparing policies"""

    fill_dir = os.path.join(results_dir, "fill")
    if not os.path.exists(fill_dir):
        print("No fill/ directory for comparison")
        return
    
    # Look for any available data
    pattern = re.compile(r"results_fill_(\w+)_web_realistic_8nodes\.csv")
    
    policies = []
    final_metrics = []
    
    for fname in os.listdir(fill_dir):
        match = pattern.match(fname)
        if not match:
            continue
        
        policy = match.group(1)
        filepath = os.path.join(fill_dir, fname)
        df = pd.read_csv(filepath)
        
        # Get final metrics (highest fill level)
        if len(df) > 0:
            final = df.iloc[-1]
            policies.append(policy)
            final_metrics.append(final)
    
    if not policies:
        print("No data for policy comparison")
        return
    
    # Create comparison bar chart
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    
    # Plot 1: Average Write Latency
    ax = axes[0, 0]
    latencies = [m['avg_write_latency_ms'] for m in final_metrics]
    ax.bar(policies, latencies, color='steelblue')
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Average Write Latency")
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Plot 2: Load Imbalance
    ax = axes[0, 1]
    imbalances = [m['load_imbalance'] for m in final_metrics]
    ax.bar(policies, imbalances, color='coral')
    ax.set_ylabel("Coefficient of Variation")
    ax.set_title("Load Imbalance")
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Plot 3: Load Std Dev
    ax = axes[1, 0]
    std_devs = [m['load_std_dev'] for m in final_metrics]
    ax.bar(policies, std_devs, color='lightgreen')
    ax.set_ylabel("Standard Deviation")
    ax.set_title("Load Standard Deviation")
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, axis='y')
    
    # Plot 4: Files Created
    ax = axes[1, 1]
    num_files = [m['num_files'] for m in final_metrics]
    ax.bar(policies, num_files, color='plum')
    ax.set_ylabel("Number of Files")
    ax.set_title("Total Files Created")
    ax.tick_params(axis='x', rotation=45)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.suptitle("Policy Comparison at 90% Fill (8 nodes)", 
                 fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    filename = "plots/policy_comparison.png"
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Saved {filename}")
    plt.close()

# ============================================================================
# Main
# ============================================================================

def main():
    print("=" * 60)
    print("DFS Workload Visualization")
    print("=" * 60)
    
    print("\nGenerating plots...")
    
    plot_fill_workload()
    plot_mixed_workload()
    plot_readheavy_workload()
    plot_imbalance_workload()
    plot_node_scaling()
    plot_policy_comparison()
    
    print("\n" + "=" * 60)
    print("All plots saved to ./plots/ directory")
    print("=" * 60)

if __name__ == "__main__":
    main()

