import pandas as pd
import matplotlib.pyplot as plt
import os

# --- Configuration ---
CSV_FILE = 'results_node_scaling_summary_16.csv'
OUTPUT_DIR = '.'
METRICS = [
    "avg_read_latency_ms", 
    "avg_write_latency_ms",
    "write_throughput_mbps"
]

# --- Helper Function ---
def load_data(file_path):
    """Loads and preprocesses the CSV data from a single file."""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at {file_path}")
        return None
    
    df = pd.read_csv(file_path)
    
    # 1. Ensure node count is integer and sort
    df['num_nodes'] = df['num_nodes'].astype(int)
    df = df.sort_values(by=['policy', 'num_nodes'])
    
    # 2. Remove duplicates (keeping the last run)
    df = df.drop_duplicates(subset=['policy', 'num_nodes'], keep='last')
    
    return df

def plot_scaling_metric(df, metric):
    """
    Generates a line plot showing policy performance vs. number of nodes for a single metric.
    Uses the clean, single-plot style requested.
    """
    
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 1. Restructure data: results[policy][num_nodes] = value
    results = {}
    
    # Note: We group by policy first
    for policy, policy_df in df.groupby('policy'):
        # Create dictionary mapping node count -> metric value
        results[policy] = dict(zip(policy_df['num_nodes'], policy_df[metric]))
    
    
    # Get all unique node counts for axis reference
    node_counts = sorted(df['num_nodes'].unique())
    
    # Determine workload context for plot title
    workload_context = df['distribution'].iloc[0].replace('_', ' ').title()
    block_size_context = df['block_size_bytes'].iloc[0]

    # --- Plotting ---
    plt.figure(figsize=(10, 6))

    for policy, data in results.items():
        # Get the sorted list of X-axis values present for this policy
        plot_x = sorted(data.keys())
        plot_y = [data[n] for n in plot_x]
        
        plt.plot(
            plot_x,
            plot_y,
            marker='o',
            linestyle='-',
            label=policy
        )

    # Apply scaling and formatting
    plt.xscale('log', base=2)

    # Set X-tick labels explicitly to show the node counts (2, 4, 8, 16...)
    plt.xticks(node_counts, [f"{n} Nodes" for n in node_counts], rotation=20)
    
    # Dynamic labeling based on the metric name
    ylabel = metric.replace('_', ' ').title().replace('Ms', '(ms)').replace('Mbps', '(MB/s)').replace('Kb', '(KB)')
    title = f'{ylabel} vs. Data Node Count\n(Workload: {workload_context}, Block Size: {block_size_context // 1024} KB)'
    
    plt.title(title, fontsize=14)
    plt.xlabel('Number of Data Nodes (N)', fontsize=12)
    plt.ylabel(ylabel, fontsize=12)
    
    plt.legend(title='Policy')
    plt.grid(True, which="both", ls="--", linewidth=0.5)

    plt.tight_layout()
    
    output_filename = os.path.join(OUTPUT_DIR, f'{metric}_scaling_plot.pdf')
    plt.savefig(output_filename)
    plt.close()
    print(f"Generated plot: {output_filename}")


if __name__ == "__main__":
    # Load data once
    df = load_data(CSV_FILE)
    if df is None:
        exit(1)
        
    # --- Execute plotting for requested metrics ---
    
    # Mapping metrics to their scale type
    METRIC_SCALES = {
        'write_throughput_mbps': 'linear',
        'avg_write_latency_ms': 'linear',
        'avg_read_latency_ms': 'linear',
        'load_imbalance': 'log', # Critical to use log scale here
        'files_created': 'linear',
        'metadata_kb': 'linear',
    }

    for metric in METRICS:
        if metric in df.columns:
             plot_scaling_metric(df, metric)
        else:
            print(f"Warning: Metric '{metric}' not found in CSV columns.")