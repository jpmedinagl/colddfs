# import pandas as pd
# import matplotlib.pyplot as plt
# import os
# import argparse
# import glob

# # --- Configuration ---
# # The target file provided by the user
# CSV_FILE = 'results_fileaware_tuning_summary.csv' 
# OUTPUT_DIR = '.'
# DEFAULT_METRICS = [
#     "avg_write_latency_ms",
# ]

# def load_data(file_path):
#     """Loads and preprocesses the CSV data."""
#     if not os.path.exists(file_path):
#         # Fallback using glob if the direct path fails (common in terminal setups)
#         files = glob.glob(file_path)
#         if files:
#             file_path = files[0]
#         else:
#             print(f"Error: CSV file not found at {file_path}")
#             return None
    
#     df = pd.read_csv(file_path)
    
#     # 1. Ensure threshold is sorted for plotting
#     df['threshold'] = df['threshold'].astype(int)
#     df = df.sort_values(by=['policy', 'threshold'])
    
#     # 2. Remove duplicates (keeping the last run)
#     df = df.drop_duplicates(subset=['policy', 'threshold'], keep='last')
    
#     return df

# def format_threshold(x):
#     """Formats the threshold (block count) for the X-axis label."""
#     if x == 1:
#         return f"{x} Block"
#     return f"{x} Blocks"

# def plot_tuning_metric(df, metric):
#     """
#     Generates a line plot showing policy performance vs. the fileaware threshold.
#     """
    
#     os.makedirs(OUTPUT_DIR, exist_ok=True)
    
#     # 1. Restructure data: results[policy][threshold] = value
#     results = {}
    
#     for policy, policy_df in df.groupby('policy'):
#         # Map threshold (X) to metric value (Y)
#         results[policy] = dict(zip(policy_df['threshold'], policy_df[metric]))
    
#     # Get all unique thresholds for axis reference
#     thresholds = sorted(df['threshold'].unique())

#     # Determine workload context for plot title
#     workload_context = df['distribution'].iloc[0].replace('_', ' ').title()
#     block_size_context = df['block_size_bytes'].iloc[0]

#     # --- Plotting ---
#     plt.figure(figsize=(10, 6))
#     plt.style.use('ggplot') # Ensure consistent styling

#     for policy, data in results.items():
#         # Get the sorted list of X-axis values present for this policy
#         plot_x = sorted(data.keys())
#         plot_y = [data[t] for t in plot_x]
        
#         plt.plot(
#             plot_x,
#             plot_y,
#             marker='o',
#             linestyle='-',
#             label=policy # For your data, this will just be 'fileaware'
#         )

#     # Apply scaling and formatting
#     plt.xscale("log", base=2)
    
#     # Set X-tick labels using the helper function
#     plt.xticks(thresholds, [format_threshold(t) for t in thresholds], rotation=45, ha='right')
    
#     # Dynamic labeling based on the metric name
#     ylabel = metric.replace('_', ' ').title().replace('Ms', '(ms)').replace('Kb', '(KB)')
    
#     title = f'File Aware Policy Tuning: {ylabel}\n(Workload: {workload_context}, Block Size: {block_size_context // 1024} KB)'
    
#     plt.title(title, fontsize=14)
#     plt.xlabel('File Size Threshold (Blocks)', fontsize=12)
#     plt.ylabel(ylabel, fontsize=12)
    
#     # Add a vertical line at the default threshold (4) for reference
#     plt.axvline(x=4, color='gray', linestyle='--', linewidth=1, alpha=0.7)
#     plt.text(4, plt.ylim()[1] * 0.9, 'Default (4 Blocks)', rotation=90, 
#              color='gray', verticalalignment='top', horizontalalignment='right', 
#              fontsize=10, bbox=dict(facecolor='white', alpha=0.5, edgecolor='none'))
    
#     plt.legend(title='Policy')
#     plt.grid(True, which="major", ls="--", linewidth=0.5)

#     plt.tight_layout()
    
#     output_filename = os.path.join(OUTPUT_DIR, f'fileaware_tuning_{metric}.pdf')
#     plt.savefig(output_filename)
#     plt.close()
#     print(f"Generated plot: {output_filename}")


# if __name__ == "__main__":
    
#     # Load data once
#     df = load_data(CSV_FILE)
#     if df is None:
#         exit(1)
        
#     # --- Execute plotting for requested metrics ---
    
#     for metric in DEFAULT_METRICS:
#         if metric in df.columns:
#              plot_tuning_metric(df, metric)
#         else:
#             print(f"Warning: Metric '{metric}' not found in CSV columns.")

import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV
df = pd.read_csv("results_fileaware_tuning_summary_32k.csv")

def plot_metric(df, metric):
    """
    Plot <metric> vs threshold for the given dataframe.
    """
    if metric not in df.columns:
        raise ValueError(f"Metric '{metric}' not found. Available: {list(df.columns)}")
    
    thresholds = df["threshold"]

    plt.figure(figsize=(7,4))
    plt.plot(thresholds, df[metric], marker="o")

    plt.xscale("log", base=2)
    plt.xticks(thresholds, [str(t) for t in thresholds])

    plt.xlabel("Threshold (Blocks)")

    ylabel = metric.replace('_', ' ').title().replace('Ms', '(ms)').replace('Kb', '(KB)')
    plt.ylabel(ylabel)

    workload_context = df['distribution'].iloc[0].replace('_', ' ').title()
    block_size_context = df['block_size_bytes'].iloc[0]
    
    title = f'File Aware Policy Tuning: {ylabel}' #\n(Workload: {workload_context}, Block Size: {block_size_context // 1024} KB)'
    
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    # plt.show()
    plt.savefig(metric + "32k.png")

# Example usage:
if __name__ == '__main__':
    plot_metric(df, "avg_write_latency_ms")
