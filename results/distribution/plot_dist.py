import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os

# --- Configuration ---
CSV_FILE = 'results_distribution_summary.csv'
OUTPUT_DIR = '.'

# --- Custom Plotting Style ---
plt.style.use('ggplot')
POLICY_COLORS = {
    'roundrobin': '#1f77b4',
    'leastloaded': '#2ca02c',
    'mostloaded': '#d62728',
    'fileaware': '#ff7f0e',
    'rand': '#9467bd',
}

# --- Distribution Mapping (for consistent bar colors across plots) ---
# We map the display name to a consistent color/marker for the legend
DISTRIBUTION_COLORS = {
    'Uniform Small': '#1f77b4',  # Blue (Small/Tiny focus)
    'Uniform Large': '#ff7f0e',  # Orange (Large focus)
    'Bimodal': '#2ca02c',        # Green (Mixed extreme)
    'Web Realistic': '#d62728',  # Red (Real-world complexity)
    'Video': '#9467bd',          # Purple (Extremely large I/O)
}
# --- Helper to load and preprocess data ---

def load_data(file_path):
    """Loads and preprocesses the CSV data."""
    if not os.path.exists(file_path):
        print(f"Error: CSV file not found at {file_path}")
        return None
    
    df = pd.read_csv(file_path)
    
    # 1. Standardize Distribution Names for Plotting
    df['distribution_display'] = df['distribution'].str.replace('_', ' ').str.title()
    
    # 2. Ensure only unique policy-distribution combinations are used (handles reruns)
    df = df.drop_duplicates(subset=['policy', 'distribution'], keep='last')
    
    return df

# --- NEW PLOTTING FUNCTION: GROUPED BAR CHARTS ---

def plot_grouped_bars(df, metric, title, ylabel, ascending=False):
    """
    Generates a grouped bar plot comparing policies (X-axis) across 
    different distributions (grouped bars).
    """
    policies = df['policy'].unique()
    distributions = list(DISTRIBUTION_COLORS.keys())
    
    # Sort policies alphabetically for consistent grouping order
    policies.sort()
    
    # Pivot the data to get policies as the main columns
    pivot_df = df.pivot(index='distribution_display', columns='policy', values=metric)
    
    # Reindex rows and columns for guaranteed order
    pivot_df = pivot_df.reindex(index=distributions, columns=policies)

    num_policies = len(policies)
    num_distributions = len(distributions)
    bar_width = 0.15
    
    # Set figure size based on the number of groups
    fig, ax = plt.subplots(figsize=(14, 7))

    # Calculate x positions for the groups (policies)
    policy_x = np.arange(num_policies)
    
    # Iterate through each distribution to draw its bar for every policy
    for i, dist in enumerate(distributions):
        # Calculate the starting position for this distribution's set of bars
        offset = policy_x + (i - num_distributions / 2 + 0.5) * bar_width
        
        # Get data for this distribution across all policies
        values = pivot_df.loc[dist].values
        
        ax.bar(
            offset,
            values,
            bar_width,
            label=dist,
            color=DISTRIBUTION_COLORS[dist]
        )
        
        # Add labels on top of the bars
        for x, y in zip(offset, values):
            # Only label bars that aren't zero or NaN
            if pd.notna(y) and y != 0:
                ax.text(x, y, f'{y:.2f}', ha='center', va='bottom', fontsize=8, rotation=90)


    # Final plot customization
    ax.set_xticks(policy_x)
    ax.set_xticklabels(policies)
    ax.set_xlabel('Allocation Policy', fontsize=14)
    ax.set_ylabel(ylabel, fontsize=14)
    ax.set_title(f'Policy Performance by Workload: {title}', fontsize=16)
    
    # Move legend outside the plot
    ax.legend(title='Workload Distribution', bbox_to_anchor=(1.05, 1), loc='upper left')
    ax.grid(axis='y')

    plt.tight_layout() #rect=[0, 0, 0.85, 1]) # Adjust layout for outside legend
    plt.savefig(os.path.join(OUTPUT_DIR, f'{metric}_grouped_policies.png'))
    plt.close()
    print(f"Generated grouped bar chart: {metric}_grouped_policies.png")


# --- Original Line Plot (Kept for Load Imbalance sensitivity) ---

def plot_load_imbalance(df):
    """Generates a line plot showing load imbalance sensitivity across policies."""
    
    # Pivot table to put distributions on the X-axis and policies as separate columns
    pivot_df = df.pivot(index='distribution_display', columns='policy', values='load_imbalance')
    
    # Reorder the index (distributions) logically
    order = list(DISTRIBUTION_COLORS.keys())
    pivot_df = pivot_df.reindex(order)

    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Plot each policy
    for policy in pivot_df.columns:
        ax.plot(
            pivot_df.index, 
            pivot_df[policy], 
            label=policy, 
            marker='o', 
            color=POLICY_COLORS.get(policy, 'gray')
        )
        
    ax.set_title('Load Imbalance Sensitivity by Workload Distribution (RQ2)', fontsize=16)
    ax.set_xlabel('File Size Distribution', fontsize=12)
    ax.set_ylabel('Load Imbalance (Coefficient of Variation)', fontsize=12)
    ax.legend(title='Policy')
    ax.grid(True)
    plt.xticks(rotation=15, ha='right')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, 'load_imbalance_sensitivity.png'))
    plt.close()
    print("Generated plot: load_imbalance_sensitivity.png")


def run_analysis():
    """Main function to run the data loading and plotting."""
    
    # 1. Setup output directory
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 2. Load data
    df = load_data(CSV_FILE)
    if df is None:
        return

    # 3. Generate individual metric plots using the new grouped bar chart function
    
    # A. Write Performance (Throughput)
    plot_grouped_bars(
        df, 'write_throughput_mbps', 
        'Write Throughput (MB/s)', 
        'Throughput (MB/s)'
    )
    
    # B. Read Performance (Latency)
    plot_grouped_bars(
        df, 'avg_read_latency_ms', 
        'Average Read Latency (ms)', 
        'Latency (ms)'
    )
    
    # C. Metadata Overhead
    plot_grouped_bars(
        df, 'metadata_kb', 
        'Metadata Overhead (KB)', 
        'Metadata Size (KB)'
    )
    
    # # D. Load Imbalance (Crucial for RQ2)
    # plot_grouped_bars(
    #     df, 'load_imbalance',
    #     'Load Imbalance (Coefficient of Variation)',
    #     'Imbalance Score'
    # )
    
    # E. Fragmentation (Capacity Efficiency)
    plot_grouped_bars(
        df, 'frag_percent',
        'Capacity Wasted due to Fragmentation (%)',
        'Fragmentation (%)'
    )

    # 4. Generate comparative sensitivity plot (Kept as Line Plot, better for sensitivity)
    plot_load_imbalance(df)

if __name__ == '__main__':
    run_analysis()
