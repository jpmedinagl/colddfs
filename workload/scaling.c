#include <time.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345

// Workload: Test how performance scales with number of data nodes (RQ3)
// Keep total capacity constant, vary number of nodes
void workload_node_scaling_single(const char *policy, int num_nodes, 
                                 size_t total_capacity, FileDistribution dist)
{
    srand(SEED);

    printf("\n========================================\n");
    printf("WORKLOAD: Node Scaling\n");
    printf("Policy: %s, Nodes: %d, Distribution: %s\n", 
           policy, num_nodes, dist_to_string(dist));
    printf("========================================\n");
    
    metadatanode_init(num_nodes, total_capacity, policy);
    
    SystemMetrics metrics[10];
    int metric_idx = 0;
    
    double total_write_time = 0;
    int write_count = 0;
    
    size_t blocks_allocated = 0;
    size_t total_blocks = total_capacity / BLOCK_SIZE;
    
    // Fill to 70% with consistent workload
    for (int target_fill = 10; target_fill <= 70; target_fill += 10) {
        printf("\nFilling to %d%%...\n", target_fill);
        
        while ((blocks_allocated * 100) / total_blocks < target_fill) {
            char filename[64];
            snprintf(filename, sizeof(filename), "file_%d.dat", write_count);
            
            size_t file_size = generate_file_size(dist);
            size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            char *buffer = malloc(file_size);
            memset(buffer, 'A' + (write_count % 26), file_size);
            
            double start = get_time_ms();
            
            int fid;
            MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
            
            if (status != MDN_SUCCESS) {
                free(buffer);
                break;
            }
            
            metadatanode_write_file(fid, buffer, file_size);
            
            double end = get_time_ms();
            total_write_time += (end - start);
            write_count++;
            blocks_allocated += blocks;
            
            free(buffer);
        }
        
        metrics[metric_idx] = capture_metrics(total_write_time, 0, write_count, 0);
        print_metrics(&metrics[metric_idx], "Current State");
        metric_idx++;
    }
    
    // Export results
    char csv_filename[256];
    snprintf(csv_filename, sizeof(csv_filename), 
             "results/results_scaling_%s_%s_%dnodes.csv", 
             policy, dist_to_string(dist), num_nodes);
    export_metrics_csv(csv_filename, metrics, metric_idx, policy);
    
    metadatanode_exit(1);
}

// Run scaling test across multiple node counts
void workload_node_scaling_comparison(const char *policy, FileDistribution dist)
{
    size_t total_capacity = 1000 * BLOCK_SIZE; // Keep constant
    int node_counts[] = {2, 4, 8, 16};
    
    // Open or create summary file
    FILE *summary = fopen("results/results_scaling_summary.csv", "r");
    if (summary) {
        fclose(summary);
        summary = fopen("results/results_scaling_summary.csv", "a");
    } else {
        summary = fopen("results/results_scaling_summary.csv", "w");
        fprintf(summary, "policy,num_nodes,files_created,avg_write_latency_ms,"
                        "throughput_mbps,load_imbalance,final_fill_pct\n");
    }
    
    if (!summary) {
        perror("Failed to open summary file");
        return;
    }
    
    for (int i = 0; i < 4; i++) {
        int num_nodes = node_counts[i];
        
        printf("\n========== Testing with %d nodes ==========\n", num_nodes);
        
        srand(SEED); // Reset for consistency
        metadatanode_init(num_nodes, total_capacity, policy);
        
        double total_write_time = 0;
        int write_count = 0;
        size_t blocks_allocated = 0;
        size_t total_blocks = total_capacity / BLOCK_SIZE;
        
        // Fill to 70%
        while ((blocks_allocated * 100) / total_blocks < 70) {
            char filename[64];
            snprintf(filename, sizeof(filename), "file_%d.dat", write_count);
            
            size_t file_size = generate_file_size(dist);
            size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            char *buffer = malloc(file_size);
            memset(buffer, 'X', file_size);
            
            double start = get_time_ms();
            
            int fid;
            MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
            
            if (status != MDN_SUCCESS) {
                free(buffer);
                break;
            }
            
            metadatanode_write_file(fid, buffer, file_size);
            
            double end = get_time_ms();
            total_write_time += (end - start);
            write_count++;
            blocks_allocated += blocks;
            
            free(buffer);
        }
        
        SystemMetrics m = capture_metrics(total_write_time, 0, write_count, 0);
        
        // Calculate throughput (MB/s)
        double total_data_mb = (blocks_allocated * BLOCK_SIZE) / (1024.0 * 1024.0);
        double total_time_sec = total_write_time / 1000.0;
        double throughput = total_data_mb / total_time_sec;
        
        printf("\nResults for %d nodes:\n", num_nodes);
        printf("  Files created: %d\n", write_count);
        printf("  Avg latency: %.3f ms\n", m.avg_write_latency_ms);
        printf("  Throughput: %.2f MB/s\n", throughput);
        printf("  Load imbalance: %.4f\n", m.load_imbalance);
        printf("  Fill: %d%%\n", m.fill_percentage);
        
        // Write to summary
        fprintf(summary, "%s,%d,%d,%.6f,%.6f,%.6f,%d\n",
                policy, num_nodes, write_count, m.avg_write_latency_ms,
                throughput, m.load_imbalance, m.fill_percentage);
        fflush(summary);
        
        metadatanode_exit(1);
    }
    
    fclose(summary);
}

int main(int argc, char *argv[])
{
    printf("========================================\n");
    printf("DFS Node Scaling Workload (RQ3)\n");
    printf("========================================\n");

    FileDistribution dist = DIST_WEB_REALISTIC;
    
    const char *policies[] = { "sequential", "roundrobin", "leastloaded", 
                              "weightedroundrobin", "rand", "fileaware" };
    
    // Test each policy across different node counts
    for (int i = 0; i < 6; i++) {
        printf("\n\n========== POLICY: %s ==========\n", policies[i]);
        workload_node_scaling_comparison(policies[i], dist);
    }
    
    printf("\n========================================\n");
    printf("Scaling tests complete!\n");
    printf("See results_scaling_summary.csv for comparison\n");
    printf("========================================\n");

    return 0;
}
