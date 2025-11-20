#include <time.h>
#include <math.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345
#define CONSTANT_TOTAL_CAPACITY (1024 * 1024 * 16) // Fixed 64 MB total file system size
#define NUM_OPERATIONS 1000 // Number of write/read attempts

// Track created files for read/truncate operations
typedef struct {
    int fid;
    size_t current_size;
} FileTracker;

/**
 * @brief Executes a mixed workload to test performance scaling with node count.
 * * @param policy The allocation policy to use.
 * @param num_nodes The number of Data Nodes to initialize.
 * @param dist The file size distribution.
 */
void workload_node_scaling(const char *policy, int num_nodes, FileDistribution dist)
{
    srand(SEED);

    // Calculate total blocks based on the FIXED byte capacity and the CURRENT BLOCK_SIZE
    size_t total_blocks = CONSTANT_TOTAL_CAPACITY / BLOCK_SIZE;
    const char* dist_name = dist_to_string(dist);
    
    if (total_blocks == 0) {
        printf("ERROR: Block size (%d bytes) is larger than total capacity (%d bytes).\n", 
               BLOCK_SIZE, CONSTANT_TOTAL_CAPACITY);
        return;
    }
    
    printf("\n========================================\n");
    printf("WORKLOAD: Data Node Scaling (RQ3)\n");
    printf("Policy: %s, Nodes: %d, Distribution: %s, Block Size: %d bytes\n", 
           policy, num_nodes, dist_name, BLOCK_SIZE);
    printf("Total Capacity (Bytes): %d, Total Blocks: %zu\n", 
           CONSTANT_TOTAL_CAPACITY, total_blocks);
    printf("========================================\n");
    
    // Initialize the MetadataNode with the variable number of nodes
    metadatanode_init(num_nodes, CONSTANT_TOTAL_CAPACITY, policy);
    
    SystemMetrics final_metrics = {0};
    
    double total_write_time = 0;
    double total_read_time = 0;
    int write_count = 0; 
    int read_count = 0;
    int truncate_count = 0;
    
    size_t total_logical_data_bytes = 0;
    
    FileTracker *files = malloc(sizeof(FileTracker) * NUM_OPERATIONS);
    int num_files = 0;
    size_t blocks_allocated = 0;
    
    // Perform mixed operations
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        
        // Operation distribution (50% Writes/Creates, 40% Reads, 10% Truncates)
        int op_type = rand() % 100;
        
        if (op_type < 50 || num_files == 0) {
            // 1. WRITE/CREATE OPERATION
            char filename[64];
            snprintf(filename, sizeof(filename), "file_%d.dat", num_files);
            
            size_t file_size = generate_file_size(dist); 
            size_t blocks_needed = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            if (blocks_needed > total_blocks - blocks_allocated) {
                 // Stop test if capacity is exhausted
                 break;
            }

            char *buffer = malloc(file_size);
            memset(buffer, 'C', file_size);
            
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
            
            blocks_allocated += blocks_needed;
            total_logical_data_bytes += file_size;
            
            if (num_files < NUM_OPERATIONS) {
                files[num_files].fid = fid;
                files[num_files].current_size = file_size;
                num_files++;
            }
            
            free(buffer);
            
        } else if (op_type < 90) {
            // 2. READ OPERATION
            if (num_files > 0) {
                int file_idx = rand() % num_files;
                int fid = files[file_idx].fid;
                
                double start = get_time_ms();
                
                void *read_buffer = NULL;
                size_t size;
                metadatanode_read_file(fid, &read_buffer, &size);
                
                double end = get_time_ms();
                
                total_read_time += (end - start);
                read_count++;
                if (read_buffer) free(read_buffer);
            }
            
        } else {
            // 3. TRUNCATE OPERATION (10%) - Shrink by 1-3 blocks
            if (num_files > 0) {
                int file_idx = rand() % num_files;
                int fid = files[file_idx].fid;
                size_t current_size = files[file_idx].current_size;
                
                int current_blocks = (current_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
                int blocks_to_remove = 1 + (rand() % 3);
                
                if (current_blocks > blocks_to_remove) {
                    int new_blocks = current_blocks - blocks_to_remove;
                    size_t new_size = new_blocks * BLOCK_SIZE; 
                    
                    double start = get_time_ms();
                    MDNStatus status = metadatanode_truncate_file(fid, new_size);
                    double end = get_time_ms();
                    
                    if (status == MDN_SUCCESS) {
                        total_write_time += (end - start);
                        truncate_count++;
                        
                        size_t logical_size_removed = (current_blocks - new_blocks) * BLOCK_SIZE;
                        
                        total_logical_data_bytes -= logical_size_removed;
                        blocks_allocated -= (current_blocks - new_blocks);
                        files[file_idx].current_size = new_size;
                    }
                }
            }
        }
    }
    
    // Final metric capture
    final_metrics = capture_metrics(total_write_time, total_read_time, 
                                  write_count, read_count);
                                  
    // --- CALCULATE KEY SCALING METRICS ---
    size_t total_physical_bytes_used = final_metrics.blocks_used * BLOCK_SIZE;
    
    double total_time_sec = total_write_time / 1000.0;
    double throughput = (total_time_sec > 0) ? 
                        (total_physical_bytes_used / (1024.0 * 1024.0) / total_time_sec) : 0.0;
    
    // Export results (append mode)
    FILE *f = fopen("results/results_node_scaling_summary.csv", "a");
    if (!f) {
        perror("Failed to open summary CSV file");
    } else {
        // Write header only if file is empty
        if (ftell(f) == 0 || num_files == 0) {
            fprintf(f, "policy,num_nodes,distribution,block_size_bytes,total_capacity_mb,files_created,"
                       "avg_write_latency_ms,avg_read_latency_ms,write_throughput_mbps,load_imbalance,metadata_kb\n");
        }
        
        fprintf(f, "%s,%d,%s,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%.2f\n",
                policy, 
                num_nodes,
                dist_name,
                BLOCK_SIZE, 
                CONSTANT_TOTAL_CAPACITY / (1024 * 1024), 
                num_files,
                final_metrics.avg_write_latency_ms,
                final_metrics.avg_read_latency_ms,
                throughput,
                final_metrics.load_imbalance,
                final_metrics.metadata_bytes / 1024.0);
        
        fclose(f);
        printf("Summary results appended to results/results_node_scaling_summary.csv\n");
    }
    
    free(files);
    metadatanode_exit(1);
}

int main(int argc, char *argv[])
{
    // Policies to test (Need load awareness vs naive)
    const char *policies[] = { "roundrobin", "leastloaded", "sequential", "fileaware" };
    
    // Node Counts (N) to test: 2 -> 4 -> 8 -> 16
    int node_counts[] = { 2, 4, 8, 16 };
    
    // Primary Distribution: Large files to stress I/O and expose load balance issues
    FileDistribution dist = DIST_UNIFORM_LARGE;

    int num_policies = sizeof(policies) / sizeof(policies[0]);
    int num_counts = sizeof(node_counts) / sizeof(node_counts[0]);
    
    // Nested loop to test every policy against every node count
    for (int n = 0; n < num_counts; n++) {
        for (int p = 0; p < num_policies; p++) {
            workload_node_scaling(policies[p], node_counts[n], dist);
        }
    }

    return 0;
}
