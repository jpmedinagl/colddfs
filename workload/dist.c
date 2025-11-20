#include <time.h>
#include <math.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345
#define NUM_NODES 8
#define TOTAL_CAPACITY (1024 * 1024 * 16) // 16 MB total file system size (fixed bytes)
#define NUM_OPERATIONS 1000 // Increased operations for better statistical sample

// Track created files for read/truncate operations
typedef struct {
    int fid;
    size_t current_size;
} FileTracker;

/**
 * @brief Executes a mixed workload of C/R/W/T to test the effect of File Distribution.
 * * @param policy The allocation policy to use (e.g., "leastloaded").
 * @param dist The file size distribution (e.g., DIST_UNIFORM_LARGE).
 */
void workload_distribution_test(const char *policy, FileDistribution dist)
{
    srand(SEED);

    size_t total_blocks = TOTAL_CAPACITY / BLOCK_SIZE;
    const char* dist_name = dist_to_string(dist);
    
    if (total_blocks == 0) {
        printf("ERROR: Block size (%d bytes) is larger than total capacity (%d bytes).\n", 
               BLOCK_SIZE, TOTAL_CAPACITY);
        return;
    }
    
    printf("\n========================================\n");
    printf("WORKLOAD: Distribution Impact (RQ2)\n");
    printf("Policy: %s, Distribution: %s, Block Size: %d bytes\n", 
           policy, dist_name, BLOCK_SIZE);
    printf("Total Capacity (Bytes): %d, Total Blocks: %zu\n", 
           TOTAL_CAPACITY, total_blocks);
    printf("========================================\n");
    
    // Initialize the MetadataNode
    metadatanode_init(NUM_NODES, TOTAL_CAPACITY, policy);
    
    SystemMetrics final_metrics = {0};
    
    double total_write_time = 0;
    double total_read_time = 0;
    int write_count = 0; // Includes creation and truncation writes
    int read_count = 0;
    int truncate_count = 0;
    
    // --- METRIC TRACKING: LOGICAL DATA ---
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
                 printf("Warning: System ran out of space (pre-check) at op %d.\n", i);
                 break;
            }

            char *buffer = malloc(file_size);
            memset(buffer, 'C', file_size);
            
            double start = get_time_ms();
            
            int fid;
            MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
            
            if (status != MDN_SUCCESS) {
                printf("Warning: System ran out of space at op %d.\n", i);
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
            // 3. TRUNCATE OPERATION (10%)
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
                                  
    // --- FRAGMENTATION CALCULATION ---
    size_t total_physical_bytes_used = final_metrics.blocks_used * BLOCK_SIZE;
    size_t wasted_bytes = total_physical_bytes_used > total_logical_data_bytes ? 
                          total_physical_bytes_used - total_logical_data_bytes : 0;
    
    double frag_percent = 0.0;
    if (total_physical_bytes_used > 0) {
        frag_percent = (wasted_bytes * 100.0) / total_physical_bytes_used;
    }
                                  
    // Calculate Throughput (MB/s) based on PHYSICAL SPACE WRITTEN
    double total_time_sec = total_write_time / 1000.0;
    double throughput = (total_time_sec > 0) ? 
                        (total_physical_bytes_used / (1024.0 * 1024.0) / total_time_sec) : 0.0;
    
    // Export results (append mode)
    FILE *f = fopen("results/results_distribution_summary.csv", "a");
    if (!f) {
        perror("Failed to open summary CSV file");
    } else {
        // Write header only if file is empty
        if (ftell(f) == 0 || num_files == 0) {
            fprintf(f, "policy,distribution,block_size_bytes,total_capacity_mb,files_created,"
                       "avg_write_latency_ms,avg_read_latency_ms,write_throughput_mbps,load_imbalance,"
                       "metadata_kb,logical_data_kb,wasted_space_kb,frag_percent\n");
        }
        
        fprintf(f, "%s,%s,%d,%d,%d,%.6f,%.6f,%.6f,%.6f,%.2f,%.2f,%.2f,%.2f\n",
                policy, 
                dist_name,
                BLOCK_SIZE, 
                TOTAL_CAPACITY / (1024 * 1024), 
                num_files,
                final_metrics.avg_write_latency_ms,
                final_metrics.avg_read_latency_ms,
                throughput,
                final_metrics.load_imbalance,
                final_metrics.metadata_bytes / 1024.0,
                total_logical_data_bytes / 1024.0,
                wasted_bytes / 1024.0,
                frag_percent);
        
        fclose(f);
        printf("Summary results appended to results/results_distribution_summary.csv\n");
    }
    
    free(files);
    metadatanode_exit(1);
}

int main(int argc, char *argv[])
{
    // Policies to test (Simple, Load-Aware, Worst-Case, Adaptive)
    const char *policies[] = { "roundrobin", "leastloaded", "sequential", "fileaware" };
    
    // Distributions to test (RQ2)
    FileDistribution distributions[] = { 
        DIST_UNIFORM_SMALL,    // All small, minimal variation (Stress test for capacity)
        DIST_UNIFORM_LARGE,    // All medium/large, minimal variation (Stress test for load balance)
        DIST_BIMODAL,          // Extreme mix (80% tiny, 20% large)
        DIST_WEB_REALISTIC,    // Your baseline workload (70% tiny, 20% medium, 10% large)
        DIST_VIDEO             // All large, high variation (Stress test for write time/IO)
    };
    
    int num_policies = sizeof(policies) / sizeof(policies[0]);
    int num_distributions = sizeof(distributions) / sizeof(distributions[0]);
    
    // Nested loop to test every policy against every distribution
    for (int d = 0; d < num_distributions; d++) {
        for (int p = 0; p < num_policies; p++) {
            workload_distribution_test(policies[p], distributions[d]);
        }
    }

    return 0;
}
