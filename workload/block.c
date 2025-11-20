#include <time.h>
#include <math.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345
#define NUM_NODES 8
#define CONSTANT_TOTAL_CAPACITY (1024 * 1024 * 16) // 16 MB total file system size (fixed bytes)
#define NUM_OPERATIONS 500

// Track created files for read/truncate operations
typedef struct {
    int fid;
    size_t current_size;
} FileTracker;

/**
 * @brief Executes a mixed workload of C/R/W/T to test the effect of BLOCK_SIZE.
 * * The total capacity is kept constant in bytes (16MB) to ensure a fair comparison.
 * The test focuses on a Web Realistic distribution, as this mix of small and large 
 * files will expose fragmentation issues and metadata overhead best.
 * * @param policy The allocation policy to use (e.g., "leastloaded").
 * @param dist The file size distribution to use.
 */
void workload_block_size_test(const char *policy, FileDistribution dist)
{
    srand(SEED);

    // Calculate total blocks based on the FIXED byte capacity and the CURRENT BLOCK_SIZE
    size_t total_blocks = CONSTANT_TOTAL_CAPACITY / BLOCK_SIZE;
    if (total_blocks == 0) {
        printf("ERROR: Block size (%d bytes) is larger than or equal to total capacity (%d bytes).\n", 
               BLOCK_SIZE, CONSTANT_TOTAL_CAPACITY);
        return;
    }
    
    printf("\n========================================\n");
    printf("WORKLOAD: Block Size Impact (RQ5)\n");
    printf("Policy: %s, Nodes: %d, Block Size: %d bytes\n", 
           policy, NUM_NODES, BLOCK_SIZE);
    printf("Total Capacity (Bytes): %d, Total Blocks: %zu\n", 
           CONSTANT_TOTAL_CAPACITY, total_blocks);
    printf("========================================\n");
    
    metadatanode_init(NUM_NODES, CONSTANT_TOTAL_CAPACITY, policy);
    
    // We will only capture the final metrics
    SystemMetrics final_metrics = {0};
    
    double total_write_time = 0;
    double total_read_time = 0;
    int write_count = 0; // Includes creation and truncation writes
    int read_count = 0;
    int truncate_count = 0;
    
    // Track files for operations (up to 500 files)
    FileTracker *files = malloc(sizeof(FileTracker) * NUM_OPERATIONS);
    int num_files = 0; // Only counts files created, not removed/truncated to zero
    
    size_t blocks_allocated = 0;
    
    // Perform mixed operations until either the operation count is reached 
    // or the system runs out of space.
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        
        // Operation distribution:
        // 50% Writes/Creates
        // 40% Reads
        // 10% Truncates (only if files exist)
        int op_type = rand() % 100;
        
        if (op_type < 50 || num_files == 0) {
            // 1. WRITE/CREATE OPERATION
            char filename[64];
            snprintf(filename, sizeof(filename), "file_%d.dat", num_files);
            
            // Use Web Realistic distribution to get a good mix
            size_t file_size = generate_file_size(dist); 
            size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            // Allocate a buffer for the entire file content
            char *buffer = malloc(file_size);
            memset(buffer, 'C', file_size); // Fill with content for a realistic write
            
            double start = get_time_ms();
            
            int fid;
            MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
            
            if (status != MDN_SUCCESS) {
                // Out of space, terminate test
                printf("Warning: System ran out of space at op %d.\n", i);
                free(buffer);
                break; 
            }
            
            // Write the file content
            metadatanode_write_file(fid, buffer, file_size);
            
            double end = get_time_ms();
            total_write_time += (end - start);
            write_count++;
            blocks_allocated += blocks;
            
            // Track this file
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
                
                void *read_buffer;
                size_t size;
                MDNStatus status = metadatanode_read_file(fid, &read_buffer, &size);
                
                double end = get_time_ms();
                
                if (status == MDN_SUCCESS) {
                    total_read_time += (end - start);
                    read_count++;
                    free(read_buffer);
                } else {
                    printf("Read failed for fid=%d\n", fid);
                }
            }
            
        } else {
            // 3. TRUNCATE OPERATION (10%)
            if (num_files > 0) {
                int file_idx = rand() % num_files;
                int fid = files[file_idx].fid;
                size_t current_size = files[file_idx].current_size;
                
                // Truncate to a smaller size (remove 1-3 blocks)
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
                        files[file_idx].current_size = new_size;
                        blocks_allocated -= (current_blocks - new_blocks);
                    }
                }
            }
        }
    }
    
    // Final metric capture
    final_metrics = capture_metrics(total_write_time, total_read_time, 
                                  write_count, read_count);
                                  
    printf("\n=== FINAL TEST RESULTS ===\n");
    printf("Total operations completed: %d\n", NUM_OPERATIONS);
    printf("Created/Written: %d files\n", write_count);
    printf("Reads: %d, Truncates: %d\n", read_count, truncate_count);
    print_metrics(&final_metrics, "Final State");
    
    // Calculate Throughput (MB/s) for writes
    double total_data_bytes = blocks_allocated * BLOCK_SIZE;
    double total_data_mb = total_data_bytes / (1024.0 * 1024.0);
    double total_time_sec = total_write_time / 1000.0;
    double throughput = (total_time_sec > 0) ? (total_data_mb / total_time_sec) : 0.0;
    
    printf("Write Throughput: %.2f MB/s\n", throughput);
    
    // Export results (append mode)
    FILE *f = fopen("results/results_block_size_summary.csv", "a");
    if (!f) {
        perror("Failed to open summary CSV file");
    } else {
        // Only write header if file is empty
        if (ftell(f) == 0) {
            fprintf(f, "policy,block_size_bytes,total_capacity_mb,total_blocks,"
                       "files_created,avg_write_latency_ms,avg_read_latency_ms,"
                       "write_throughput_mbps,load_imbalance,metadata_kb\n");
        }
        
        fprintf(f, "%s,%d,%d,%zu,%d,%.6f,%.6f,%.6f,%.6f,%.2f\n",
                policy, 
                BLOCK_SIZE, 
                CONSTANT_TOTAL_CAPACITY / (1024 * 1024), // Total capacity in MB
                total_blocks,
                num_files,
                final_metrics.avg_write_latency_ms,
                final_metrics.avg_read_latency_ms,
                throughput,
                final_metrics.load_imbalance,
                final_metrics.metadata_bytes / 1024.0);
        
        fclose(f);
        printf("Summary results appended to results/results_block_size_summary.csv\n");
    }
    
    free(files);
    metadatanode_exit(1);
}

int main(int argc, char *argv[])
{
    // For this test, you should choose one or two allocation policies 
    // that you believe are best (e.g., 'leastloaded' and 'fileaware')
    const char *policies[] = { "rand", "rand", "sequential", "roundrobin", "leastloaded" }; //, "fileaware" };
    
    for (int i = 0; i < 5; i++) {
        workload_block_size_test(policies[i], DIST_WEB_REALISTIC);
    }

    return 0;
}
