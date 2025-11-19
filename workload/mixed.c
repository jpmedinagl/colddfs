#include <time.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345

// Track created files for read/truncate operations
typedef struct {
    int fid;
    size_t current_size;
    int num_blocks;
} FileTracker;

// Workload: Mixed operations (write, read, truncate) to test real-world patterns
void workload_mixed_operations(const char *policy, int num_nodes, 
                               size_t total_capacity, FileDistribution dist)
{
    srand(SEED);

    printf("\n========================================\n");
    printf("WORKLOAD: Mixed Operations\n");
    printf("Policy: %s, Nodes: %d, Distribution: %s\n", 
           policy, num_nodes, dist_to_string(dist));
    printf("========================================\n");
    
    metadatanode_init(num_nodes, total_capacity, policy);
    
    SystemMetrics metrics[10];
    int metric_idx = 0;
    
    double total_write_time = 0;
    double total_read_time = 0;
    int write_count = 0;
    int read_count = 0;
    int truncate_count = 0;
    
    // Track files for operations
    FileTracker *files = malloc(sizeof(FileTracker) * 1000);
    int num_files = 0;
    
    size_t blocks_allocated = 0;
    size_t total_blocks = total_capacity / BLOCK_SIZE;
    
    int operation_num = 0;
    
    // Fill to 90% with mixed operations
    for (int target_fill = 10; target_fill <= 90; target_fill += 10) {
        printf("\nFilling to %d%%...\n", target_fill);
        
        while ((blocks_allocated * 100) / total_blocks < target_fill) {
            operation_num++;
            
            // Operation distribution:
            // 60% writes, 30% reads, 10% truncates
            int op_type = rand() % 100;
            
            if (op_type < 60 || num_files == 0) {
                // WRITE OPERATION
                char filename[64];
                snprintf(filename, sizeof(filename), "file_%d.dat", num_files);
                
                size_t file_size = generate_file_size(dist);
                size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
                
                char *buffer = malloc(file_size);
                for (size_t i = 0; i < file_size; i++) {
                    buffer[i] = 'A' + (i % 26);
                }
                buffer[file_size - 1] = '\0';
                
                double start = get_time_ms();
                
                int fid;
                MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
                
                if (status != MDN_SUCCESS) {
                    printf("Failed to create file (system full)\n");
                    free(buffer);
                    break;
                }
                
                metadatanode_write_file(fid, buffer, file_size);
                
                double end = get_time_ms();
                total_write_time += (end - start);
                write_count++;
                blocks_allocated += blocks;
                
                // Track this file
                files[num_files].fid = fid;
                files[num_files].current_size = file_size;
                files[num_files].num_blocks = blocks;
                num_files++;
                
                free(buffer);
                
            } else if (op_type < 90) {
                // READ OPERATION
                int file_idx = rand() % num_files;
                int fid = files[file_idx].fid;
                
                double start = get_time_ms();
                
                void *buffer;
                size_t size;
                MDNStatus status = metadatanode_read_file(fid, &buffer, &size);
                
                double end = get_time_ms();
                
                if (status == MDN_SUCCESS) {
                    total_read_time += (end - start);
                    read_count++;
                    free(buffer);
                } else {
                    printf("Read failed for fid=%d\n", fid);
                }
                
            } else {
                // TRUNCATE OPERATION (10% of ops)
                int file_idx = rand() % num_files;
                int fid = files[file_idx].fid;
                size_t current_size = files[file_idx].current_size;
                int current_blocks = files[file_idx].num_blocks;
                
                // 70% shrink, 30% grow
                size_t new_size;
                int new_blocks;
                
                if (rand() % 100 < 70) {
                    // Shrink file (remove 1-3 blocks)
                    int blocks_to_remove = 1 + (rand() % 3);
                    if (blocks_to_remove >= current_blocks) {
                        blocks_to_remove = current_blocks / 2; // Don't remove everything
                    }
                    new_blocks = current_blocks - blocks_to_remove;
                    new_size = new_blocks * BLOCK_SIZE;
                    
                    blocks_allocated -= blocks_to_remove;
                } else {
                    // Grow file (add 1-2 blocks)
                    int blocks_to_add = 1 + (rand() % 2);
                    new_blocks = current_blocks + blocks_to_add;
                    new_size = new_blocks * BLOCK_SIZE;
                    
                    blocks_allocated += blocks_to_add;
                }
                
                double start = get_time_ms();
                MDNStatus status = metadatanode_truncate_file(fid, new_size);
                double end = get_time_ms();
                
                if (status == MDN_SUCCESS) {
                    total_write_time += (end - start); // Count as write time
                    truncate_count++;
                    files[file_idx].current_size = new_size;
                    files[file_idx].num_blocks = new_blocks;
                } else {
                    printf("Truncate failed for fid=%d\n", fid);
                    // Rollback blocks_allocated change
                    if (new_blocks > current_blocks) {
                        blocks_allocated -= (new_blocks - current_blocks);
                    } else {
                        blocks_allocated += (current_blocks - new_blocks);
                    }
                }
            }
        }
        
        // Capture metrics at this fill level
        metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                              write_count, read_count);
        printf("Operations: %d writes, %d reads, %d truncates\n", 
               write_count, read_count, truncate_count);
        print_metrics(&metrics[metric_idx], "Current State");
        metric_idx++;
    }
    
    // Export results
    char csv_filename[256];
    snprintf(csv_filename, sizeof(csv_filename), 
             "results/results_mixed_%s_%s_%dnodes.csv", 
             policy, dist_to_string(dist), num_nodes);
    export_metrics_csv(csv_filename, metrics, metric_idx, policy);
    
    free(files);
    metadatanode_exit(1);
    
    printf("\nFinal stats: %d writes, %d reads, %d truncates\n",
           write_count, read_count, truncate_count);
}

int main(int argc, char *argv[])
{
    printf("========================================\n");
    printf("DFS Mixed Workload Suite\n");
    printf("========================================\n");

    size_t total_capacity = 1000 * BLOCK_SIZE;
    
    const char *policies[] = { "sequential", "roundrobin", "leastloaded", 
                              "weightedroundrobin", "rand", "fileaware" };
    
    for (int i = 0; i < 6; i++) {
        workload_mixed_operations(policies[i], 8, total_capacity, DIST_WEB_REALISTIC);
    }

    return 0;
}
