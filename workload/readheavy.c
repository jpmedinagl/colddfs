#include <time.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345

// Workload: Read-heavy (typical web server pattern)
// Phase 1: Create files to 50% capacity
// Phase 2: Heavy reads (90% reads, 10% writes)
void workload_read_heavy(const char *policy, int num_nodes, 
                        size_t total_capacity, FileDistribution dist)
{
    srand(SEED);

    printf("\n========================================\n");
    printf("WORKLOAD: Read-Heavy\n");
    printf("Policy: %s, Nodes: %d, Distribution: %s\n", 
           policy, num_nodes, dist_to_string(dist));
    printf("========================================\n");
    
    metadatanode_init(num_nodes, total_capacity, policy);
    
    SystemMetrics metrics[20]; // More granular metrics
    int metric_idx = 0;
    
    double total_write_time = 0;
    double total_read_time = 0;
    int write_count = 0;
    int read_count = 0;
    
    size_t blocks_allocated = 0;
    size_t total_blocks = total_capacity / BLOCK_SIZE;
    
    // PHASE 1: Populate system to 50%
    printf("\n=== PHASE 1: Populating to 50%% ===\n");
    
    int *file_ids = malloc(sizeof(int) * 1000);
    int num_files = 0;
    
    while ((blocks_allocated * 100) / total_blocks < 50) {
        char filename[64];
        snprintf(filename, sizeof(filename), "file_%d.dat", num_files);
        
        size_t file_size = generate_file_size(dist);
        size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        
        char *buffer = malloc(file_size);
        for (size_t i = 0; i < file_size; i++) {
            buffer[i] = 'A' + (i % 26);
        }
        
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
        
        file_ids[num_files] = fid;
        num_files++;
        
        free(buffer);
    }
    
    printf("Created %d files, system at %zu%%\n", 
           num_files, (blocks_allocated * 100) / total_blocks);
    
    // Capture metrics after population
    metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                          write_count, read_count);
    print_metrics(&metrics[metric_idx], "After Population");
    metric_idx++;
    
    // PHASE 2: Read-heavy workload (90% reads, 10% writes)
    printf("\n=== PHASE 2: Read-Heavy Operations ===\n");
    
    const int NUM_OPERATIONS = 500;
    
    for (int i = 0; i < NUM_OPERATIONS; i++) {
        int op_type = rand() % 100;
        
        if (op_type < 90) {
            // READ OPERATION (90%)
            int file_idx = rand() % num_files;
            int fid = file_ids[file_idx];
            
            double start = get_time_ms();
            
            void *buffer;
            size_t size;
            MDNStatus status = metadatanode_read_file(fid, &buffer, &size);
            
            double end = get_time_ms();
            
            if (status == MDN_SUCCESS) {
                total_read_time += (end - start);
                read_count++;
                free(buffer);
            }
            
        } else {
            // WRITE OPERATION (10%)
            char filename[64];
            snprintf(filename, sizeof(filename), "new_file_%d.dat", write_count);
            
            size_t file_size = generate_file_size(dist);
            size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            char *buffer = malloc(file_size);
            for (size_t j = 0; j < file_size; j++) {
                buffer[j] = 'X';
            }
            
            double start = get_time_ms();
            
            int fid;
            MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
            
            if (status == MDN_SUCCESS) {
                metadatanode_write_file(fid, buffer, file_size);
                blocks_allocated += blocks;
                write_count++;
                
                // Add to file list
                if (num_files < 1000) {
                    file_ids[num_files++] = fid;
                }
            }
            
            double end = get_time_ms();
            total_write_time += (end - start);
            
            free(buffer);
        }
        
        // Capture metrics every 50 operations
        if ((i + 1) % 50 == 0) {
            metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                                  write_count, read_count);
            printf("After %d operations: %d reads, %d writes\n", 
                   i + 1, read_count, write_count);
            print_metrics(&metrics[metric_idx], "Current State");
            metric_idx++;
        }
    }
    
    printf("\n=== FINAL STATS ===\n");
    printf("Total operations: %d\n", NUM_OPERATIONS);
    printf("Reads: %d (%.1f%%)\n", read_count, 100.0 * read_count / NUM_OPERATIONS);
    printf("Writes: %d (%.1f%%)\n", write_count - num_files, 
           100.0 * (write_count - num_files) / NUM_OPERATIONS);
    
    // Export results
    char csv_filename[256];
    snprintf(csv_filename, sizeof(csv_filename), 
             "results/results_readheavy_%s_%s_%dnodes.csv", 
             policy, dist_to_string(dist), num_nodes);
    export_metrics_csv(csv_filename, metrics, metric_idx, policy);
    
    free(file_ids);
    metadatanode_exit(1);
}

int main(int argc, char *argv[])
{
    printf("========================================\n");
    printf("DFS Read-Heavy Workload\n");
    printf("========================================\n");

    size_t total_capacity = 1000 * BLOCK_SIZE;
    
    const char *policies[] = { "sequential", "roundrobin", "leastloaded", 
                              "weightedroundrobin", "rand", "fileaware" };
    
    for (int i = 0; i < 6; i++) {
        workload_read_heavy(policies[i], 8, total_capacity, DIST_WEB_REALISTIC);
    }

    return 0;
}
