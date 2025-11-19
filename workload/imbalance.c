#include <time.h>
#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345

extern MetadataNode *md;

// Workload designed to create and expose load imbalances
// Strategy: Create files, then selectively truncate to unbalance nodes
void workload_imbalance_test(const char *policy, int num_nodes, 
                             size_t total_capacity, FileDistribution dist)
{
    srand(SEED);

    printf("\n========================================\n");
    printf("WORKLOAD: Imbalance Test\n");
    printf("Policy: %s, Nodes: %d, Distribution: %s\n", 
           policy, num_nodes, dist_to_string(dist));
    printf("========================================\n");
    
    metadatanode_init(num_nodes, total_capacity, policy);
    
    SystemMetrics metrics[30];
    int metric_idx = 0;
    
    double total_write_time = 0;
    double total_read_time = 0;
    int write_count = 0;
    int read_count = 0;
    
    // Track files and which nodes they're primarily on
    typedef struct {
        int fid;
        size_t current_size;
        int primary_node; // Node with first block
    } FileInfo;
    
    FileInfo *files = malloc(sizeof(FileInfo) * 1000);
    int num_files = 0;
    
    size_t blocks_allocated = 0;
    size_t total_blocks = total_capacity / BLOCK_SIZE;
    
    // PHASE 1: Fill to 60%
    printf("\n=== PHASE 1: Initial fill to 60%% ===\n");
    
    while ((blocks_allocated * 100) / total_blocks < 60) {
        char filename[64];
        snprintf(filename, sizeof(filename), "file_%d.dat", num_files);
        
        size_t file_size = generate_file_size(dist);
        size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        
        char *buffer = malloc(file_size);
        memset(buffer, 'A', file_size);
        
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
        
        // Determine which node has this file's first block
        int block_id = md->files[fid].blocks[0];
        int node_id = md->block_mapping[block_id];
        
        files[num_files].fid = fid;
        files[num_files].current_size = file_size;
        files[num_files].primary_node = node_id;
        num_files++;
        
        free(buffer);
    }
    
    metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                          write_count, read_count);
    print_metrics(&metrics[metric_idx], "After Initial Fill");
    metric_idx++;
    
    printf("Created %d files across %d nodes\n", num_files, num_nodes);
    
    // Print initial distribution
    printf("\nInitial blocks per node:\n");
    for (int i = 0; i < num_nodes; i++) {
        size_t node_capacity = md->fs_capacity / md->num_nodes;
        int max_blocks = node_capacity / BLOCK_SIZE;
        int used = max_blocks - md->blocks_free[i];
        printf("  Node %d: %d blocks\n", i, used);
    }
    
    // PHASE 2: Strategic truncation to create imbalance
    printf("\n=== PHASE 2: Creating imbalance through truncation ===\n");
    
    // Find the node with the most files
    int *files_per_node = calloc(num_nodes, sizeof(int));
    for (int i = 0; i < num_files; i++) {
        files_per_node[files[i].primary_node]++;
    }
    
    int busiest_node = 0;
    int max_files_on_node = 0;
    for (int i = 0; i < num_nodes; i++) {
        printf("Node %d has %d files\n", i, files_per_node[i]);
        if (files_per_node[i] > max_files_on_node) {
            max_files_on_node = files_per_node[i];
            busiest_node = i;
        }
    }
    
    printf("\nBusiest node: %d with %d files\n", busiest_node, max_files_on_node);
    
    // Strategy: Heavily truncate files on the busiest node
    int truncate_count = 0;
    for (int i = 0; i < num_files; i++) {
        if (files[i].primary_node == busiest_node) {
            int fid = files[i].fid;
            size_t current_size = files[i].current_size;
            size_t current_blocks = (current_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            // Shrink by 50-75%
            int blocks_to_remove = current_blocks * (50 + rand() % 25) / 100;
            if (blocks_to_remove < 1) blocks_to_remove = 1;
            if (blocks_to_remove >= current_blocks) {
                blocks_to_remove = current_blocks - 1;
            }
            
            size_t new_blocks = current_blocks - blocks_to_remove;
            size_t new_size = new_blocks * BLOCK_SIZE;
            
            double start = get_time_ms();
            MDNStatus status = metadatanode_truncate_file(fid, new_size);
            double end = get_time_ms();
            
            if (status == MDN_SUCCESS) {
                total_write_time += (end - start);
                blocks_allocated -= blocks_to_remove;
                files[i].current_size = new_size;
                truncate_count++;
            }
            
            // Only truncate some files
            if (truncate_count >= max_files_on_node / 2) {
                break;
            }
        }
    }
    
    printf("Truncated %d files on node %d\n", truncate_count, busiest_node);
    
    metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                          write_count, read_count);
    print_metrics(&metrics[metric_idx], "After Truncation");
    metric_idx++;
    
    printf("\nBlocks per node after truncation:\n");
    for (int i = 0; i < num_nodes; i++) {
        size_t node_capacity = md->fs_capacity / md->num_nodes;
        int max_blocks = node_capacity / BLOCK_SIZE;
        int used = max_blocks - md->blocks_free[i];
        printf("  Node %d: %d blocks (%d free)\n", i, used, md->blocks_free[i]);
    }
    
    // PHASE 3: Continue allocating - see how policy responds to imbalance
    printf("\n=== PHASE 3: Allocate more files (test rebalancing) ===\n");
    
    int new_files_added = 0;
    while (new_files_added < 50 && 
           (blocks_allocated * 100) / total_blocks < 80) {
        char filename[64];
        snprintf(filename, sizeof(filename), "newfile_%d.dat", new_files_added);
        
        size_t file_size = generate_file_size(dist);
        size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        
        char *buffer = malloc(file_size);
        memset(buffer, 'B', file_size);
        
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
        new_files_added++;
        
        free(buffer);
        
        // Capture metrics every 10 files
        if (new_files_added % 10 == 0) {
            metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                                  write_count, read_count);
            print_metrics(&metrics[metric_idx], "During Rebalancing");
            metric_idx++;
        }
    }
    
    printf("Added %d new files after creating imbalance\n", new_files_added);
    
    printf("\nFinal blocks per node:\n");
    for (int i = 0; i < num_nodes; i++) {
        size_t node_capacity = md->fs_capacity / md->num_nodes;
        int max_blocks = node_capacity / BLOCK_SIZE;
        int used = max_blocks - md->blocks_free[i];
        printf("  Node %d: %d blocks (%d free)\n", i, used, md->blocks_free[i]);
    }
    
    // Final metrics
    metrics[metric_idx] = capture_metrics(total_write_time, total_read_time, 
                                          write_count, read_count);
    print_metrics(&metrics[metric_idx], "Final State");
    metric_idx++;
    
    // Export results
    char csv_filename[256];
    snprintf(csv_filename, sizeof(csv_filename), 
             "results/results_imbalance_%s_%s_%dnodes.csv", 
             policy, dist_to_string(dist), num_nodes);
    export_metrics_csv(csv_filename, metrics, metric_idx, policy);
    
    free(files);
    free(files_per_node);
    metadatanode_exit(1);
}

int main(int argc, char *argv[])
{
    printf("========================================\n");
    printf("DFS Imbalance Workload\n");
    printf("========================================\n");

    size_t total_capacity = 1000 * BLOCK_SIZE;
    
    const char *policies[] = { "sequential", "roundrobin", "leastloaded", 
                              "weightedroundrobin", "rand", "fileaware" };
    
    for (int i = 0; i < 6; i++) {
        workload_imbalance_test(policies[i], 8, total_capacity, DIST_WEB_REALISTIC);
    }

    return 0;
}
