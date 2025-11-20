#include <time.h>

#include "metric.h"
#include "datanode.h"
#include "metadatanode.h"

#define SEED 12345

void workload_fill_levels(const char *policy, int num_nodes, 
                         size_t total_capacity, FileDistribution dist)
{
	srand(SEED);

    printf("\n========================================\n");
    printf("WORKLOAD: Fill Levels\n");
    printf("Policy: %s, Nodes: %d, Distribution: %s\n", 
           policy, num_nodes, dist_to_string(dist));
    printf("========================================\n");
    
    metadatanode_init(num_nodes, total_capacity, policy);
    
    SystemMetrics metrics[10]; // One for each 10% increment
    int metric_idx = 0;
    
    double total_write_time = 0;
    int write_count = 0;

	size_t blocks_allocated = 0;
	size_t total_blocks = total_capacity / BLOCK_SIZE;
    
    // Fill to 90% in 10% increments
    for (int target_fill = 10; target_fill <= 90; target_fill += 10) {        
		printf("\nFilling to %d%%...\n", target_fill);
        
	    while ((blocks_allocated * 100) / total_blocks < target_fill) {
            // Generate file
            char filename[64];
            snprintf(filename, sizeof(filename), "file_%d.dat", write_count);
            
            size_t file_size = generate_file_size(dist);

			// prepare buffer before timing
			char buffer[file_size];
			for (size_t i = 0; i < file_size; i++) {
				buffer[i] = 'A' + (i % 26);
			}
			buffer[file_size - 1] = '\0';
			size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            
            // Time the operation
            double start = get_time_ms();
            
            int fid;
            MDNStatus status = metadatanode_create_file(filename, file_size, &fid);
            
            if (status != MDN_SUCCESS) {
                printf("Failed to create file (system full)\n");
                break;
            }

			metadatanode_write_file(fid, buffer, file_size);
            
            // Write some data
            // size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
            // for (size_t i = 0; i < blocks; i++) {
            //     char buffer[BLOCK_SIZE];
            //     memset(buffer, 'A' + (i % 26), BLOCK_SIZE);
            //     buffer[BLOCK_SIZE - 1] = '\0';
            //     metadatanode_write_block(fid, i, buffer);
            // }
            
            double end = get_time_ms();
            total_write_time += (end - start);
			blocks_allocated += blocks;
            write_count++;
			
			// if (write_count % 2 == 0) {
			//	int fid = rand() % write_count;
			//	int new_size = 4 * BLOCK_SIZE;
			//	metadatanode_truncate_file(fid, new_size);
			//}
		}
        
        // Capture metrics at this fill level
        metrics[metric_idx] = capture_metrics(total_write_time, 0, write_count, 0);
        print_metrics(&metrics[metric_idx], "Current State");
        metric_idx++;
    }
    
    // Export results
    char csv_filename[256];
    snprintf(csv_filename, sizeof(csv_filename), 
             "results/results_distribution_%s_%s_%dnodes.csv", 
             policy, dist_to_string(dist), num_nodes);
    export_metrics_csv(csv_filename, metrics, metric_idx, policy);
    
    metadatanode_exit(1);
}

int main(int argc, char *argv[])
{
	printf("========================================\n");
    printf("DFS Workload Suite\n");
    printf("========================================\n");

	size_t total_capacity = 1000 * BLOCK_SIZE;
	
	// POLICY
	// int nodes_arr[] = {2, 4, 8, 16, 32};
	// int node_size = sizeof(nodes_arr);
	
	const char *policies[] = { "sequential", "roundrobin", "leastloaded", "weightedroundrobin", "rand", "fileaware" };
	FileDistribution distributions[] = { DIST_UNIFORM_SMALL, DIST_UNIFORM_LARGE, 
                                           DIST_BIMODAL, DIST_WEB_REALISTIC, DIST_VIDEO };
	
	for (int d = 0; d < 5; d++) {
		for (int i = 0; i < 6; i++) {
			workload_fill_levels(policies[i], 8, total_capacity, distributions[d]);
		}
	}
	
	return 0;
}

