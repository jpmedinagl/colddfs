#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "datanode.h"
#include "metadatanode.h"
#include "metric.h"

// Set tuning parameter for this specific test
#define SMALL_FILE_THRESHOLD 128

// Include the policy implementation here, redefining the parameter
#include "../src/fileaware.c"

#define CONSTANT_TOTAL_CAPACITY (1024 * 1024 * 16)

// Run the core distribution test logic with fixed parameters
void run_fileaware_test(FileDistribution dist) {
    srand(12345);
    const char *policy_name = "fileaware";
    
    printf("\n--- FILE AWARE TUNING: Threshold=%d, Distribution=%s ---\n", 
           SMALL_FILE_THRESHOLD, dist_to_string(dist));

    // Initialize with a standard capacity and node count
	int num_nodes = 8;
    metadatanode_init(num_nodes, CONSTANT_TOTAL_CAPACITY, policy_name);

    double total_write_time = 0;
    int write_count = 0;
    size_t blocks_allocated = 0;
    size_t total_blocks = CONSTANT_TOTAL_CAPACITY / BLOCK_SIZE;

    // Run until 50% capacity is reached
    while ((blocks_allocated * 100) / total_blocks < 50) {
        char filename[64];
        snprintf(filename, sizeof(filename), "file_%d.dat", write_count);
        
        size_t file_size = generate_file_size(dist);
        size_t blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
        
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
        blocks_allocated += blocks;
        
        free(buffer);
    }

    SystemMetrics m = capture_metrics(total_write_time, 0, write_count, 0);
    printf("Result for Threshold=%d:\n", SMALL_FILE_THRESHOLD);
    print_metrics(&m, "Final State");

	const char* dist_name = dist_to_string(dist);
    
	FILE *f = fopen("results/results_fileaware_tuning_summary.csv", "a");
    if (!f) {
        perror("Failed to open summary CSV file");
    } else {
        // Write header only if file is empty
        if (ftell(f) == 0) {
            fprintf(f, "policy,num_nodes,distribution,block_size_bytes,total_capacity_mb,"
                       "avg_write_latency_ms,avg_read_latency_ms,metadata_kb\n");
        }
        
        fprintf(f, "%s,%d,%d,%s,%d,%d,%.6f,%.6f,%.2f\n",
                policy_name,
				SMALL_FILE_THRESHOLD,
                num_nodes,
                dist_name,
                BLOCK_SIZE, 
                CONSTANT_TOTAL_CAPACITY / (1024 * 1024), 
                m.avg_write_latency_ms,
				m.avg_read_latency_ms,
                m.metadata_bytes / 1024.0);
        
        fclose(f);
        printf("Summary results appended to results/results_fileaware_tuning_summary.csv\n");
    }

    metadatanode_exit(1);
}

int main(void) {
    // We are testing the policy on a large file workload (Uniform Large)
    // and a balanced workload (Web Realistic) to see how the threshold changes behavior.
    
    // run_fileaware_test(DIST_WEB_REALISTIC);
    run_fileaware_test(DIST_UNIFORM_LARGE);
    
    return 0;
}
