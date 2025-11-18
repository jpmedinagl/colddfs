#include <stdio.h>
#include <stddef.h>
#include <limits.h>
#include <math.h>
#include <sys/time.h>

#include "metadatanode.h"

typedef enum {
    DIST_UNIFORM_SMALL,    // 1-2 blocks
    DIST_UNIFORM_LARGE,    // 8-16 blocks
    DIST_BIMODAL,          // 80% small (1 block), 20% large (16 blocks)
    DIST_WEB_REALISTIC,    // 70% tiny (1-2), 20% medium (4-8), 10% large (16-32)
    DIST_VIDEO            // 32-128 blocks
} FileDistribution;

typedef struct {
	double timestamp_ms;
    
	int fill_percentage;
    size_t blocks_used;
    size_t blocks_free;
    
	double load_imbalance;
    double load_std_dev;
    int max_blocks_on_node;
    int min_blocks_on_node;
    
	double total_write_time_ms;
    double total_read_time_ms;
    int write_count;
    int read_count;
    double avg_write_latency_ms;
    double avg_read_latency_ms;
    
	int num_files;
    
	size_t metadata_bytes;
} SystemMetrics;

double get_time_ms();

void calculate_load_balance(double *imbalance, double *std_dev, int *max_blocks, int *min_blocks);

SystemMetrics capture_metrics(double write_time_ms, double read_time_ms, int write_count, int read_count);

void print_metrics(SystemMetrics *m, const char *label);

void export_metrics_csv(const char *filename, SystemMetrics *metrics, int count, const char *policy_name);

size_t generate_file_size(FileDistribution dist);

const char* dist_to_string(FileDistribution dist);
