#include "metric.h"
#include "datanode.h"

extern MetadataNode *md;

double get_time_ms()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (tv.tv_sec * 1000.0) + (tv.tv_usec / 1000.0);
}

void calculate_load_balance(double *imbalance, double *std_dev, 
                           int *max_blocks, int *min_blocks)
{
    *max_blocks = 0;
    *min_blocks = INT_MAX;
    double sum = 0;
    double sum_sq = 0;
    
    for (int i = 0; i < md->num_nodes; i++) {
        int blocks = md->blocks_per_node[i];
        if (blocks > *max_blocks) *max_blocks = blocks;
        if (blocks < *min_blocks) *min_blocks = blocks;
        sum += blocks;
        sum_sq += blocks * blocks;
    }
    
    double avg = sum / md->num_nodes;
    double variance = (sum_sq / md->num_nodes) - (avg * avg);
    *std_dev = sqrt(variance);
    
    // Coefficient of variation (normalized standard deviation)
    *imbalance = (avg > 0) ? (*std_dev / avg) : 0.0;
}

SystemMetrics capture_metrics(double write_time_ms, double read_time_ms,
                              int write_count, int read_count)
{
    SystemMetrics m = {0};
    
    m.timestamp_ms = get_time_ms();
    
    m.blocks_used = md->num_blocks - md->free_blocks;
    m.blocks_free = md->free_blocks;
    m.fill_percentage = (int)((m.blocks_used * 100) / md->num_blocks);
    
    calculate_load_balance(&m.load_imbalance, &m.load_std_dev,
                          &m.max_blocks_on_node, &m.min_blocks_on_node);
    
    m.total_write_time_ms = write_time_ms;
    m.total_read_time_ms = read_time_ms;
    m.write_count = write_count;
    m.read_count = read_count;
    m.avg_write_latency_ms = (write_count > 0) ? (write_time_ms / write_count) : 0;
    m.avg_read_latency_ms = (read_count > 0) ? (read_time_ms / read_count) : 0;
    
	m.num_files = md->num_files;
    
	m.metadata_bytes = sizeof(MetadataNode) + 
                       (md->num_files * sizeof(FileEntry)) +
                       (m.blocks_used * sizeof(int)); // + size of allocation logic
    
    return m;
}

void print_metrics(SystemMetrics *m, const char *label)
{
    printf("\n--- Metrics: %s ---\n", label);
    printf("Fill: %d%% (%zu/%zu blocks)\n", 
           m->fill_percentage, m->blocks_used, m->blocks_used + m->blocks_free);
    printf("Load Imbalance: %.4f (std_dev=%.2f)\n", 
           m->load_imbalance, m->load_std_dev);
    printf("  Min/Max blocks per node: %d / %d\n", 
           m->min_blocks_on_node, m->max_blocks_on_node);
    printf("Files: %d\n", m->num_files);
    printf("Avg Write Latency: %.3f ms (%d writes)\n", 
           m->avg_write_latency_ms, m->write_count);
    printf("Avg Read Latency: %.3f ms (%d reads)\n", 
           m->avg_read_latency_ms, m->read_count);
    printf("Metadata: %.2f KB\n", m->metadata_bytes / 1024.0);
}

void export_metrics_csv(const char *filename, SystemMetrics *metrics, 
                       int count, const char *policy_name) {
    FILE *f = fopen(filename, "w");
    if (!f) {
        perror("Failed to open CSV file");
        return;
    }
    
	fprintf(f, "policy,timestamp_ms,fill_pct,blocks_used,blocks_free,"
               "load_imbalance,load_std_dev,max_blocks,min_blocks,"
               "num_files,write_count,read_count,avg_write_latency_ms,"
               "avg_read_latency_ms,metadata_bytes\n");
    
    for (int i = 0; i < count; i++) {
        SystemMetrics *m = &metrics[i];
        fprintf(f, "%s,%.2f,%d,%zu,%zu,%.6f,%.4f,%d,%d,%d,%d,%d,%.6f,%.6f,%zu\n",
                policy_name,
                m->timestamp_ms,
                m->fill_percentage,
                m->blocks_used,
                m->blocks_free,
                m->load_imbalance,
                m->load_std_dev,
                m->max_blocks_on_node,
                m->min_blocks_on_node,
                m->num_files,
                m->write_count,
                m->read_count,
                m->avg_write_latency_ms,
                m->avg_read_latency_ms,
                m->metadata_bytes);
    }
    
    fclose(f);
    printf("Exported %d metrics to %s\n", count, filename);
}

size_t generate_file_size(FileDistribution dist) {
    int blocks;
    
    switch (dist) {
        case DIST_UNIFORM_SMALL:
            blocks = 1 + (rand() % 2); // 1-2 blocks
            break;
            
        case DIST_UNIFORM_LARGE:
            blocks = 8 + (rand() % 9); // 8-16 blocks
            break;
            
        case DIST_BIMODAL:
            if ((rand() % 100) < 80) {
                blocks = 1; // 80% small
            } else {
                blocks = 16; // 20% large
            }
            break;
            
        case DIST_WEB_REALISTIC:
            {
                int r = rand() % 100;
                if (r < 70) {
                    blocks = 1 + (rand() % 2); // 70% tiny (1-2)
                } else if (r < 90) {
                    blocks = 4 + (rand() % 5); // 20% medium (4-8)
                } else {
                    blocks = 16 + (rand() % 17); // 10% large (16-32)
                }
            }
            break;
            
        case DIST_VIDEO:
            blocks = 32 + (rand() % 97); // 32-128 blocks
            break;
            
        default:
            blocks = 1;
    }
    
    return blocks * BLOCK_SIZE;
}

const char* dist_to_string(FileDistribution dist) {
    switch (dist) {
        case DIST_UNIFORM_SMALL: return "uniform_small";
        case DIST_UNIFORM_LARGE: return "uniform_large";
        case DIST_BIMODAL: return "bimodal";
        case DIST_WEB_REALISTIC: return "web_realistic";
        case DIST_VIDEO: return "video";
        default: return "unknown";
    }
}

