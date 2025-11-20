#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "datanode.h"
#include "metadatanode.h"

#define NUM_TRIALS 5

// Get current time in milliseconds
double now_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1e6;
}

void test_write_latency_throughput(const char *filename, FILE *outfile) {
    int fid;
    size_t file_size = 17 * BLOCK_SIZE;

    if (metadatanode_create_file(filename, file_size, &fid) != MDN_SUCCESS) {
        fprintf(stderr, "Failed to create file '%s'\n", filename);
        return;
    }

    char *buffer = malloc(file_size);
    if (!buffer) {
        perror("malloc");
        return;
    }

    // fill buffer with pseudo-random data
    for (size_t i = 0; i < file_size; i++) {
        buffer[i] = (char)(i % 256);
    }

    printf("Writing file '%s' (%zu MB)\n", filename, file_size);

    for (int trial = 0; trial < NUM_TRIALS; trial++) {
        double start = now_ms();
        if (metadatanode_write_file(fid, buffer, file_size) != MDN_SUCCESS) {
            fprintf(stderr, "Write failed for trial %d\n", trial+1);
            free(buffer);
            return;
        }
        double end = now_ms();

        double duration_ms = end - start;
        double throughput_mbps = (file_size / 1024.0 / 1024.0) / (duration_ms / 1000.0);

        printf("Trial %d: Time = %.3f ms, Throughput = %.3f MB/s\n",
               trial + 1, duration_ms, throughput_mbps);

		if (outfile) {
            fprintf(outfile, "%s,%d,%.3f,%.3f\n", filename, trial + 1, duration_ms, throughput_mbps);
        }
    }

    free(buffer);
}

int main(void) {
    printf("Initializing Metadata Node...\n");
    if (metadatanode_init(4, 1024 * BLOCK_SIZE, "roundrobin") != MDN_SUCCESS) {
        fprintf(stderr, "Failed to init metadata node\n");
        return 1;
    }

    FILE *outfile = fopen("write_results.csv", "w");
    if (!outfile) {
        perror("fopen");
        return 1;
    }

	fprintf(outfile, "filename,trial,write_time_ms,throughput_MBps\n");

	test_write_latency_throughput("pre_allocate_test", outfile);
    // test_write_latency_throughput("post_allocate_test");
	
	fclose(outfile);

    metadatanode_exit(1);
    printf("Test complete.\n");

    return 0;
}

