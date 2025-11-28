#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "datanode.h"
#include "metadatanode.h"

double get_time_ms() {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec * 1000.0 + ts.tv_nsec / 1000000.0;
}

int test_batch(int file_size, double *total_create, double *total_write, double *total_read) {
	int num_datanodes = 4;
	int num_blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    metadatanode_init(num_datanodes, file_size * 4, "roundrobin");

	double start, create_time, write_time, read_time;

    const char *filename = "test_file.txt";
    int fid;
    
    // Create file
	start = get_time_ms();
    metadatanode_create_file(filename, file_size, &fid);
	create_time = get_time_ms() - start;
	*total_create += create_time;	

	printf("Created file in %.2f ms (%.3f ms/block)\n", create_time, create_time / num_blocks);

    // Prepare write buffer
    char *write_buffer = malloc(file_size);
    for (int i = 0; i < num_blocks; i++) {
        char *block = write_buffer + i * BLOCK_SIZE;
        memset(block, 0, BLOCK_SIZE);
        snprintf(block, BLOCK_SIZE, "This is block number %d with some test data", i);
    }

    // Time write
    printf("\nWriting file...\n");
    start = get_time_ms();
    
    if (metadatanode_write_file(fid, write_buffer, file_size) != MDN_SUCCESS) {
        fprintf(stderr, "Failed to write file\n");
        return 1;
    }
    
    write_time = get_time_ms() - start;
	*total_write += write_time;

	printf("Write completed in %.2f ms (%.3f ms/block)\n", write_time, write_time / num_blocks);

    free(write_buffer);

    // Time read
    printf("\nReading file...\n");
    void *read_buffer;
    size_t read_size;
    
    start = get_time_ms();
    
    if (metadatanode_read_file(fid, &read_buffer, &read_size) != MDN_SUCCESS) {
        fprintf(stderr, "Failed to read file\n");
        return 1;
    }
    
    read_time = get_time_ms() - start;
	*total_read += read_time;

	printf("Read completed in %.2f ms (%.3f ms/block)\n", read_time, read_time / num_blocks);
    printf("Read %zu bytes\n", read_size);

    // Verify first and last block
    printf("\nVerifying data...\n");
    printf("First block: %.50s\n", (char *)read_buffer);
    printf("Last block:  %.50s\n", (char *)read_buffer + (num_blocks - 1) * BLOCK_SIZE);

    free(read_buffer);

	metadatanode_exit(1);

	printf("create=%.2f ms, write=%.2f ms, read=%.2f ms\n", create_time, write_time, read_time);
	return 0;
}

int main(void) {
    printf("File System Performance Test\n");
    printf("========================================\n\n");

	int runs = 1;

	double total_create = 0, total_write = 0, total_read = 0;
	int file_size = 4096 * 4096;
	
	for (int r = 0; r < runs; r++) {
		test_batch(file_size, &total_create, &total_write, &total_read);
	}
    
	FILE *fp = fopen("avg.csv", "a");
	if (!fp) {
		perror("fopen");
		return 1;
	}

	fprintf(fp, "%d,%.6f,%.6f,%.6f\n",
        BLOCK_SIZE,
        total_create / runs,
        total_write / runs,
        total_read) / runs;
	
	fclose(fp);
    
    return 0;
}



