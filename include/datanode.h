#ifndef DATANODE_H
#define DATANODE_H

#define BLOCK_SIZE 4096

#define _XOPEN_SOURCE 500

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ftw.h>

#include "communication.h"

#define LOGD(node_id, fmt, ...) \
    do { \
        printf("[DataNode %d] " fmt "\n", node_id, ##__VA_ARGS__); \
        fflush(stdout); \
    } while (0)

typedef struct DataNode {
    int node_id;
    char dir_path[256];
    size_t capacity;
    size_t size;

    int sock_fd;
} DataNode;

// Initialize a data node, create its starting directory
DNStatus datanode_init(int sock_fd, void *payload, size_t payload_size);

DNStatus datanode_service_loop(int sock_fd);

// Allocate a block, this is for creating a file
DNStatus datanode_alloc_block(int block_index);

// Freeing blocks, might not be used in DFS
DNStatus datanode_free_block(int block_index);

// Reading a block from its index
DNStatus datanode_read_block(int block_index, void * buffer);

// Writing a block to its index
DNStatus datanode_write_block(int block_index, void * buffer);

DNStatus datanode_exit(int cleanup, int * sock_fd);

#endif
