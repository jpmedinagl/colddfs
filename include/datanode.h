#ifndef DATANODE_H
#define DATANODE_H

#define BLOCK_SIZE 4096

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>

#define LOGD(node_id, fmt, ...) \
    do { \
        printf("[DataNode %d] " fmt "\n", node_id, ##__VA_ARGS__); \
        fflush(stdout); \
    } while (0)

typedef enum {
    DN_SUCCESS = 0,
    DN_NO_SPACE,
    DN_INVALID_BLOCK,
    DN_FAIL
} DNStatus;

typedef struct {
    int node_id;
    char dir_path[256];
    size_t capacity;
    size_t size;

    int sock_fd;
} DataNode;

// Initialize a data node, create its starting directory
DNStatus datanode_init(int sock_fd);

DNStatus datanode_service_loop();

// Allocate a block, this is for creating a file
DNStatus datanode_alloc_block(int *block_index);

// Freeing blocks, might not be used in DFS
DNStatus datanode_free_block(int block_index);

// Reading a block from its index
DNStatus datanode_read_block(int block_index, void * buffer);

// Writing a block to its index
DNStatus datanode_write_block(int block_index, void * buffer);

#endif