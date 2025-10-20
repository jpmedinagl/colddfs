#ifndef METADATA_NODE_H
#define METADATA_NODE_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "bitmap.h"

#define LOGM(fmt, ...) \
    do { \
        printf("[MetadataNode] " fmt "\n", ##__VA_ARGS__); \
        fflush(stdout); \
    } while (0)

typedef struct DataNode DataNode;
typedef struct AllocPolicy AllocPolicy;

typedef enum {
    MDN_SUCCESS = 0,
    MDN_NO_SPACE,
    MDN_INVALID_BLOCK,
    MDN_FILE_DNE,
    MDN_FAIL
} MDNStatus;

typedef struct {
    int block_id;
    int data_node_id;
} BlockLocation;

typedef struct {
    int fid;
    char * filename;
    int num_blocks;
    BlockLocation * blocks;
} FileEntry;

typedef struct {
    int pid;
    int sock_fd;
} NodeConnection;

typedef struct {
    int fs_capacity;
    size_t num_blocks;
    bitmap_t *bitmap;
    size_t free_blocks;
    
    int num_files;
    FileEntry * files;

    int num_nodes;
    DataNode * nodes;
    NodeConnection * connections;

    AllocPolicy * policy;
} MetadataNode;

MDNStatus metadatanode_init(int num_dns, size_t capacity, const char *policy_name);

MDNStatus metadatanode_exit();

MDNStatus metadatanode_create_file(size_t file_size, int * fid);

MDNStatus metadatanode_find_file(const char * filename, int * fid);

MDNStatus metadatanode_read_file(int fid, void * buffer, size_t * file_size);

MDNStatus metadatanode_write_file(int fid, void * buffer, size_t buffer_size);

MDNStatus metadatanode_alloc_block(int * block_index, int * node_id);

MDNStatus metadatanode_read_block(int fid, int block_index, void * buffer);

MDNStatus metadatanode_write_block(int fid, int block_index, void * buffer);

MDNStatus metadatanode_end(void);

#endif // METADATA_NODE_H