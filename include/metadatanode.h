#ifndef METADATA_NODE_H
#define METADATA_NODE_H

#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/wait.h>

#include "bitmap.h"

#if LOG
	#define LOGM(fmt, ...) \
		do { \
			printf("[MetaDtNode] " fmt "\n", ##__VA_ARGS__); \
			fflush(stdout); \
		} while (0)
#else
	#define LOGM(fmt, ...) do {} while(0)
#endif

typedef struct DataNode DataNode;
typedef struct AllocPolicy AllocPolicy;
typedef struct AllocContext AllocContext;

typedef enum {
    MDN_SUCCESS = 0,
    MDN_NO_SPACE,
    MDN_INVALID_BLOCK,
    MDN_FILE_DNE,
    MDN_FAIL
} MDNStatus;

typedef struct {
	int num_blocks;
	int * logical_blocks;
	int * file_blocks;
} NodeBlock;

typedef struct {
    int fid;
    char * filename;
	// size_t file_size;
	int num_blocks;
	NodeBlock * nodes;
} FileEntry;

typedef struct {
    int pid;
    int sock_fd;
} NodeConnection;

typedef struct {
    int fs_capacity;
    size_t num_blocks;
    size_t free_blocks;
	bitmap_t *bitmap;
	int * block_mapping;

    int num_files;
    FileEntry * files;

    int num_nodes;
    DataNode * nodes;
    NodeConnection * connections;
	int * blocks_per_node;
	int * blocks_free;
} MetadataNode;

MDNStatus metadatanode_init(int num_dns, size_t capacity, const char *policy_name);

MDNStatus metadatanode_exit(int cleanup);

MDNStatus metadatanode_create_file(const char * filename, size_t file_size, int * fid);

MDNStatus metadatanode_find_file(const char * filename, int * fid);

MDNStatus metadatanode_truncate_file(int fid, size_t new_size);

MDNStatus metadatanode_read_file(int fid, void ** buffer, size_t * file_size);

MDNStatus metadatanode_write_file(int fid, void * buffer, size_t buffer_size);

MDNStatus metadatanode_alloc_block(AllocContext ctx, int * block_index, int * node_id);

MDNStatus metadatanode_dealloc_block(int block_index);

MDNStatus metadatanode_read_block(int fid, int file_index, void * buffer);

MDNStatus metadatanode_write_block(int fid, int file_index, void * buffer);

MDNStatus metadatanode_end(void);

#endif // METADATA_NODE_H
