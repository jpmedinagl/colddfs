#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#define BLOCK_SIZE (1 * 4096) // 2097152 // 1048576 // 524288 // 262144 // 131072 // 65536 // 32768 // 16384 // 8192 // 4096

typedef enum {
    DN_INIT,
    DN_ALLOC_BLOCK,
    DN_FREE_BLOCK,
    DN_READ_BLOCK,
    DN_WRITE_BLOCK,
	DN_BATCH_READ,
	DN_BATCH_WRITE,
    DN_EXIT,
} DNCommand;

typedef enum {
    DN_SUCCESS = 0,
    DN_NO_SPACE,
    DN_INVALID_BLOCK,
    DN_FAIL
} DNStatus;

typedef struct {
    DNCommand cmd;
    size_t payload_size;
} DNHeader;

typedef struct {
    DNStatus status;
    size_t payload_size;
} DNResponseHeader;

typedef struct {
    int node_id;
    size_t capacity;
} DNInitPayload;

typedef struct {
    int block_index;
} DNBlockIndexPayload;

typedef struct {
    int block_index;
    char buffer[BLOCK_SIZE]; // for read/write
} DNBlockPayload;

typedef struct {
	int num_blocks;
	int logical_blocks[];
} DNBatchPayload;

typedef struct {
	int cleanup;
} DNExitPayload;

ssize_t send_all(int sock_fd, const void *buf, size_t len);

ssize_t recv_all(int sock_fd, void *buf, size_t len);

ssize_t md_send_command(int sock_fd, DNCommand cmd, void *payload, size_t payload_size);

ssize_t md_recv_response(int sock_fd, DNStatus *status, void **payload, size_t *payload_size);

ssize_t dn_recv_command(int sock_fd, DNCommand *cmd, void **payload, size_t *payload_size);

ssize_t dn_send_response(int sock_fd, DNStatus status, void *payload, size_t payload_size);

ssize_t md_send_data(int sock_fd, void *data, size_t data_size);

ssize_t dn_recv_data(int sock_fd, void *buffer, size_t expected_size);

#endif // COMMUNICATION_H
