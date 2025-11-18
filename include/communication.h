#ifndef COMMUNICATION_H
#define COMMUNICATION_H

#include <stddef.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

typedef enum {
    DN_INIT,
    DN_ALLOC_BLOCK,
    DN_FREE_BLOCK,
    DN_READ_BLOCK,
    DN_WRITE_BLOCK,
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
    char buffer[4096]; // for read/write
} DNBlockPayload;

typedef struct {
	int cleanup;
} DNExitPayload;

ssize_t send_all(int sock_fd, const void *buf, size_t len);

ssize_t recv_all(int sock_fd, void *buf, size_t len);

ssize_t md_send_command(int sock_fd, DNCommand cmd, void *payload, size_t payload_size);

ssize_t md_recv_response(int sock_fd, DNStatus *status, void **payload, size_t *payload_size);

ssize_t dn_recv_command(int sock_fd, DNCommand *cmd, void **payload, size_t *payload_size);

ssize_t dn_send_response(int sock_fd, DNStatus status, void *payload, size_t payload_size);

#endif // COMMUNICATION_H
