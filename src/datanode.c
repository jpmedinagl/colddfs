#include "datanode.h"
#include "communication.h"

DataNode * dn = NULL;

DNStatus datanode_init(int sock_fd, void *payload, size_t payload_size)
{
    dn = malloc(sizeof(DataNode));
    dn->sock_fd = sock_fd;

    if (payload_size < sizeof(int) + sizeof(size_t)) {
        DNStatus status = DN_FAIL;
        dn_send_response(sock_fd, status, NULL, 0);
        free(dn);
        return DN_FAIL;
    }

    DNInitPayload *init = (DNInitPayload*)payload;
    dn->node_id = init->node_id;
    dn->capacity = init->capacity;

    LOGD(dn->node_id, "received node id=%d", dn->node_id);
    LOGD(dn->node_id, "received capacity=%zu", dn->capacity);

    snprintf(dn->dir_path, sizeof(dn->dir_path), "dn_%d", dn->node_id);
    mkdir(dn->dir_path, 0755);

    dn->size = 0;
    
    return DN_SUCCESS;
}

DNStatus datanode_alloc_block(int block_index)
{
    if (dn->size + BLOCK_SIZE > dn->capacity) {
        LOGD(dn->node_id, "no more space in this node");
        return DN_NO_SPACE;
    }

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/block_%d.dat", dn->dir_path, block_index);
    
    FILE *f = fopen(filepath, "wb");
    if (!f) {
        perror("fopen");
        return DN_FAIL;
    }
    fclose(f);

    dn->size += BLOCK_SIZE;
    
    LOGD(dn->node_id, "block with id=%d created", block_index);    
    return DN_SUCCESS;
}

DNStatus datanode_free_block(int block_index)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/block_%d.dat", dn->dir_path, block_index);

    if (unlink(filepath) != 0) {
        perror("unlink");
        return DN_FAIL;
    }

    dn->size -= BLOCK_SIZE;

    LOGD(dn->node_id, "block with id=%d deleted", block_index);
    return DN_SUCCESS;
}

DNStatus datanode_read_block(int block_index, void * buffer)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/block_%d.dat", dn->dir_path, block_index);

    FILE *f = fopen(filepath, "rb");
    if (!f) {
        perror("fopen");
        return DN_FAIL;
    }

    size_t read_bytes = fread(buffer, 1, BLOCK_SIZE, f);
    fclose(f);

    if (read_bytes != BLOCK_SIZE) {
        LOGD(dn->node_id, "incomplete read for block %d", block_index);
        return DN_FAIL;
    }

    LOGD(dn->node_id, "read block %d", block_index);
    return DN_SUCCESS;
}

DNStatus datanode_write_block(int block_index, void * buffer)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/block_%d.dat", dn->dir_path, block_index);

    FILE *f = fopen(filepath, "wb");
    if (!f) {
        perror("fopen");
        return DN_FAIL;
    }

    size_t written = fwrite(buffer, 1, BLOCK_SIZE, f);
    fclose(f);

    if (written != BLOCK_SIZE) {
        LOGD(dn->node_id, "incomplete write for block %d", block_index);
        return DN_FAIL;
    }

    LOGD(dn->node_id, "wrote block %d", block_index);
    return DN_SUCCESS;
}

DNStatus datanode_exit(int * sockfd)
{   
    *sockfd = dn->sock_fd;
    if (dn) {
        free(dn);
        dn = NULL;
    }
    return DN_SUCCESS;
}

DNStatus datanode_service_loop(int sock_fd)
{
    while (1) {
        DNCommand cmd;
        void *payload = NULL;
        size_t payload_size = 0;

        if (dn_recv_command(sock_fd, &cmd, &payload, &payload_size) == -1) {
            if (payload) free(payload);
            return DN_FAIL;
        }

        DNStatus status;

        switch(cmd) {
            case DN_INIT:
                status = datanode_init(sock_fd, payload, payload_size);
                dn_send_response(sock_fd, status, NULL, 0);
                break;
            case DN_ALLOC_BLOCK: {
                int block_index;
                memcpy(&block_index, payload, sizeof(int));
                status = datanode_alloc_block(block_index);
                dn_send_response(sock_fd, status, NULL, 0);
                break;
            }
            case DN_FREE_BLOCK: {
                if (payload_size >= sizeof(int)) {
                    int block_index;
                    memcpy(&block_index, payload, sizeof(int));
                    status = datanode_free_block(block_index);
                    dn_send_response(sock_fd, status, NULL, 0);
                } else {
                    dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                }
                break;
            }
            case DN_READ_BLOCK: {
                if (payload_size >= sizeof(int)) {
                    int block_index;
                    memcpy(&block_index, payload, sizeof(int));
                    char buffer[4096];
                    status = datanode_read_block(block_index, buffer);
                    dn_send_response(sock_fd, status, buffer, sizeof(buffer));
                } else {
                    dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                }
                break;
            }
            case DN_WRITE_BLOCK: {
                if (payload_size >= sizeof(int) + 4096) {
                    int block_index;
                    char buffer[4096];
                    memcpy(&block_index, payload, sizeof(int));
                    memcpy(buffer, (char*)payload + sizeof(int), 4096);
                    status = datanode_write_block(block_index, buffer);
                    dn_send_response(sock_fd, status, NULL, 0);
                } else {
                    dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                }
                break;
            }
            case DN_EXIT:
                int sockfd;
                status = datanode_exit(&sockfd);
                dn_send_response(sock_fd, status, NULL, 0);
                if (payload) free(payload);
                return DN_SUCCESS;
        }
        
        if (payload) free(payload);
    }
}
