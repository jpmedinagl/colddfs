#include "datanode.h"
#include "communication.h"

DataNode * dn = NULL;

DNStatus datanode_init(int sock_fd, void *payload, size_t payload_size)
{
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

    LOGD(dn->node_id, "received node id=%d capacity=%zu", dn->node_id, dn->capacity);

    snprintf(dn->dir_path, sizeof(dn->dir_path), "dn_%d", dn->node_id);
    mkdir(dn->dir_path, 0755);

    dn->size = 0;
    
    return DN_SUCCESS;
}

DNStatus datanode_alloc_block(int block_index)
{
    LOGD(dn->node_id, "Allocating block %d (current size=%zu, capacity=%zu)", block_index, dn->size, dn->capacity);

    if (dn->size + BLOCK_SIZE > dn->capacity) {
        LOGD(dn->node_id, "ERROR: No space for block %d (would exceed capacity)", block_index);
        return DN_NO_SPACE;
    }

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/block_%d.dat", dn->dir_path, block_index);
    
    int fd = open(filepath, O_CREAT | O_WRONLY, 0644);
    if (!fd) {
        LOGD(dn->node_id, "ERROR: Failed to create block file '%s'", filepath);
        perror("open");
        return DN_FAIL;
    }

    if (ftruncate(fd, BLOCK_SIZE) == -1) {
        perror("ftruncate");
        close(fd);
        return DN_FAIL;
    }

    close(fd);

    dn->size += BLOCK_SIZE;

    LOGD(dn->node_id, "Block %d created successfully (new size=%zu)", block_index, dn->size);
    
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

    int fd = open(filepath, O_RDONLY);
    if (fd < 0) {
        perror("open");
        return DN_FAIL;
    }

    ssize_t read_bytes = read(fd, buffer, BLOCK_SIZE);
    close(fd);

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

static int remove_callback(const char *fpath, const struct stat *sb,
                           int typeflag, struct FTW *ftwbuf)
{
    int ret = remove(fpath);
    if (ret) perror(fpath);
    return ret;
}

DNStatus datanode_exit(int cleanup, int * sockfd)
{   
    *sockfd = dn->sock_fd;
    
	if (cleanup) {
		if (dn->dir_path[0] != '\0') {
			if (nftw(dn->dir_path, remove_callback, 64, FTW_DEPTH | FTW_PHYS) != 0) {
				perror("nftw failed");
			}
		}
	}

	if (dn) {
        free(dn);
        dn = NULL;
    }	
    return DN_SUCCESS;
}

DNStatus datanode_batch_read(int *logical_blocks, int num_blocks, void *buffer)
{	
	DNStatus status = DN_SUCCESS;
	for (int i = 0; i < num_blocks; i++) {
		int logical_block = logical_blocks[i];
		void *block_dest = (char *)buffer + i * BLOCK_SIZE;

		status = datanode_read_block(logical_block, block_dest);
		if (status != DN_SUCCESS) {
			return status;
		}
	}
	return status;
}

DNStatus datanode_service_loop(int sock_fd)
{
    dn = malloc(sizeof(DataNode));
    memset(dn, 0, sizeof(DataNode));

    while (1) {
        DNCommand cmd = DN_EXIT;
        void *payload = NULL;
        size_t payload_size = 0;

        if (dn_recv_command(sock_fd, &cmd, &payload, &payload_size) == -1) {
            if (payload) free(payload);
            return DN_FAIL;
        }

        LOGD(dn->node_id, "Command %d", cmd);

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
                    
                    void *buffer = malloc(BLOCK_SIZE);
                    if (!buffer) {
                        dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                        break;
                    }

                    LOGD(dn->node_id, "Received read request for block %d", block_index);

                    status = datanode_read_block(block_index, buffer);
                    LOGD(dn->node_id, "Block %d read %s",
                        block_index, status == DN_SUCCESS ? "succeeded" : "failed");

                    LOGD(dn->node_id, "%s", (char *)buffer);
                    
                    dn_send_response(sock_fd, status, buffer, BLOCK_SIZE);
                    free(buffer);
                } else {
                    dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                }
                break;
            }
            case DN_WRITE_BLOCK: {
                if (payload_size >= sizeof(int) + BLOCK_SIZE) {
                    DNBlockPayload *p = (DNBlockPayload *)payload;
                    int block_index = p->block_index;

                    void *buffer = malloc(BLOCK_SIZE);
                    if (!buffer) {
                        dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                        break;
                    }

                    memcpy(buffer, p->buffer, BLOCK_SIZE);

                    LOGD(dn->node_id, "Received write request for block %d", block_index);
                    status = datanode_write_block(block_index, buffer);
                    LOGD(dn->node_id, "Block %d write %s",
                        block_index, status == DN_SUCCESS ? "succeeded" : "failed");

                    dn_send_response(sock_fd, status, NULL, 0);
                    free(buffer);
                } else {
                    dn_send_response(sock_fd, DN_FAIL, NULL, 0);
                }
                break;
            }
			case DN_BATCH_READ: {
				DNBatchPayload *p = (DNBatchPayload *)payload;
				int num_blocks = p->num_blocks;
				int *logical_blocks = p->logical_blocks;
				
				size_t response_size = num_blocks * BLOCK_SIZE;
				void *response_buffer = malloc(response_size);
				if (!response_buffer) {
					dn_send_response(sock_fd, DN_FAIL, NULL, 0);
					break;
				}

				DNStatus status = datanode_batch_read(logical_blocks, num_blocks, response_buffer);

				if (status == DN_SUCCESS) {
					dn_send_response(sock_fd, status, response_buffer, response_size);
				} else {
					dn_send_response(sock_fd, DN_FAIL, NULL, 0);
				}

				free(response_buffer);
				break;
			}
			case DN_BATCH_WRITE: {
				DNBatchPayload *p = (DNBatchPayload *)payload;
    
				// Receive block data
				size_t data_size = p->num_blocks * BLOCK_SIZE;
				void *data_buffer = malloc(data_size);
				dn_recv_data(sock_fd, data_buffer, data_size);
    
				// Write each block
				for (int i = 0; i < p->num_blocks; i++) {
					void *block_data = (char *)data_buffer + i * BLOCK_SIZE;
					datanode_write_block(p->logical_blocks[i], block_data);
				}
    
				free(data_buffer);
				dn_send_response(sock_fd, DN_SUCCESS, NULL, 0);
				break;
			}
			case DN_EXIT:
				DNExitPayload *p = (DNExitPayload *)payload;

                int sockfd;
                status = datanode_exit(p->cleanup, &sockfd);
                dn_send_response(sock_fd, status, NULL, 0);
                if (payload) free(payload);
                return DN_SUCCESS;
        }
        
        if (payload) free(payload);
    }
}
