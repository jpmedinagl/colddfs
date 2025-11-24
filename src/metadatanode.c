#include "metadatanode.h"
#include "datanode.h"
#include "allocationpolicy.h"

MetadataNode * md = NULL;

MDNStatus initialize_datanodes()
{
	int total_blocks = md->num_blocks;

	int base = total_blocks / md->num_nodes;
    int rem  = total_blocks % md->num_nodes;

    LOGM("Initializing %d data nodes", md->num_nodes);
    for (int i = 0; i < md->num_nodes; i++) {
		int blocks_for_node = base + (i < rem ? 1 : 0);
		md->blocks_per_node[i] = blocks_for_node;
        md->blocks_free[i] = blocks_for_node;

        int fds[2];
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            perror("socketpair failed");
            return MDN_FAIL;
        }

        pid_t pid = fork();
        if (pid < 0) {
            perror("fork failed");
            return MDN_FAIL;
        }

        if (pid == 0) {
            // child
            close(fds[0]);
            
            metadatanode_end();
            
            datanode_service_loop(fds[1]);
            exit(0);
        } else {
            // parent
            close(fds[1]);
            md->connections[i].pid = pid;
            md->connections[i].sock_fd = fds[0];
        }
    }

    return MDN_SUCCESS;
}

MDNStatus connect_datanodes()
{
    LOGM("===================================================================");

    LOGM("Connecting %d data nodes", md->num_nodes);
    for (int i = 0; i < md->num_nodes; i++) {
        DNInitPayload payload = {0};
        payload.node_id = i;
        payload.capacity= md->blocks_per_node[i] * BLOCK_SIZE;
        
        if (md_send_command(md->connections[i].sock_fd, DN_INIT, &payload, sizeof(payload)) != 0) {
            perror("Failed to send DN_INIT");
            return MDN_FAIL;
        }
        
        DNStatus status;
        void *response_payload = NULL;
        size_t response_size = 0;

        if (md_recv_response(md->connections[i].sock_fd, &status, &response_payload, &response_size) != 0) {
            perror("Failed to receive DN_INIT response");
            return MDN_FAIL;
        }

        if (status != DN_SUCCESS) {
            fprintf(stderr, "Datanode %i failed to initialize.\n", i);
            return MDN_FAIL;
        }

        LOGM("Datanode %d connected", i);
    }

    LOGM("===================================================================\n");
    
    return MDN_SUCCESS;
}

MDNStatus metadatanode_init(int num_dns, size_t capacity, const char *policy_name)
{   
    LOGM("===================================================================");
    LOGM("=== Initializing MetadataNode ===");
    LOGM("Configuration:");
    LOGM("  - Data nodes: %d", num_dns);
    LOGM("  - Total capacity: %zu bytes", capacity);
    LOGM("  - Block size: %d bytes", BLOCK_SIZE);
    LOGM("  - Total blocks: %zu", (capacity + BLOCK_SIZE - 1) / BLOCK_SIZE);
    LOGM("  - Allocation policy: %s", policy_name);

    md = malloc(sizeof(MetadataNode));

    md->fs_capacity = capacity;
    md->num_blocks = (capacity + BLOCK_SIZE - 1) / BLOCK_SIZE;
	md->free_blocks = md->num_blocks;

    size_t bits_per_word = sizeof(size_t) * CHAR_BIT;
    uint32_t nwords = (md->num_blocks + bits_per_word - 1) / bits_per_word;
    md->bitmap = malloc(sizeof(bitmap_t) * nwords);
    bitmap_init(md->bitmap, md->num_blocks);

	md->block_mapping = malloc(sizeof(int) * md->num_blocks);
	
	md->num_files = 0;
    md->files = NULL;

    md->num_nodes = num_dns;
    md->nodes = malloc(md->num_nodes * sizeof(DataNode));
    if (!md->nodes) return MDN_FAIL;

	md->connections = malloc(md->num_nodes * sizeof(NodeConnection));
    if (!md->connections) return MDN_FAIL;
	
	md->blocks_per_node = malloc(sizeof(int) * md->num_nodes);
	md->blocks_free = malloc(sizeof(int) * md->num_nodes);

    if (!alloc_policy_init(policy_name)) {
        LOGM("ERROR: Failed to initialize allocation policy '%s'", policy_name);
        return MDN_FAIL;
    }
    LOGM("Allocation policy '%s' initialized", policy_name);
    
    MDNStatus status;
    status = initialize_datanodes();
    if (status != MDN_SUCCESS) {
        LOGM("Could not initialize datanodes");
        return status;
    }

    LOGM("=== MetadataNode Initialization Complete ===");
    LOGM("===================================================================\n");
    
    return connect_datanodes();
}

MDNStatus metadatanode_exit(int cleanup)
{
    if (!md || !md->connections) return MDN_FAIL;
    
    LOGM("===================================================================");
    LOGM("Exiting");

    for (int i = 0; i < md->num_nodes; i++) {
        pid_t pid = md->connections[i].pid;
        if (pid > 0) {
            DNCommand cmd = DN_EXIT;
			
			DNExitPayload payload;
			payload.cleanup = cleanup;

            if (md_send_command(md->connections[i].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
                perror("Failed to send DN_EXIT");
                return MDN_FAIL;
            }

            DNStatus status;
            void *response_payload = NULL;
            size_t response_size = 0;
            
            if (md_recv_response(md->connections[i].sock_fd, &status, &response_payload, &response_size) != 0) {
                perror("Failed to receive DN_EXIT response");
            }

            (void)response_payload;
            (void)response_size;

            if (status == DN_FAIL) {
                fprintf(stderr, "Datanode %i failed to exit.\n", i);
            }

            int exit;
            waitpid(pid, &exit, 0);
            LOGM("Datanode %d exited", i);
        }

        close(md->connections[i].sock_fd);
    }

    metadatanode_end();

    LOGM("===================================================================\n");

    return MDN_SUCCESS;
}

MDNStatus metadatanode_create_file(const char * filename, size_t file_size, int * fid)
{
    LOGM("===================================================================");
    LOGM("Creating file '%s' with size %zu bytes", filename, file_size);

    FileEntry * files = realloc(md->files, sizeof(FileEntry) * (md->num_files + 1));
    if (!files) return MDN_FAIL;

    md->files = files;

    FileEntry * new_file = &md->files[md->num_files];
    new_file->fid = md->num_files;
    new_file->filename = strdup(filename);
    if (!new_file->filename) {
        return MDN_FAIL;
    }

	// new_file->file_size = file_size;
	new_file->num_blocks = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

	new_file->nodes = calloc(md->num_nodes, sizeof(NodeBlock));
    if (!new_file->nodes) {
        free(new_file->filename);
        return MDN_FAIL;
    }

	for (int i = 0; i < md->num_nodes; i++) {
        new_file->nodes[i].num_blocks = 0;
        new_file->nodes[i].logical_blocks = NULL;
        new_file->nodes[i].file_blocks = NULL;
    }

    *fid = new_file->fid;
    md->num_files++;

    LOGM("Successfully created file '%s' with fid=%d, size=%d", filename, *fid, file_size);
    LOGM("===================================================================\n");

    return MDN_SUCCESS;
}

MDNStatus metadatanode_find_file(const char * filename, int * fid)
{   
    FileEntry entry;
    for (int i = 0; i < md->num_files; i++) {
        entry = md->files[i];
        if (strcmp(entry.filename, filename) == 0) {
            *fid = entry.fid;
            return MDN_SUCCESS;
        }
    }
    return MDN_FAIL;
}

MDNStatus metadatanode_truncate_file(int fid, size_t new_size)
{
	if (fid < 0 || fid >= md->num_files) {
		return MDN_FAIL;
	}

	FileEntry *file = &md->files[fid];

	size_t current_size = file->num_blocks * BLOCK_SIZE;
	int blocks_current = (current_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
	int blocks_new = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

	LOGM("===================================================================");
    LOGM("Truncating file fid=%d (%s) from %zu to %zu bytes", fid, file->filename, current_size, new_size);
    LOGM("Current blocks: %d, New blocks: %d", blocks_current, blocks_new);

	if (blocks_new > blocks_current) {
		file->num_blocks = blocks_new;
	} else if (blocks_new < blocks_current) {
		for (int dn = 0; dn < md->num_nodes; dn++) {
			NodeBlock *nb = &file->nodes[dn];
			if (nb->num_blocks == 0) continue;

			int new_count = 0;

			for (int j = 0; j < nb->num_blocks; j++) {
				if (nb->file_blocks[j] < blocks_new) {
					new_count++;
				} else {
					metadatanode_dealloc_block(nb->logical_blocks[j]);
				}
			}

			if (new_count > 0) {
				int *new_logical = malloc(sizeof(int) * new_count);
				int *new_file = malloc(sizeof(int) * new_count);

				int idx = 0;
				for (int j = 0; j < nb->num_blocks; j++) {
					if (nb->file_blocks[j] < blocks_new) {
						new_logical[idx] = nb->logical_blocks[j];
						new_file[idx] = nb->file_blocks[j];
						idx++;
					}
				}

				free(nb->logical_blocks);
				free(nb->file_blocks);
				nb->logical_blocks = new_logical;
				nb->file_blocks = new_file;
				nb->num_blocks = new_count;
			} else {
				free(nb->logical_blocks);
                free(nb->file_blocks);
                nb->logical_blocks = NULL;
                nb->file_blocks = NULL;
                nb->num_blocks = 0;
			}
		}

		file->num_blocks = blocks_new;
	} else {
		LOGM("File size unchanged");
	}

	LOGM("Truncate complete: file now has %zu bytes", new_size);
    LOGM("===================================================================\n");

	return MDN_SUCCESS;
}

MDNStatus nodeblock_add_mapping(NodeBlock *nb, int logical_block, int file_block)
{
	int *new_logical = realloc(nb->logical_blocks, sizeof(int) * (nb->num_blocks + 1));
    int *new_file = realloc(nb->file_blocks, sizeof(int) * (nb->num_blocks + 1));
    
    if (!new_logical || !new_file) {
        free(new_logical);
        free(new_file);
        return MDN_FAIL;
    }
    
    nb->logical_blocks = new_logical;
    nb->file_blocks = new_file;
    nb->logical_blocks[nb->num_blocks] = logical_block;
    nb->file_blocks[nb->num_blocks] = file_block;
    nb->num_blocks++;
    
    return MDN_SUCCESS;
}

int find_block_location(FileEntry *file, int file_index, int *logical_block)
{
	for (int dn = 0; dn < md->num_nodes; dn++) {
        NodeBlock *nb = &file->nodes[dn];
        for (int i = 0; i < nb->num_blocks; i++) {
            if (nb->file_blocks[i] == file_index) {
                *logical_block = nb->logical_blocks[i];
                return dn;
			}
		}
	}
	return -1;
}

void nodeblock_trim(NodeBlock *nb, int max_file_block) {
    int new_count = 0;
    
    // Count blocks to keep
    for (int j = 0; j < nb->num_blocks; j++) {
        if (nb->file_blocks[j] < max_file_block) {
            new_count++;
        } else {
            metadatanode_dealloc_block(nb->logical_blocks[j]);
        }
    }
    
    if (new_count == 0) {
        free(nb->logical_blocks);
        free(nb->file_blocks);
        nb->logical_blocks = NULL;
        nb->file_blocks = NULL;
        nb->num_blocks = 0;
        return;
    }
    
    // Rebuild arrays
    int *new_logical = malloc(sizeof(int) * new_count);
    int *new_file = malloc(sizeof(int) * new_count);
    
    int idx = 0;
    for (int j = 0; j < nb->num_blocks; j++) {
        if (nb->file_blocks[j] < max_file_block) {
            new_logical[idx] = nb->logical_blocks[j];
            new_file[idx] = nb->file_blocks[j];
            idx++;
        }
    }
    
    free(nb->logical_blocks);
    free(nb->file_blocks);
    nb->logical_blocks = new_logical;
    nb->file_blocks = new_file;
    nb->num_blocks = new_count;
}

MDNStatus batch_read_blocks(int node, int *logical_blocks, int num_blocks, void ** buffer)
{	
    DNCommand cmd = DN_BATCH_READ;

	size_t payload_size = sizeof(DNBatchPayload) + sizeof(int) * num_blocks;

	DNBatchPayload *payload = malloc(payload_size);
	payload->num_blocks = num_blocks;
	memcpy(payload->logical_blocks, logical_blocks, sizeof(int) * num_blocks);

    if (md_send_command(md->connections[node].sock_fd, cmd, payload, payload_size) != 0) {
        perror("Failed to send DN_READ");
		free(payload);
        return MDN_FAIL;
    }
	free(payload);

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = BLOCK_SIZE;

    if (md_recv_response(md->connections[node].sock_fd, &status, &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_READ response");
        return MDN_FAIL;
    }

    if (status != DN_SUCCESS) {
        LOGM("ERROR: DataNode %d failed to read block %d (status=%d)", node_id, block_id, status);
        free(response_payload);
        return MDN_FAIL;
    }

    *buffer = response_payload;

    return MDN_SUCCESS;
}

MDNStatus metadatanode_read_file(int fid, void ** buffer, size_t * file_size)
{
    FileEntry *file = &md->files[fid];

    *file_size = file->num_blocks * BLOCK_SIZE;
    *buffer = malloc(*file_size);

	for (int n = 0; n < md->num_nodes; n++) {
		NodeBlock *nb = &file->nodes[n];
		if (nb->num_blocks == 0) continue;

		void *blocks_data = malloc(nb->num_blocks * BLOCK_SIZE);
		if (batch_read_blocks(n, nb->logical_blocks, nb->num_blocks, &blocks_data) != MDN_SUCCESS) {
			free(blocks_data);
			return MDN_FAIL;
		}

		for (int i = 0; i < nb->num_blocks; i++) {
			int file_index = nb->file_blocks[i];
			void *dest = (char *)(*buffer) + file_index * BLOCK_SIZE;
			void *src = (char *)blocks_data + i * BLOCK_SIZE;
			memcpy(dest, src, BLOCK_SIZE);
		}
		free(blocks_data);
	}

    return MDN_SUCCESS;
}

MDNStatus batch_write_blocks(int node, int *logical_blocks, int num_blocks, void *buffer)
{	
    DNCommand cmd = DN_BATCH_WRITE;

    size_t payload_size = sizeof(DNBatchPayload) + sizeof(int) * num_blocks;
    DNBatchPayload *payload = malloc(payload_size);
    if (!payload) return MDN_FAIL;
    
    payload->num_blocks = num_blocks;
    memcpy(payload->logical_blocks, logical_blocks, sizeof(int) * num_blocks);

    // Send the payload (header with block IDs)
    if (md_send_command(md->connections[node].sock_fd, cmd, payload, payload_size) != 0) {
        perror("Failed to send DN_BATCH_WRITE");
        free(payload);
        return MDN_FAIL;
    }
    free(payload);

    // Send the actual block data
    size_t data_size = num_blocks * BLOCK_SIZE;
    if (md_send_data(md->connections[node].sock_fd, buffer, data_size) != data_size) {
        perror("Failed to send block data");
        return MDN_FAIL;
	}

    // Receive response
    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = 0;

    if (md_recv_response(md->connections[node].sock_fd, &status, &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_BATCH_WRITE response");
        return MDN_FAIL;
    }

    free(response_payload);
    
    if (status != DN_SUCCESS) {
        return MDN_FAIL;
    }

    return MDN_SUCCESS;
}

MDNStatus metadatanode_write_file(int fid, void *buffer, size_t buffer_size)
{
    FileEntry *file = &md->files[fid];
    size_t needed_blocks = (buffer_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // First pass: allocate all blocks
    for (size_t i = 0; i < needed_blocks; i++) {
        int logical_block;
        int node_id = find_block_location(file, i, &logical_block);
        
        // Allocate if not found
        if (node_id == -1) {
            AllocContext ctx = {.file_blocks = file->num_blocks};
            if (metadatanode_alloc_block(ctx, &logical_block, &node_id) != MDN_SUCCESS) {
                return MDN_NO_SPACE;
            }
            
            if (nodeblock_add_mapping(&file->nodes[node_id], logical_block, i) != MDN_SUCCESS) {
                return MDN_FAIL;
            }
        }
    }

    // Second pass: batch write to each datanode
    for (int n = 0; n < md->num_nodes; n++) {
        NodeBlock *nb = &file->nodes[n];
        if (nb->num_blocks == 0) continue;

        // Prepare buffer with blocks for this datanode
        void *batch_buffer = malloc(nb->num_blocks * BLOCK_SIZE);
        if (!batch_buffer) return MDN_FAIL;

        for (int i = 0; i < nb->num_blocks; i++) {
            int file_index = nb->file_blocks[i];
            void *dest = (char *)batch_buffer + i * BLOCK_SIZE;
            
            // Copy data from input buffer
            size_t offset = file_index * BLOCK_SIZE;
            size_t remaining = buffer_size > offset ? buffer_size - offset : 0;
            size_t to_copy = remaining < BLOCK_SIZE ? remaining : BLOCK_SIZE;
            
            memset(dest, 0, BLOCK_SIZE);
            if (to_copy > 0) {
                memcpy(dest, (char *)buffer + offset, to_copy);
            }
        }

        // Send batch to datanode
        if (batch_write_blocks(n, nb->logical_blocks, nb->num_blocks, batch_buffer) != MDN_SUCCESS) {
            free(batch_buffer);
            return MDN_FAIL;
        }

        free(batch_buffer);
    }

    return MDN_SUCCESS;
}

MDNStatus metadatanode_alloc_block(AllocContext ctx, int * block_index, int * node_id)
{
    LOGM("===================================================================");

    uint32_t blk;
    if (bitmap_alloc(md->bitmap, md->num_blocks, &blk) != 0) {
        LOGM("ERROR: Bitmap allocation failed (no free blocks)");
        return MDN_NO_SPACE;
    }

    md->free_blocks--;

    int data_idx;
    if (policy->allocate_block(ctx, &data_idx) != 0) {
        bitmap_free(md->bitmap, md->num_blocks, blk);
        md->free_blocks++;
        LOGM("ERROR: Policy failed to allocate block");
        return MDN_FAIL;
    }

    *block_index = (int)blk;
    *node_id = data_idx;

	md->blocks_free[data_idx]--;

    LOGM("Block allocated: global_id=%d, node=%d, free_blocks=%zu", blk, data_idx, md->free_blocks);

    LOGM("===================================================================\n");

    return MDN_SUCCESS;
}

MDNStatus metadatanode_dealloc_block(int block_index)
{
    LOGM("===================================================================");
	int node_id = md->block_mapping[block_index];

	LOGM("Deallocating block: blk=%d node=%d", block_index, node_id);

    bitmap_free(md->bitmap, md->num_blocks, (uint32_t)block_index);    
    md->free_blocks++;

    DNCommand cmd = DN_FREE_BLOCK;
    DNBlockIndexPayload payload = {0};
    payload.block_index = block_index;

    if (md_send_command(md->connections[node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        perror("Failed to send DN_DEALLOC");
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = 0;

    if (md_recv_response(md->connections[node_id].sock_fd, &status, &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_DEALLOC response");
        return MDN_FAIL;
    }

	md->blocks_free[node_id]++;

    LOGM("===================================================================\n");

    return status;
}

MDNStatus metadatanode_read_block(int fid, int file_index, void * buffer)
{
	FileEntry *file = &md->files[fid];
    
    if (file_index < 0 || file_index >= file->num_blocks) {
        return MDN_FAIL;
    }
    
    // Find which datanode has this block
    int logical_block;
    int node_id = find_block_location(file, file_index, &logical_block);
    
    // If block not allocated, return zeros
    if (node_id == -1) {
        memset(buffer, 0, BLOCK_SIZE);
        return MDN_SUCCESS;
    }
    
    // Read from datanode
    DNCommand cmd = DN_READ_BLOCK;
    DNBlockIndexPayload payload = {0};
    payload.block_index = logical_block;

    if (md_send_command(md->connections[node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = BLOCK_SIZE;

    if (md_recv_response(md->connections[node_id].sock_fd, &status, &response_payload, &response_size) != 0) {
        return MDN_FAIL;
    }

    if (status != DN_SUCCESS) {
        free(response_payload);
        return MDN_FAIL;
    }

    memcpy(buffer, response_payload, BLOCK_SIZE);
    free(response_payload);

    return MDN_SUCCESS;
}

MDNStatus metadatanode_write_block(int fid, int file_index, void * buffer) 
{
	FileEntry *file = &md->files[fid];
    
    if (file_index < 0 || file_index >= file->num_blocks)
        return MDN_FAIL;
	
	int logical_block;
    int node_id = find_block_location(file, file_index, &logical_block);

	if (node_id == -1) {
        AllocContext ctx = {.file_blocks = file->num_blocks};
        if (metadatanode_alloc_block(ctx, &logical_block, &node_id) != MDN_SUCCESS) {
            return MDN_NO_SPACE;
        }
        
        if (nodeblock_add_mapping(&file->nodes[node_id], logical_block, file_index) != MDN_SUCCESS) {
            return MDN_FAIL;
        }
    }

	DNCommand cmd = DN_WRITE_BLOCK;
    DNBlockPayload payload;
    payload.block_index = logical_block;
    memcpy(payload.buffer, buffer, BLOCK_SIZE);

    if (md_send_command(md->connections[node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = 0;

    if (md_recv_response(md->connections[node_id].sock_fd, &status, &response_payload, &response_size) != 0) {
        return MDN_FAIL;
    }

    free(response_payload);

    return MDN_SUCCESS;
}

MDNStatus metadatanode_end(void)
{
 //    for (int i = 0; i < md->num_files; i++) {
 //        free(md->files[i].filename);
 //        free(md->files[i].blocks);
 //    }

	// free(md->block_mapping);
	// free(md->blocks_free);
 //    free(md->files);

 //    free(md->bitmap);
 //    md->bitmap = NULL;

 //    free(md->connections);
 //    md->connections = NULL;

 //    free(md->nodes);
 //    md->nodes = NULL;
    
 //    free(md);
 //    md = NULL;
    
 //    alloc_policy_end();

    return MDN_SUCCESS;
}

