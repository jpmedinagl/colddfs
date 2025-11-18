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
    LOGM("Creating file '%s' with size %zu bytes (%zu blocks needed)", filename, file_size, (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE);

    FileEntry * files = realloc(md->files, sizeof(FileEntry) * (md->num_files + 1));
    if (!files) return MDN_FAIL;

    md->files = files;

    FileEntry * new_file = &md->files[md->num_files];
    new_file->fid = md->num_files;
    new_file->filename = strdup(filename);
    if (!new_file->filename) {
        return MDN_FAIL;
    }

    int blocks_needed = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (blocks_needed > md->free_blocks) {
        free(new_file->filename);
        new_file->filename = NULL;
        return MDN_NO_SPACE;
    }

    new_file->num_blocks = 0;
    new_file->blocks = malloc(sizeof(int) * blocks_needed);
    if (!new_file->blocks) {
        free(new_file->filename);
        new_file->filename = NULL;
        return MDN_FAIL;
    }

	AllocContext ctx = {
        .file_blocks = blocks_needed,
    };

    for (int i = 0; i < blocks_needed; i++) {
        int blk, node;
        if (metadatanode_alloc_block(ctx, &blk, &node) != MDN_SUCCESS) {
            LOGM("ERROR: Failed to allocate block %d for file '%s'", i, filename);
            for (int j = 0; j < i; j++) {
                int prev = new_file->blocks[j];
                if (metadatanode_dealloc_block(prev) != MDN_SUCCESS) {
                    fprintf(stderr, "Warning: failed to dealloc block %d\n", prev);
                    return MDN_FAIL;
                }
            }
            free(new_file->blocks);
            new_file->blocks = NULL;
            free(new_file->filename);
            new_file->filename = NULL;
            return MDN_NO_SPACE;
        }

        new_file->blocks[i] = blk;
		md->block_mapping[blk] = node;
        new_file->num_blocks++;

        LOGM("Allocated block %d (global id=%d) on node %d for file '%s'", i, blk, node, filename);
    }

    *fid = new_file->fid;
    md->num_files++;

    LOGM("Successfully created file '%s' with fid=%d, num_blocks=%d", filename, *fid, new_file->num_blocks);
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
	int blocks_new = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

	LOGM("===================================================================");
    LOGM("Truncating file fid=%d (%s) from %zu to %zu bytes", fid, file->filename, current_size, new_size);
    LOGM("Current blocks: %d, New blocks: %d", file->num_blocks, blocks_new);

	if (blocks_new > file->num_blocks) {
		// need to allocate new blocks
		int blocks_needed = blocks_new - file->num_blocks;

		int *new_blocks = realloc(file->blocks, sizeof(int) * blocks_new);
        if (!new_blocks) {
            return MDN_FAIL;
        }
		file->blocks = new_blocks;

		AllocContext ctx = {
			.file_blocks = blocks_new,
		};

		for (int i = 0; i < blocks_needed; i++) {
			int blk, node;
			if (metadatanode_alloc_block(ctx, &blk, &node) != MDN_SUCCESS) {
				// in this case we don't free the file
				LOGM("ERROR: Failed to allocate block %d for file '%s'", i, file->filename);
				for (int j = file->num_blocks; j < file->num_blocks + i; j++) {
                    metadatanode_dealloc_block(file->blocks[j]);
                }
				return MDN_NO_SPACE;
			}
			
			int block_idx = file->num_blocks + i;
			file->blocks[block_idx] = blk;
			md->block_mapping[blk] = node;

			LOGM("Allocated block %d (global id=%d) on node %d for file '%s'", block_idx, blk, node, file->filename);
		}
		
		file->num_blocks = blocks_new;
	} else if (blocks_new < file->num_blocks) {
		// free file blocks
		int blocks_to_remove = file->num_blocks - blocks_new;

		for (int i = file->num_blocks - 1; i >= blocks_new; i--) {
			int block_id = file->blocks[i];
			if (metadatanode_dealloc_block(block_id) != MDN_SUCCESS) {
                fprintf(stderr, "Warning: failed to dealloc block %d\n", block_id);
                return MDN_FAIL;
            }
		}

		file->num_blocks = blocks_new;

		if (blocks_new > 0) {
			int * new_blocks = realloc(file->blocks, sizeof(int) * blocks_new);
			if (new_blocks) {
				file->blocks = new_blocks;
			}
		} else {
			// truncated to 0
			free(file->blocks);
			file->blocks = NULL;
		}
	} else {
		LOGM("File size unchanged");
	}

	LOGM("Truncate complete: file now has %d blocks", file->num_blocks);
    LOGM("===================================================================\n");

	return MDN_SUCCESS;
}

MDNStatus metadatanode_read_file(int fid, void ** buffer, size_t * file_size)
{
    FileEntry file = md->files[fid];

    *file_size = file.num_blocks * BLOCK_SIZE;
    *buffer = malloc(*file_size);

    for (int i = 0; i < file.num_blocks; i++) {
        void *block_ptr = (char *)(*buffer) + i * BLOCK_SIZE;
        if (metadatanode_read_block(fid, i, block_ptr) != MDN_SUCCESS) {
            free(*buffer);
            return MDN_FAIL;
        }
    }

    return MDN_SUCCESS;
}

MDNStatus metadatanode_write_file(int fid, void * buffer, size_t buffer_size)
{
    FileEntry * file = &md->files[fid];
    size_t needed_blocks = (buffer_size + BLOCK_SIZE - 1) / BLOCK_SIZE;

	AllocContext ctx = {
		.file_blocks = needed_blocks
	};

    // allocate more blocks for file
    if (needed_blocks > file->num_blocks) {
        // size_t extra_blocks = needed_blocks - file->num_blocks;
        int *new_blocks = realloc(file->blocks, sizeof(int) * needed_blocks);
        if (!new_blocks) return MDN_FAIL;
        file->blocks = new_blocks;

        for (size_t i = file->num_blocks; i < needed_blocks; i++) {
            int blk, node;
            if (metadatanode_alloc_block(ctx, &blk, &node) != MDN_SUCCESS)
                return MDN_NO_SPACE;

            file->blocks[i] = blk;
			md->block_mapping[blk] = node;
        }

        file->num_blocks = needed_blocks;
    }

    // write file block by block
    for (size_t i = 0; i < needed_blocks; i++) {
        char block_buf[BLOCK_SIZE];
        memset(block_buf, 0, BLOCK_SIZE);

        size_t offset = i * BLOCK_SIZE;
        size_t remaining = buffer_size - offset;
        size_t to_copy = remaining < BLOCK_SIZE ? remaining : BLOCK_SIZE;

        memcpy(block_buf, (char *)buffer + offset, to_copy);

        if (to_copy < BLOCK_SIZE)
            block_buf[to_copy] = '\0';

        if (metadatanode_write_block(fid, i, block_buf) != MDN_SUCCESS)
            return MDN_FAIL;
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

    DNCommand cmd = DN_ALLOC_BLOCK;
    DNBlockIndexPayload payload = {0};
    payload.block_index = *block_index;

    if (md_send_command(md->connections[*node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        bitmap_free(md->bitmap, md->num_blocks, blk);
        md->free_blocks++;
        perror("Failed to send DN_ALLOC");
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = 0;

    if (md_recv_response(md->connections[*node_id].sock_fd, &status, &response_payload, &response_size) != 0) {
        bitmap_free(md->bitmap, md->num_blocks, blk);
        md->free_blocks++;
        perror("Failed to receive DN_ALLOC response");
        return MDN_FAIL;
    }

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
    
    LOGM("===================================================================");
    LOGM("Reading block %d from file fid=%d (%s)", file_index, fid, file->filename);

    if (file_index < 0 || file_index >= file->num_blocks) {
        LOGM("ERROR: Invalid file index %d (file has %d blocks)", file_index, file->num_blocks);
        return MDN_FAIL;
    }

    int block_id = file->blocks[file_index];
	int node_id = md->block_mapping[block_id];

    LOGM("Block %d maps to: node=%d, block_id=%d", file_index, node_id, block_id);
         
    DNCommand cmd = DN_READ_BLOCK;

    DNBlockIndexPayload payload = {0};
    payload.block_index = block_id;

    if (md_send_command(md->connections[node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        perror("Failed to send DN_READ");
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = BLOCK_SIZE;

    if (md_recv_response(md->connections[node_id].sock_fd, &status, &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_READ response");
        return MDN_FAIL;
    }

    if (status != DN_SUCCESS) {
        LOGM("ERROR: DataNode %d failed to read block %d (status=%d)", node_id, block_id, status);
        free(response_payload);
        return MDN_FAIL;
    }

    memcpy(buffer, response_payload, BLOCK_SIZE);
    free(response_payload);

    LOGM("Successfully read block %d from node %d", block_id, node_id);

    LOGM("===================================================================\n");

    return MDN_SUCCESS;
}

MDNStatus metadatanode_write_block(int fid, int file_index, void * buffer) 
{
    LOGM("===================================================================");

    FileEntry *file = &md->files[fid];

    if (file_index < 0 || file_index >= file->num_blocks)
        return MDN_FAIL;

    int block_id = file->blocks[file_index];
	int node_id = md->block_mapping[block_id];

    DNCommand cmd = DN_WRITE_BLOCK;

    DNBlockPayload payload;
    payload.block_index = block_id;
    memcpy(payload.buffer, buffer, BLOCK_SIZE);

    if (md_send_command(md->connections[node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        perror("Failed to send DN_WRITE");
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = 0;

    if (md_recv_response(md->connections[node_id].sock_fd, &status,
                         &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_WRITE response");
        return MDN_FAIL;
    }

    free(response_payload);

    if (status != DN_SUCCESS) {
        fprintf(stderr, "Data node %d failed to write block %d\n", node_id, block_id);
        return MDN_FAIL;
    }

    LOGM("===================================================================\n");

    return MDN_SUCCESS;
}

MDNStatus metadatanode_end(void)
{
    for (int i = 0; i < md->num_files; i++) {
        free(md->files[i].filename);
        free(md->files[i].blocks);
    }

	free(md->block_mapping);
	free(md->blocks_free);
    free(md->files);

    free(md->bitmap);
    md->bitmap = NULL;

    free(md->connections);
    md->connections = NULL;

    free(md->nodes);
    md->nodes = NULL;
    
    free(md);
    md = NULL;
    
    alloc_policy_end();

    return MDN_SUCCESS;
}

