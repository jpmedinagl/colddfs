#include "metadatanode.h"
#include "datanode.h"
#include "allocationpolicy.h"

MetadataNode * md = NULL;

MDNStatus initialize_datanodes()
{
    md->connections = malloc(md->num_nodes * sizeof(NodeConnection));
    if (!md->connections) return MDN_FAIL;

    LOGM("Initializing %d data nodes", md->num_nodes);
    for (int i = 0; i < md->num_nodes; i++) {
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
    size_t node_capacity = md->fs_capacity / md->num_nodes;

    LOGM("Connecting %d data nodes", md->num_nodes);
    for (int i = 0; i < md->num_nodes; i++) {
        DNInitPayload payload = {0};
        payload.node_id = i;
        payload.capacity= node_capacity;
        
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
    
    return MDN_SUCCESS;
}

MDNStatus metadatanode_init(int num_dns, size_t capacity, const char *policy_name)
{   
    md = malloc(sizeof(MetadataNode));

    md->fs_capacity = capacity;
    md->num_blocks = (capacity + BLOCK_SIZE - 1) / BLOCK_SIZE;

    size_t bits_per_word = sizeof(size_t) * CHAR_BIT;
    uint32_t nwords = (md->num_blocks + bits_per_word - 1) / bits_per_word;
    md->bitmap = malloc(sizeof(bitmap_t) * nwords);
    bitmap_init(md->bitmap, md->num_blocks);
    md->free_blocks = md->num_blocks;

    md->num_files = 0;
    md->files = NULL;

    md->num_nodes = num_dns;
    md->nodes = malloc(md->num_nodes * sizeof(DataNode));
    if (!md->nodes) return MDN_FAIL;

    if (!alloc_policy_init(policy_name)) {
        fprintf(stderr, "Failed to initialize allocation policy\n");
        return MDN_FAIL;
    }
    
    MDNStatus status;
    status = initialize_datanodes(md);
    if (status != MDN_SUCCESS)
        return status;

    return connect_datanodes(md);
}

MDNStatus metadatanode_exit()
{
    if (!md || !md->connections) return MDN_FAIL;

    LOGM("Exiting...");
    for (int i = 0; i < md->num_nodes; i++) {
        pid_t pid = md->connections[i].pid;
        if (pid > 0) {
            DNCommand cmd = DN_EXIT;

            if (md_send_command(md->connections[i].sock_fd, cmd, NULL, 0) != 0) {
                perror("Failed to send DN_EXIT");
                return MDN_FAIL;
            }

            DNStatus status;
            void *response_payload = NULL;
            size_t response_size = 0;
            
            if (md_recv_response(md->connections[i].sock_fd, &status, &response_payload, &response_size) != 0) {
                perror("Failed to receive DN_EXIT response");
            }

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

    return MDN_SUCCESS;
}

MDNStatus metadatanode_create_file(const char * filename, size_t file_size, int * fid)
{
    FileEntry * files = realloc(md->files, sizeof(FileEntry) * (md->num_files + 1));
    if (!files) return MDN_FAIL;

    md->files = files;

    FileEntry new_file = md->files[md->num_files];
    new_file.fid = md->num_files;
    new_file.filename = strdup(filename);

    int blocks_needed = (file_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
    if (blocks_needed > md->free_blocks)
        return MDN_NO_SPACE;

    new_file.num_blocks = blocks_needed;
    new_file.blocks = malloc(sizeof(BlockLocation) * blocks_needed);

    for (int i = 0; i < blocks_needed; i++) {
        uint32_t blk;
        if (bitmap_alloc(md->bitmap, md->num_blocks, &blk) != 0) {
            return MDN_NO_SPACE;
        }

        md->free_blocks--;

        int data_indx;
        if (policy->allocate_block(&data_indx) != 0)
            return MDN_FAIL;

        new_file.blocks[i] = (BlockLocation){
            .block_id = (int)blk,
            .data_node_id = data_indx
        };
    }

    md->files[md->num_files] = new_file;
    *fid = new_file.fid;
    md->num_files++;

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
    return MDN_FAIL;
}

MDNStatus metadatanode_alloc_block(int * block_index, int * node_id)
{
    return MDN_FAIL;
}

MDNStatus metadatanode_read_block(int fid, int block_index, void * buffer)
{
    FileEntry *file = &md->files[fid];

    if (block_index < 0 || block_index >= file->num_blocks)
        return MDN_FAIL;

    BlockLocation block = file->blocks[block_index];
    DNCommand cmd = DN_READ_BLOCK;

    DNBlockIndexPayload payload = {0};
    payload.block_index = block.block_id;

    if (md_send_command(md->connections[block.data_node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        perror("Failed to send DN_READ");
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = BLOCK_SIZE;

    if (md_recv_response(md->connections[block.data_node_id].sock_fd, &status,
                         &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_READ response");
        return MDN_FAIL;
    }

    if (status != DN_SUCCESS) {
        fprintf(stderr, "Data node %d failed to read block %d\n",
                block.data_node_id, block.block_id);
        free(response_payload);
        return MDN_FAIL;
    }

    memcpy(buffer, response_payload, BLOCK_SIZE);
    free(response_payload);

    return MDN_SUCCESS;
}

MDNStatus metadatanode_write_block(int fid, int block_index, void * buffer) 
{
    FileEntry *file = &md->files[fid];

    if (block_index < 0 || block_index >= file->num_blocks)
        return MDN_FAIL;

    BlockLocation block = file->blocks[block_index];
    DNCommand cmd = DN_WRITE_BLOCK;

    DNBlockPayload payload;
    payload.block_index = block.block_id;
    memcpy(payload.buffer, buffer, BLOCK_SIZE);

    if (md_send_command(md->connections[block.data_node_id].sock_fd, cmd, &payload, sizeof(payload)) != 0) {
        perror("Failed to send DN_WRITE");
        return MDN_FAIL;
    }

    DNStatus status;
    void *response_payload = NULL;
    size_t response_size = 0;

    if (md_recv_response(md->connections[block.data_node_id].sock_fd, &status,
                         &response_payload, &response_size) != 0) {
        perror("Failed to receive DN_WRITE response");
        return MDN_FAIL;
    }

    free(response_payload);

    if (status != DN_SUCCESS) {
        fprintf(stderr, "Data node %d failed to write block %d\n",
                block.data_node_id, block.block_id);
        return MDN_FAIL;
    }

    return MDN_SUCCESS;
}

MDNStatus metadatanode_end(void)
{
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