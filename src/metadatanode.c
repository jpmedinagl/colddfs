#include "metadatanode.h"

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
            free(md->connections);
            free(md->nodes);
            free(md);
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
        DNInitPayload payload = { .node_id = i, .capacity = node_capacity };
        
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

MDNStatus metadatanode_init(int num_dns, size_t capacity)
{   
    md = malloc(sizeof(MetadataNode));

    MDNStatus status;

    md->fs_capacity = capacity;
    md->num_files = 0;
    md->files = NULL;

    md->num_nodes = num_dns;
    md->nodes = malloc(md->num_nodes * sizeof(DataNode));
    if (!md->nodes) return MDN_FAIL;
    
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

    free(md->connections);
    md->connections = NULL;

    free(md->nodes);
    md->nodes = NULL;
    
    free(md);
    md = NULL;

    return MDN_SUCCESS;
}

MDNStatus metadatanode_create_file(size_t file_size, int * fid)
{
    return MDN_FAIL;
}

MDNStatus metadatanode_find_file(const char * filename, int * fid)
{
    return MDN_FAIL;
}

MDNStatus metadatanode_read_file(int fid, void * buffer, size_t * file_size)
{
    return MDN_FAIL;
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
    return MDN_FAIL;
}

MDNStatus metadatanode_write_block(int fid, int block_index, void * buffer) 
{
    return MDN_FAIL;
}