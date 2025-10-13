#include "datanode.h"

DataNode * dn = NULL;

DNStatus datanode_init(int sock_fd)
{
    dn = malloc(sizeof(DataNode));

    dn->sock_fd = sock_fd;

    if (read(dn->sock_fd, &dn->node_id, sizeof(dn->node_id)) != sizeof(dn->node_id)) {
        return DN_FAIL;
    }
    LOGD(dn->node_id, "received node id=%d", dn->node_id);

    if (read(dn->sock_fd, &dn->capacity, sizeof(dn->capacity)) != sizeof(dn->capacity)) {
        return DN_FAIL;
    }
    LOGD(dn->node_id, "received capacity=%zu", dn->capacity);

    int ack = 1;
    write(dn->sock_fd, &ack, sizeof(ack));
    LOGD(dn->node_id, "sent ack");

    snprintf(dn->dir_path, sizeof(dn->dir_path), "dn_%d", dn->node_id);
    mkdir(dn->dir_path, 0755);

    dn->size = 0;
    
    return DN_SUCCESS;
}

DNStatus datanode_alloc_block(int *block_index)
{
    return DN_FAIL;
}

DNStatus datanode_free_block(int block_index)
{
    return DN_FAIL;
}

DNStatus datanode_read_block(int block_index, void * buffer)
{
    return DN_FAIL;
}

DNStatus datanode_write_block(int block_index, void * buffer)
{
    return DN_FAIL;
}

DNStatus datanode_service_loop()
{
    while (1) {}
    return DN_FAIL;
}
