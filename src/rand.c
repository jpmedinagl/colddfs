#include "allocationpolicy.h"
#include "metadatanode.h"

extern MetadataNode *md;

int rand_init()
{
    srand(time(NULL));
    return 0;
}

int rand_allocate_block(int *node_index)
{
    if (md->num_nodes <= 0) return -1;

    int idx = rand() % md->num_nodes;
    *node_index = idx;

    return 0;
}

void rand_destroy() {}
