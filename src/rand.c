#include "allocationpolicy.h"
#include "metadatanode.h"

extern MetadataNode *md;

int rand_init()
{
    if (md->num_nodes < 1) return -1;

    srand(time(NULL));
    return 0;
}

int rand_allocate_block(AllocContext ctx, int *node_index)
{
    (void)ctx;

    for (int attempt = 0; attempt < md->num_nodes; attempt++) {
        int idx = rand() % md->num_nodes;
        if (0 < md->blocks_free[idx]) {
            *node_index = idx;
            return 0;
        }
    }

    for (int idx = 0; idx < md->num_nodes; idx++) {
        if (0 < md->blocks_free[idx]) {
            *node_index = idx;
            return 0;
        }
    }

    // no space
    return -1;
}

void rand_destroy() {}

