#include "allocationpolicy.h"
#include "metadatanode.h"

extern MetadataNode *md;
extern AllocPolicy *policy;

int leastloaded_init()
{
	if (md->num_nodes < 1) {
		return -1;
	}
    return 0;
}

int leastloaded_allocate_block(int *node_index)
{
    int least_loaded = -1;
	for (int i = 0; i < md->num_nodes; i++) {
		if (md->blocks_free[i] < 0) {
			continue;
		}
		
		if (least_loaded == -1 || md->blocks_free[i] > md->blocks_free[least_loaded]) {
			least_loaded = i;
		}
	}

	if (least_loaded == -1) {
		// no space
		return -1;
	}

    *node_index = least_loaded;
    return 0;
}

void leastloaded_destroy() {}

