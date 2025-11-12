#include "allocationpolicy.h"
#include "metadatanode.h"

extern MetadataNode *md;
extern AllocPolicy *policy;

#define SMALL_FILE 4

int fileaware_init()
{
	if (md->num_nodes < 1) return -1;
	srand(time(NULL));
    return 0;
}

int fileaware_allocate_block(AllocContext ctx, int *node_index)
{
	int blocks_needed = ctx.file_blocks;

	// 1. if blocks needed is small -> random
	if (blocks_needed < SMALL_FILE) {
		for (int attempts = 0; attempts < md->num_nodes * 2; attempts++) {
            int candidate = rand() % md->num_nodes;
            if (0 < md->blocks_free[candidate]) {
                *node_index = candidate;
                return 0;
            }
        }	
	}

	// 2. large file -> use least loaded
	int least_loaded = -1;
	int max_blocks_free = -1;

	for (int i = 0; i < md->num_nodes; i++) {
		if (md->blocks_free[i] < 1) {
			continue;
		}

		if (md->blocks_free[i] > max_blocks_free) {
			max_blocks_free = md->blocks_free[i];
			least_loaded = i;
		}
	}

	if (least_loaded == -1) {
		return -1;
	}

	return 0;
}

void fileaware_destroy() {}

