#include "allocationpolicy.h"
#include "metadatanode.h"

extern MetadataNode *md;
extern AllocPolicy *policy;

typedef struct {
    int current_index;
} SState;

int sequential_init()
{
    if (md->num_nodes < 1) return -1;

    SState *s = malloc(sizeof(SState));
    if (!s) return -1;
    
    s->current_index = 0;
    policy->state = s;
    return 0;
}

int sequential_allocate_block(AllocContext ctx, int *node_index)
{
    (void)ctx;
    SState *s = (SState *)policy->state;
	
	if (s->current_index == md->num_nodes) {
		// no more space
		return -1;
	}

	*node_index = s->current_index;
    
	if (md->blocks_free[s->current_index] < 2) {
		// no more space on this index
		s->current_index++;
	}

	return 0;
}

void sequential_destroy() 
{
    if (policy->state) {
		free(policy->state);
		policy->state = NULL;
	}
}


