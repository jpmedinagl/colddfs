#include "allocationpolicy.h"
#include "metadatanode.h"

extern MetadataNode *md;
extern AllocPolicy *policy;

typedef struct {
    int last_index;
} RRState;

int roundrobin_init()
{
    if (md->num_nodes < 1) return -1;

    RRState *s = malloc(sizeof(RRState));
    if (!s) return -1;
    
    s->last_index = -1;
    policy->state = s;
    return 0;
}

int roundrobin_allocate_block(AllocContext ctx, int *node_index)
{
    (void)ctx;
    RRState *s = (RRState *)policy->state;
    s->last_index = (s->last_index + 1) % md->num_nodes;
    while (md->blocks_free[s->last_index] < 1) {
        s->last_index = (s->last_index + 1) % md->num_nodes;
    }
    *node_index = s->last_index;
    return 0;
}

void roundrobin_destroy() 
{
    if (policy->state) {
		free(policy->state);
		policy->state = NULL;
	}
}

