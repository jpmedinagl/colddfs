#include "allocationpolicy.h"
#include "datanode.h"
#include "metadatanode.h"

extern MetadataNode *md;
extern AllocPolicy *policy;

typedef struct {
    double *weights;
    int last_index;
} WRRState;

int weightedroundrobin_init()
{
    if (md->num_nodes < 1) return -1;

    WRRState *s = malloc(sizeof(WRRState));
    if (!s) return -1;

    s->weights = malloc(sizeof(double) * md->num_nodes);
    if (!s->weights) {
        free(s);
        return -1;
    }

    for (int i = 0; i < md->num_nodes; i++) {
        s->weights[i] = 1.0;
    }

    s->last_index = -1;
    policy->state = s;
    return 0;
}

int weightedroundrobin_allocate_block(AllocContext ctx, int *node_index)
{
    (void)ctx;
    WRRState *s = (WRRState *)policy->state;
    
    size_t node_capacity = md->fs_capacity / md->num_nodes;
    int max_blocks_per_node = node_capacity / BLOCK_SIZE;

    for (int i = 0; i < md->num_nodes; i++) {
        int blocks_free = md->blocks_free[i];
        
        s->weights[i] = (double)blocks_free / max_blocks_per_node;

        if (blocks_free <= 0) {
            s->weights[i] = 0.0;
        }
    }

    int best_idx = -1;
    double best_weight = -1.0;

    for (int node = 0; node < md->num_nodes; node++) {
        int i = (s->last_index + 1 + node) % md->num_nodes;
        
        if (s->weights[i] > best_weight) {
            best_weight = s->weights[i];
            best_idx = i;
        }
    }

    if (best_idx == -1) {
        // All nodes full
        return -1;
    }

    s->last_index = best_idx;
    *node_index = best_idx;

    return 0;
}

void weightedroundrobin_destroy() 
{
    if (policy->state) {
        WRRState *s = (WRRState *)policy->state;
        free(s->weights);
		free(policy->state);
		policy->state = NULL;
	}
}

