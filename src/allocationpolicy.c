#include "allocationpolicy.h"

AllocPolicy alloc_policies[] = {
#define P(name) { #name, name##_init, name##_allocate_block, name##_destroy, NULL },
    ALLOCPOLICIES
#undef P
};

struct AllocPolicy * policy = NULL;

const int num_alloc_policies = sizeof(alloc_policies)/sizeof(AllocPolicy);

bool alloc_policy_init(const char *name)
{
    policy = NULL;
    for (int i = 0; i < num_alloc_policies; i++) {
        if (strcmp(name, alloc_policies[i].name) == 0) {
            policy = &alloc_policies[i];
            return policy->init() == 0;
        }
    }
    return false;
}

void alloc_policy_end(void)
{
    if (policy != NULL)
        policy->destroy();
    policy = NULL;
}