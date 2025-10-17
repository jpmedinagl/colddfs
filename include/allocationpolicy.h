#ifndef ALLOCATION_POLICY_H
#define ALLOCATION_POLICY_H

#include <stddef.h>
#include <stdbool.h>
#include <time.h>
#include <string.h>

#define ALLOCPOLICIES \
    P(rand) \
    P(roundrobin) \
    // P(lu)

#define P(name) \
    int name##_init(); \
    int name##_allocate_block(int *node_index); \
    void name##_destroy();
    ALLOCPOLICIES
#undef P

typedef struct AllocPolicy {
    const char *name;

    int (*init)(void);
    int (*allocate_block)(int *node_index);
    void (*destroy)();

    void *state;
} AllocPolicy;

bool alloc_policy_init(const char *name);
void alloc_policy_end(void);

#endif // ALLOCATION_POLICY_H