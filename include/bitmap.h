#ifndef BITMAP_H
#define BITMAP_H

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>
#include <limits.h>
#include <stdint.h>

typedef size_t bitmap_t;

int bitmap_init(bitmap_t *bitmap, uint32_t nbits);

int bitmap_alloc(bitmap_t *bitmap, uint32_t nbits, uint32_t *index);

void bitmap_free(bitmap_t *bitmap, uint32_t nbits, uint32_t index);

void bitmap_set(bitmap_t *bitmap, uint32_t nbits, uint32_t index, bool val);

bool bitmap_isset(bitmap_t *bitmap, uint32_t nbits, uint32_t index);

#endif // BITMAP_H