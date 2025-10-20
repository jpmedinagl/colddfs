#include "bitmap.h"

const size_t bits_per_word = sizeof(size_t) * CHAR_BIT;
const size_t word_all_bits = (size_t)-1;

int bitmap_init(bitmap_t *bitmap, uint32_t nbits)
{
    uint32_t nwords = (nbits + bits_per_word - 1) / bits_per_word;
    memset(bitmap, 0, nwords * sizeof(size_t));

    if (nwords > nbits / bits_per_word) {
        uint32_t idx = nwords - 1;
        uint32_t overbits = nbits - idx * bits_per_word;
        
        for (uint32_t j = overbits; j < bits_per_word; ++j) {
            bitmap[idx] |= ((size_t)1 << j);
        }
    }

    return 0;
}

int bitmap_alloc(bitmap_t *bitmap, uint32_t nbits, uint32_t *index)
{
    uint32_t max_idx = (nbits + bits_per_word - 1) / bits_per_word;

    for (uint32_t idx = 0; idx < max_idx; idx++) {
        if (bitmap[idx] != word_all_bits) {
            size_t inverted = ~bitmap[idx];
            int bit = __builtin_ctzl(inverted);
            uint32_t global_bit = idx * bits_per_word + bit;
            
            if (global_bit >= nbits) {
                return 1;
            }
            
            bitmap[idx] |= ((size_t)1 << bit);
            *index = global_bit;
            return 0;
        }
    }

    return -1;
}

void bitmap_free(bitmap_t *bitmap, uint32_t nbits, uint32_t index)
{
    uint32_t word_idx = index / bits_per_word;
    uint32_t bit_idx = index % bits_per_word;
    size_t mask = (size_t)1 << bit_idx;

    assert(index < nbits);
    assert((bitmap[word_idx] & mask) != 0);

    bitmap[word_idx] &= ~mask;
}

void bitmap_set(bitmap_t *bitmap, uint32_t nbits, uint32_t index, bool val)
{
    uint32_t word_idx = index / bits_per_word;
    uint32_t bit_idx = index % bits_per_word;
    size_t mask = (size_t)1 << bit_idx;

    assert(index < nbits);

    if (val == true) {
        bitmap[word_idx] |= mask;
    } else {
        bitmap[word_idx] &= ~mask;
    }
}

bool bitmap_isset(bitmap_t *bitmap, uint32_t nbits, uint32_t index)
{
    uint32_t word_idx = index / bits_per_word;
    uint32_t bit_idx = index % bits_per_word;
    size_t mask = (size_t)1 << bit_idx;
    return (bitmap[word_idx] & mask) != 0;
}
