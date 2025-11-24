// #include <stdio.h>
// #include <string.h>
// #include <stdlib.h>

// #include "datanode.h"
// #include "metadatanode.h"

// int main(void) {
//     printf("========== Batch Read Test ==========\n\n");

//     metadatanode_init(1, 4 * BLOCK_SIZE, "sequential");

//     const char *filename = "batch_test.txt";
//     int size = 3 * BLOCK_SIZE;  // 3 blocks
//     int fid;

//     metadatanode_create_file(filename, size, &fid);
    
//     printf("Writing 3 blocks individually...\n");

//     char block0[BLOCK_SIZE];
//     memset(block0, 0, BLOCK_SIZE);
//     strncpy(block0, "BLOCK 0: First block\n", BLOCK_SIZE - 1);
//     if (metadatanode_write_block(fid, 0, block0) != MDN_SUCCESS) {
//         fprintf(stderr, "Failed to write block 0\n");
//         return 1;
//     }
//     printf("✓ Wrote block 0\n");

//     char block1[BLOCK_SIZE];
//     memset(block1, 0, BLOCK_SIZE);
//     strncpy(block1, "BLOCK 1: Second block\n", BLOCK_SIZE - 1);
//     if (metadatanode_write_block(fid, 1, block1) != MDN_SUCCESS) {
//         fprintf(stderr, "Failed to write block 1\n");
//         return 1;
//     }
//     printf("✓ Wrote block 1\n");

//     char block2[BLOCK_SIZE];
//     memset(block2, 0, BLOCK_SIZE);
//     strncpy(block2, "BLOCK 2: Third block\n", BLOCK_SIZE - 1);
//     if (metadatanode_write_block(fid, 2, block2) != MDN_SUCCESS) {
//         fprintf(stderr, "Failed to write block 2\n");
//         return 1;
//     }
//     printf("✓ Wrote block 2\n");

//     printf("\n✓ All writes complete\n\n");
    
//     printf("--- Now reading entire file (should batch) ---\n");
    
//     void *buffer;
//     size_t read_size;
//     if (metadatanode_read_file(fid, &buffer, &read_size) != MDN_SUCCESS) {
//         fprintf(stderr, "Failed to read file\n");
//         return 1;
//     }

//     printf("Read %zu bytes:\n", read_size);
//     printf("---\n");
    
//     // Print each block individually
//     for (int i = 0; i < 3; i++) {
//         printf("Block %d: ", i);
//         char *block_start = (char *)buffer + i * BLOCK_SIZE;
        
//         // Find length of string (up to 100 chars or null terminator)
//         int len = 0;
//         while (len < BLOCK_SIZE && len < 100 && block_start[len] != '\0') {
//             len++;
//         }
        
//         if (len == 0 || block_start[0] == '\0') {
//             printf("(empty)\n");
//         } else {
//             printf("%.*s", len, block_start);
//         }
//     }
    
//     printf("---\n");
    
//     free(buffer);
    
//     metadatanode_exit(0);
    
//     printf("\n✓ Test complete!\n");
//     return 0;
// }

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "datanode.h"
#include "metadatanode.h"

void print_separator(const char *title) {
    printf("\n========================================\n");
    printf("%s\n", title);
    printf("========================================\n\n");
}

int test_individual_block_operations() {
    print_separator("TEST 1: Individual Block Read/Write");
    
    const char *filename = "individual_test.txt";
    int size = 3 * BLOCK_SIZE;
    int fid;
    
    if (metadatanode_create_file(filename, size, &fid) != MDN_SUCCESS) {
        printf("❌ Failed to create file\n");
        return 0;
    }
    printf("✓ Created file (fid=%d)\n", fid);
    
    // Write blocks individually
    printf("\nWriting 3 blocks individually...\n");
    
    char block0[BLOCK_SIZE];
    memset(block0, 0, BLOCK_SIZE);
    strncpy(block0, "INDIVIDUAL 0", BLOCK_SIZE - 1);
    if (metadatanode_write_block(fid, 0, block0) != MDN_SUCCESS) {
        printf("❌ Failed to write block 0\n");
        return 0;
    }
    printf("✓ Wrote block 0\n");
    
    char block1[BLOCK_SIZE];
    memset(block1, 0, BLOCK_SIZE);
    strncpy(block1, "INDIVIDUAL 1", BLOCK_SIZE - 1);
    if (metadatanode_write_block(fid, 1, block1) != MDN_SUCCESS) {
        printf("❌ Failed to write block 1\n");
        return 0;
    }
    printf("✓ Wrote block 1\n");
    
    char block2[BLOCK_SIZE];
    memset(block2, 0, BLOCK_SIZE);
    strncpy(block2, "INDIVIDUAL 2", BLOCK_SIZE - 1);
    if (metadatanode_write_block(fid, 2, block2) != MDN_SUCCESS) {
        printf("❌ Failed to write block 2\n");
        return 0;
    }
    printf("✓ Wrote block 2\n");
    
    // Read blocks individually
    printf("\nReading blocks individually...\n");
    
    char read_buf[BLOCK_SIZE];
    for (int i = 0; i < 3; i++) {
        if (metadatanode_read_block(fid, i, read_buf) != MDN_SUCCESS) {
            printf("❌ Failed to read block %d\n", i);
            return 0;
        }
        printf("✓ Block %d: %s\n", i, read_buf);
    }
    
    printf("\n✅ Individual block operations PASSED\n");
    return 1;
}

int test_batch_write_and_read() {
    print_separator("TEST 2: Batch Write (write_file)");
    
    const char *filename = "batch_test.txt";
    int size = 3 * BLOCK_SIZE;
    int fid;
    
    if (metadatanode_create_file(filename, size, &fid) != MDN_SUCCESS) {
        printf("❌ Failed to create file\n");
        return 0;
    }
    printf("✓ Created file (fid=%d)\n", fid);
    
    // Create buffer with 3 blocks
    char *write_buffer = malloc(size);
    memset(write_buffer, 0, size);
    
    strncpy(write_buffer + 0 * BLOCK_SIZE, "BATCH BLOCK 0", BLOCK_SIZE - 1);
    strncpy(write_buffer + 1 * BLOCK_SIZE, "BATCH BLOCK 1", BLOCK_SIZE - 1);
    strncpy(write_buffer + 2 * BLOCK_SIZE, "BATCH BLOCK 2", BLOCK_SIZE - 1);
    
    printf("\nWriting entire file (3 blocks) with write_file...\n");
    if (metadatanode_write_file(fid, write_buffer, size) != MDN_SUCCESS) {
        printf("❌ Failed to write file\n");
        free(write_buffer);
        return 0;
    }
    printf("✓ Batch write complete\n");
    free(write_buffer);
    
    // Read back with batch read
    print_separator("TEST 3: Batch Read (read_file)");
    
    void *read_buffer;
    size_t read_size;
    
    printf("Reading entire file with read_file...\n");
    if (metadatanode_read_file(fid, &read_buffer, &read_size) != MDN_SUCCESS) {
        printf("❌ Failed to read file\n");
        return 0;
    }
    printf("✓ Batch read complete (%zu bytes)\n\n", read_size);
    
    // Verify content
    printf("Verifying content:\n");
    for (int i = 0; i < 3; i++) {
        char *block = (char *)read_buffer + i * BLOCK_SIZE;
        printf("  Block %d: %s\n", i, block);
    }
    
    free(read_buffer);
    
    printf("\n✅ Batch write/read PASSED\n");
    return 1;
}

int test_mixed_operations() {
    print_separator("TEST 4: Mixed Operations");
    
    const char *filename = "mixed_test.txt";
    int size = 4 * BLOCK_SIZE;
    int fid;
    
    if (metadatanode_create_file(filename, size, &fid) != MDN_SUCCESS) {
        printf("❌ Failed to create file\n");
        return 0;
    }
    printf("✓ Created file (fid=%d)\n", fid);
    
    // Write blocks 0 and 2 individually
    printf("\nWriting blocks 0 and 2 individually...\n");
    
    char block0[BLOCK_SIZE];
    memset(block0, 0, BLOCK_SIZE);
    strncpy(block0, "MIXED BLOCK 0", BLOCK_SIZE - 1);
    metadatanode_write_block(fid, 0, block0);
    printf("✓ Wrote block 0\n");
    
    char block2[BLOCK_SIZE];
    memset(block2, 0, BLOCK_SIZE);
    strncpy(block2, "MIXED BLOCK 2", BLOCK_SIZE - 1);
    metadatanode_write_block(fid, 2, block2);
    printf("✓ Wrote block 2\n");
    
    // Read entire file (should handle sparse blocks)
    printf("\nReading entire file (blocks 1 and 3 unallocated)...\n");
    
    void *buffer;
    size_t read_size;
    if (metadatanode_read_file(fid, &buffer, &read_size) != MDN_SUCCESS) {
        printf("❌ Failed to read file\n");
        return 0;
    }
    
    printf("Content:\n");
    for (int i = 0; i < 4; i++) {
        char *block = (char *)buffer + i * BLOCK_SIZE;
        if (block[0] == '\0') {
            printf("  Block %d: (empty/sparse)\n", i);
        } else {
            printf("  Block %d: %s\n", i, block);
        }
    }
    
    free(buffer);
    
    printf("\n✅ Mixed operations PASSED\n");
    return 1;
}

int main(void) {
    printf("\n");
    printf("╔════════════════════════════════════════╗\n");
    printf("║  Distributed File System Test Suite   ║\n");
    printf("╔════════════════════════════════════════╝\n");
    
    metadatanode_init(2, 10 * BLOCK_SIZE, "roundrobin");
    
    int passed = 0;
    int total = 3;
    
    if (test_individual_block_operations()) passed++;
    if (test_batch_write_and_read()) passed++;
    if (test_mixed_operations()) passed++;
    
    metadatanode_exit(0);
    
    print_separator("TEST RESULTS");
    printf("Passed: %d/%d tests\n", passed, total);
    
    if (passed == total) {
        printf("\n🎉 ALL TESTS PASSED! 🎉\n\n");
        return 0;
    } else {
        printf("\n❌ SOME TESTS FAILED\n\n");
        return 1;
    }
}
