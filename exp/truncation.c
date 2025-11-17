#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "datanode.h"
#include "metadatanode.h"

// Test helper: Print test result
void print_test_result(const char *test_name, int passed) {
    if (passed) {
        printf("✓ PASS: %s\n", test_name);
    } else {
        printf("✗ FAIL: %s\n", test_name);
    }
}

// Test 1: Basic initialization and cleanup
int test_init_and_exit() {
    printf("\n=== Test 1: Initialization and Exit ===\n");
    
    MDNStatus status = metadatanode_init(2, 10 * BLOCK_SIZE, "roundrobin");
    if (status != MDN_SUCCESS) {
        printf("Failed to initialize metadata node\n");
        return 0;
    }
    
    status = metadatanode_exit();
    if (status != MDN_SUCCESS) {
        printf("Failed to exit metadata node\n");
        return 0;
    }
    
    return 1;
}

// Test 2: Create and find file
int test_create_and_find_file() {
    printf("\n=== Test 2: Create and Find File ===\n");
    
    metadatanode_init(2, 10 * BLOCK_SIZE, "roundrobin");
    
    int fid;
    MDNStatus status = metadatanode_create_file("testfile.txt", BLOCK_SIZE, &fid);
    if (status != MDN_SUCCESS) {
        printf("Failed to create file\n");
        metadatanode_exit();
        return 0;
    }
    
    printf("Created file with fid=%d\n", fid);
    
    int found_fid;
    status = metadatanode_find_file("testfile.txt", &found_fid);
    if (status != MDN_SUCCESS || found_fid != fid) {
        printf("Failed to find file\n");
        metadatanode_exit();
        return 0;
    }
    
    printf("Found file with fid=%d\n", found_fid);
    
    metadatanode_exit();
    return 1;
}

// Test 3: Write and read single block
int test_write_and_read_block() {
    printf("\n=== Test 3: Write and Read Single Block ===\n");
    
    metadatanode_init(2, 10 * BLOCK_SIZE, "roundrobin");
    
    int fid;
    metadatanode_create_file("data.txt", BLOCK_SIZE, &fid);
    
    // Write data
    char write_buffer[BLOCK_SIZE];
    memset(write_buffer, 0, BLOCK_SIZE);
    const char *msg = "Hello, Distributed File System!";
    strncpy(write_buffer, msg, BLOCK_SIZE - 1);
    
    MDNStatus status = metadatanode_write_block(fid, 0, write_buffer);
    if (status != MDN_SUCCESS) {
        printf("Failed to write block\n");
        metadatanode_exit();
        return 0;
    }
    
    // Read data back
    char read_buffer[BLOCK_SIZE];
    memset(read_buffer, 0, BLOCK_SIZE);
    
    status = metadatanode_read_block(fid, 0, read_buffer);
    if (status != MDN_SUCCESS) {
        printf("Failed to read block\n");
        metadatanode_exit();
        return 0;
    }
    
    // Verify data
    if (strcmp(write_buffer, read_buffer) != 0) {
        printf("Data mismatch!\n");
        printf("Written: %s\n", write_buffer);
        printf("Read: %s\n", read_buffer);
        metadatanode_exit();
        return 0;
    }
    
    printf("Data verified: %s\n", read_buffer);
    
    metadatanode_exit();
    return 1;
}

// Test 4: Write and read entire file
int test_write_and_read_file() {
    printf("\n=== Test 4: Write and Read Entire File ===\n");
    
    metadatanode_init(2, 10 * BLOCK_SIZE, "roundrobin");
    
    int fid;
    metadatanode_create_file("largefile.txt", 3 * BLOCK_SIZE, &fid);
    
    // Create test data
    char *write_data = malloc(3 * BLOCK_SIZE);
    memset(write_data, 0, 3 * BLOCK_SIZE);
    
    const char *msg1 = "Block 1: First block of data\n";
    const char *msg2 = "Block 2: Second block of data\n";
    const char *msg3 = "Block 3: Third block of data\n";
    
    strncpy(write_data, msg1, strlen(msg1));
    strncpy(write_data + BLOCK_SIZE, msg2, strlen(msg2));
    strncpy(write_data + 2 * BLOCK_SIZE, msg3, strlen(msg3));
    
    // Write file
    MDNStatus status = metadatanode_write_file(fid, write_data, 3 * BLOCK_SIZE);
    if (status != MDN_SUCCESS) {
        printf("Failed to write file\n");
        free(write_data);
        metadatanode_exit();
        return 0;
    }
    
    // Read file back
    void *read_data;
    size_t file_size;
    status = metadatanode_read_file(fid, &read_data, &file_size);
    if (status != MDN_SUCCESS) {
        printf("Failed to read file\n");
        free(write_data);
        metadatanode_exit();
        return 0;
    }
    
    // Verify
    if (file_size != 3 * BLOCK_SIZE) {
        printf("File size mismatch: expected %d, got %zu\n", 3 * BLOCK_SIZE, file_size);
        free(write_data);
        free(read_data);
        metadatanode_exit();
        return 0;
    }
    
    if (memcmp(write_data, read_data, 3 * BLOCK_SIZE) != 0) {
        printf("File data mismatch!\n");
        free(write_data);
        free(read_data);
        metadatanode_exit();
        return 0;
    }
    
    printf("File verified successfully\n");
    printf("Content: %.*s\n", (int)file_size, (char *)read_data);
    
    free(write_data);
    free(read_data);
    metadatanode_exit();
    return 1;
}

// Test 5: Multiple files
int test_multiple_files() {
    printf("\n=== Test 5: Multiple Files ===\n");
    
    metadatanode_init(4, 20 * BLOCK_SIZE, "roundrobin");
    
    const char *filenames[] = {"file1.txt", "file2.txt", "file3.txt", "file4.txt"};
    const char *messages[] = {
        "Content of file 1",
        "Content of file 2",
        "Content of file 3",
        "Content of file 4"
    };
    
    int fids[4];
    
    // Create and write files
    for (int i = 0; i < 4; i++) {
        MDNStatus status = metadatanode_create_file(filenames[i], BLOCK_SIZE, &fids[i]);
        if (status != MDN_SUCCESS) {
            printf("Failed to create file %s\n", filenames[i]);
            metadatanode_exit();
            return 0;
        }
        
        char buffer[BLOCK_SIZE];
        memset(buffer, 0, BLOCK_SIZE);
        strncpy(buffer, messages[i], BLOCK_SIZE - 1);
        
        status = metadatanode_write_block(fids[i], 0, buffer);
        if (status != MDN_SUCCESS) {
            printf("Failed to write to file %s\n", filenames[i]);
            metadatanode_exit();
            return 0;
        }
    }
    
    // Read and verify files
    for (int i = 0; i < 4; i++) {
        void *data;
        size_t size;
        MDNStatus status = metadatanode_read_file(fids[i], &data, &size);
        if (status != MDN_SUCCESS) {
            printf("Failed to read file %s\n", filenames[i]);
            metadatanode_exit();
            return 0;
        }
        
        printf("File %s: %.*s\n", filenames[i], (int)size, (char *)data);
        free(data);
    }
    
    metadatanode_exit();
    return 1;
}

// Test 6: Test different allocation policies
int test_allocation_policies() {
    printf("\n=== Test 6: Allocation Policies ===\n");
    
    const char *policies[] = {"rand", "roundrobin", "leastloaded", "fileaware", "weightedroundrobin"};
    
    for (int p = 0; p < 5; p++) {
        printf("\n--- Testing policy: %s ---\n", policies[p]);
        
        MDNStatus status = metadatanode_init(3, 15 * BLOCK_SIZE, policies[p]);
        if (status != MDN_SUCCESS) {
            printf("Failed to initialize with policy %s\n", policies[p]);
            return 0;
        }
        
        // Create a few files
        for (int i = 0; i < 5; i++) {
            char filename[64];
            snprintf(filename, sizeof(filename), "test_%d.txt", i);
            
            int fid;
            status = metadatanode_create_file(filename, BLOCK_SIZE, &fid);
            if (status != MDN_SUCCESS) {
                printf("Failed to create file with policy %s\n", policies[p]);
                metadatanode_exit();
                return 0;
            }
            
            // Write some data
            char buffer[BLOCK_SIZE];
            memset(buffer, 0, BLOCK_SIZE);
            snprintf(buffer, BLOCK_SIZE, "Data for file %d with policy %s", i, policies[p]);
            metadatanode_write_block(fid, 0, buffer);
        }
        
        printf("Successfully created 5 files with policy %s\n", policies[p]);
        
        metadatanode_exit();
    }
    
    return 1;
}

// Test 7: Capacity limits
int test_capacity_limits() {
    printf("\n=== Test 7: Capacity Limits ===\n");
    
    // Initialize with space for exactly 5 blocks
    metadatanode_init(2, 5 * BLOCK_SIZE, "roundrobin");
    
    // Try to create 6 single-block files (should fail on 6th)
    int fids[6];
    int success_count = 0;
    
    for (int i = 0; i < 6; i++) {
        char filename[64];
        snprintf(filename, sizeof(filename), "file_%d.txt", i);
        
        MDNStatus status = metadatanode_create_file(filename, BLOCK_SIZE, &fids[i]);
        if (status == MDN_SUCCESS) {
            success_count++;
        } else if (status == MDN_NO_SPACE) {
            printf("Correctly ran out of space at file %d\n", i);
            break;
        } else {
            printf("Unexpected error when creating file %d\n", i);
            metadatanode_exit();
            return 0;
        }
    }
    
    if (success_count != 5) {
        printf("Expected to create 5 files, but created %d\n", success_count);
        metadatanode_exit();
        return 0;
    }
    
    printf("Capacity limit correctly enforced\n");
    
    metadatanode_exit();
    return 1;
}

// Test 8: File truncation (if implemented)
int test_truncate_file() {
    printf("\n=== Test 8: File Truncation ===\n");
    
    metadatanode_init(2, 20 * BLOCK_SIZE, "roundrobin");
    
    // Create file with 3 blocks
    int fid;
    MDNStatus status = metadatanode_create_file("trunctest.txt", 3 * BLOCK_SIZE, &fid);
    if (status != MDN_SUCCESS) {
        printf("Failed to create file\n");
        metadatanode_exit();
        return 0;
    }
    
    // Write data to all blocks
    for (int i = 0; i < 3; i++) {
        char buffer[BLOCK_SIZE];
        memset(buffer, 0, BLOCK_SIZE);
        snprintf(buffer, BLOCK_SIZE, "Block %d content", i);
        metadatanode_write_block(fid, i, buffer);
    }
    
    printf("Created file with 3 blocks\n");
    
    // Truncate to 1 block
    status = metadatanode_truncate_file(fid, BLOCK_SIZE);
    if (status != MDN_SUCCESS) {
        printf("Failed to truncate file\n");
        metadatanode_exit();
        return 0;
    }
    
    printf("Truncated file to 1 block\n");
    
    // Verify file now has 1 block
    void *data;
    size_t size;
    status = metadatanode_read_file(fid, &data, &size);
    if (status != MDN_SUCCESS) {
        printf("Failed to read truncated file\n");
        metadatanode_exit();
        return 0;
    }
    
    if (size != BLOCK_SIZE) {
        printf("Truncated file has wrong size: %zu (expected %d)\n", size, BLOCK_SIZE);
        free(data);
        metadatanode_exit();
        return 0;
    }
    
    printf("Truncated file content: %s\n", (char *)data);
    free(data);
    
    // Grow file to 5 blocks
    status = metadatanode_truncate_file(fid, 5 * BLOCK_SIZE);
    if (status != MDN_SUCCESS) {
        printf("Failed to grow file\n");
        metadatanode_exit();
        return 0;
    }
    
    printf("Grew file to 5 blocks\n");
    
    // Verify
    status = metadatanode_read_file(fid, &data, &size);
    if (status != MDN_SUCCESS || size != 5 * BLOCK_SIZE) {
        printf("Failed to verify grown file\n");
        if (data) free(data);
        metadatanode_exit();
        return 0;
    }
    
    printf("File successfully grown to %zu bytes\n", size);
    free(data);
    
    metadatanode_exit();
    return 1;
}

int main(void) {
    printf("========================================\n");
    printf("DFS Functionality Test Suite\n");
    printf("========================================\n");
    
    int passed = 0;
    int total = 8;
    
    passed += test_init_and_exit();
    passed += test_create_and_find_file();
    passed += test_write_and_read_block();
    passed += test_write_and_read_file();
    passed += test_multiple_files();
    passed += test_allocation_policies();
    passed += test_capacity_limits();
    passed += test_truncate_file();
    
    printf("\n========================================\n");
    printf("Test Results: %d/%d passed\n", passed, total);
    printf("========================================\n");
    
    if (passed == total) {
        printf("✓ All tests passed!\n");
        return 0;
    } else {
        printf("✗ Some tests failed\n");
        return 1;
    }
}

