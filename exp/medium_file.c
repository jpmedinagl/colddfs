#include <stdio.h>

#include "datanode.h"
#include "metadatanode.h"

void write_file(int fid, int file_block, const char * msg) {
    char * wbuffer = malloc(BLOCK_SIZE);
    memset(wbuffer, 0, BLOCK_SIZE);
    wbuffer[BLOCK_SIZE - 1] = '\0';

    strncpy(wbuffer, msg, BLOCK_SIZE - 1);
    
    metadatanode_write_block(fid, file_block, wbuffer);
    free(wbuffer);
    
    void * buffer;
    size_t size;
    if (metadatanode_read_file(fid, &buffer, &size) != MDN_SUCCESS) {
        fprintf(stderr, "Failed to read file\n");
        return;
    }

    printf("%.*s\n", (int)size, (char *)buffer);
    free(buffer);
}

int main(void) {
    printf("Hello, CMake C project!\n");

    metadatanode_init(4, 4 * BLOCK_SIZE, "rand");

    const char * filename = "medium_file";
    int size = 4096 + 512;
    int fid;

    metadatanode_create_file(filename, size, &fid);
    write_file(fid, 0, "writing to block 0\n");
    write_file(fid, 1, "writing to block 1\n");
    
    metadatanode_exit();
    
    return 0;
}
