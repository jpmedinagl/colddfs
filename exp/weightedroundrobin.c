#include <stdio.h>

#include "datanode.h"
#include "metadatanode.h"

void create_write_file(const char * filename, int fsize, const char * msg) {
    int fid;
    metadatanode_create_file(filename, fsize, &fid);

    char * wbuffer = malloc(BLOCK_SIZE);
    memset(wbuffer, 0, BLOCK_SIZE);
    wbuffer[BLOCK_SIZE - 1] = '\0';

    strncpy(wbuffer, msg, BLOCK_SIZE - 1);
    
    metadatanode_write_block(fid, 0, wbuffer);
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

    metadatanode_init(4, 4 * BLOCK_SIZE, "weightedroundrobin");

    create_write_file("first", 512, "Just\nwriting\nI\nlove\nyou\n!\nfirst file\n");
    create_write_file("second", 512, "Just\nwriting\nI\nlove\nyou\n!\nsecond file\n");
    create_write_file("third", 512, "Just\nwriting\nI\nlove\nyou\n!\nthird file\n");
    create_write_file("fourth", 512, "Just\nwriting\nI\nlove\nyou\n!\nfourth file\n");
    
    metadatanode_exit(0);
    
    return 0;
}
