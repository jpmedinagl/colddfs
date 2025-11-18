#include <stdio.h>

#include "datanode.h"
#include "metadatanode.h"

int main(void) {
    printf("Hello, CMake C project!\n");

    metadatanode_init(1, 3 * BLOCK_SIZE, "roundrobin");

    int fid;
    metadatanode_create_file("hello.c", 256, &fid);

    char * wbuffer = malloc(BLOCK_SIZE);
    memset(wbuffer, 0, BLOCK_SIZE);
    wbuffer[BLOCK_SIZE - 1] = '\0';

    const char *msg = "Hello, metadata node!\nI love you!";
    strncpy(wbuffer, msg, BLOCK_SIZE - 1);
    
    metadatanode_write_block(fid, 0, wbuffer);
    free(wbuffer);
    
    void * buffer;
    size_t size;
    if (metadatanode_read_file(fid, &buffer, &size) != MDN_SUCCESS) {
        fprintf(stderr, "Failed to read file\n");
        return 1;
    }

    printf("%.*s\n", (int)size, (char *)buffer);
    free(buffer);
    
    metadatanode_exit(0);
    
    return 0;
}
