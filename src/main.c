#include <stdio.h>

#include "metadatanode.h"

int main(void) {
    printf("Hello, CMake C project!\n");

    metadatanode_init(3, 3 * 1024);

    metadatanode_exit();

    return 0;
}
