cmake_minimum_required(VERSION 3.10)

project(colddfs C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

include_directories(include)

add_executable(workload 
    src/main.c
    src/metadatanode.c
    src/datanode.c
    src/communication.c
    src/allocationpolicy.c
    src/rand.c
    src/roundrobin.c
    src/bitmap.c
)
