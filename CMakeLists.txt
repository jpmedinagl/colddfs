cmake_minimum_required(VERSION 3.10)

project(colddfs C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

include_directories(${CMAKE_SOURCE_DIR}/include)

# -------
# LIBRARY
# -------
set(LIB_SRCS
    src/metadatanode.c
	src/datanode.c
    src/communication.c
    src/bitmap.c
	src/allocationpolicy.c    
	src/rand.c
    src/roundrobin.c
	src/leastloaded.c
)

add_library(colddfs STATIC ${LIB_SRCS})

# ----------------------
# Workloads
# ----------------------
set(WORKLOAD_SRCS
    workload/simple.c
)
set(WORKLOAD_TARGETS "")

foreach(WORKLOAD_SRC ${WORKLOAD_SRCS})
    get_filename_component(WORKLOAD_NAME ${WORKLOAD_SRC} NAME_WE)
    add_executable(${WORKLOAD_NAME} ${WORKLOAD_SRC})
    target_link_libraries(${WORKLOAD_NAME} PRIVATE colddfs)
    target_include_directories(${WORKLOAD_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    list(APPEND WORKLOAD_TARGETS ${WORKLOAD_NAME})
endforeach()

# add_custom_target(work DEPENDS ${WORKLOAD_TARGETS})

