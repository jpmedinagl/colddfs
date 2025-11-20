cmake_minimum_required(VERSION 3.10)

project(colddfs C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -O2")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -O0 -g")

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
    src/fileaware.c
    src/weightedroundrobin.c
	src/metric.c
	src/sequential.c
)

add_library(colddfs STATIC ${LIB_SRCS})

# ----------------------
# Workloads
# ----------------------
set(WORKLOAD_SRCS
    workload/simple.c
	workload/fill.c
	workload/mixed.c
	workload/readheavy.c
	workload/scaling.c
	workload/imbalance.c
	workload/distribution.c
	
	workload/block.c
	workload/dist.c
	workload/datascale.c
	workload/fileawaretune.c
)
set(WORKLOAD_TARGETS "")

foreach(WORKLOAD_SRC ${WORKLOAD_SRCS})
    get_filename_component(WORKLOAD_NAME ${WORKLOAD_SRC} NAME_WE)
    add_executable(${WORKLOAD_NAME} ${WORKLOAD_SRC})
    target_link_libraries(${WORKLOAD_NAME} PRIVATE colddfs m)
    target_include_directories(${WORKLOAD_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    list(APPEND WORKLOAD_TARGETS ${WORKLOAD_NAME})
endforeach()

# ----------------------
# Experiments
# ----------------------
set(EXP_SRCS
    exp/rand.c
    exp/roundrobin.c
    exp/leastloaded.c
    exp/fileaware.c
    exp/weightedroundrobin.c

    exp/medium_file.c

	exp/truncation.c

	exp/prepostallocate.c
)
set(EXP_TARGETS "")

foreach(EXP_SRC ${EXP_SRCS})
    get_filename_component(EXP_NAME ${EXP_SRC} NAME_WE)
    add_executable(${EXP_NAME} ${EXP_SRC})
    target_link_libraries(${EXP_NAME} PRIVATE colddfs)
    target_include_directories(${EXP_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/include)
    list(APPEND EXP_TARGETS ${EXP_NAME})
endforeach()

# add_custom_target(work DEPENDS ${WORKLOAD_TARGETS})

set(DN_DIRS "dn_*")

add_custom_target(extra_clean
	COMMAND /bin/sh -c "rm -rf ${CMAKE_CURRENT_BINARY_DIR}/${DN_DIRS}"
	COMMENT "Removing ${DN_DIRS} directories"
)

