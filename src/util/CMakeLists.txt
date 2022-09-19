#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

# Get cache line size
execute_process(COMMAND cat /sys/devices/system/cpu/cpu0/cache/index0/coherency_line_size
                TIMEOUT 3
                RESULT_VARIABLE result_var
                OUTPUT_VARIABLE output_var
                OUTPUT_STRIP_TRAILING_WHITESPACE
                ERROR_STRIP_TRAILING_WHITESPACE)
if (${result_var} STREQUAL "0")
    set(CACHE_LINE_SIZE ${output_var})
else()
    set(CACHE_LINE_SIZE 64)
endif()
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/ucg_cpu.h.in"
               "${CMAKE_CURRENT_SOURCE_DIR}/ucg_cpu.h")

# Find all source files
file(GLOB_RECURSE SRCS ./*.c)

set(UCG_LINKER_FLAGS "-lucs ${UCG_LINKER_FLAGS}" CACHE INTERNAL "UCG link options")
if (IS_DIRECTORY ${UCG_BUILD_WITH_UCX})
    set(UCG_LINKER_FLAGS
        "-L${UCG_BUILD_WITH_UCX}/lib ${UCG_LINKER_FLAGS}"
        CACHE INTERNAL "UCG link options")
    # Makes it easier to run at build time
    set(CMAKE_BUILD_RPATH "${UCG_BUILD_WITH_UCX}/lib" PARENT_SCOPE)
    # Add ucx header
    include_directories(${UCG_BUILD_WITH_UCX}/include)
endif()

# Build libucg_util.a
add_library(ucg_util STATIC ${SRCS})