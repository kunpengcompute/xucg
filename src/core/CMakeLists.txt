#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

# Find all source files
file(GLOB_RECURSE SRCS ./*.c)

# Build libucg_core.a
add_library(ucg_core STATIC ${SRCS})
