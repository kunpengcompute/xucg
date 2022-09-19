#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

# Build ucg_mpi
file(GLOB_RECURSE SRCS ./*.c)
add_executable(ucg_mpi ${SRCS})