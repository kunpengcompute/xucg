#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

# Build ucg_perf
file(GLOB SRCS ./*.c)
add_executable(ucg_perf ${SRCS})

# Install
install(TARGETS ucg_perf
        RUNTIME DESTINATION ${UCG_INSTALL_BINDIR})