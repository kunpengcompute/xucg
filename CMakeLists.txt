#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

cmake_minimum_required(VERSION 3.14)

project(xucg VERSION 2.0.0)

## Set policy
cmake_policy(SET CMP0048 NEW)

# Define options
option(UCG_BUILD_TOOLS "Build ucg tools" ON)
option(UCG_BUILD_TESTS "Build ucg tests" ON)

# Set base include search path.
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)
if (IS_DIRECTORY ${UCG_BUILD_WITH_UCX})
    # Currently the util & tests header file includes some UCS header file,
    # so the path of the UCS header file is needed.
    include_directories(${UCG_BUILD_WITH_UCX}/include)
endif()
if (IS_DIRECTORY ${UCG_BUILD_WITH_HCCL})
    # Currently the tests header file includes some HCCL header file,
    # so the path of the HCCL header file is needed.
    include_directories(${UCG_BUILD_WITH_HCCL}/include)
endif()

# Set installation directories
set(UCG_INSTALL_BINDIR ${CMAKE_INSTALL_PREFIX}/bin)
set(UCG_INSTALL_INCLUDEDIR ${CMAKE_INSTALL_PREFIX}/include)
set(UCG_INSTALL_LIBDIR ${CMAKE_INSTALL_PREFIX}/lib)
set(UCG_INSTALL_PLANCDIR ${CMAKE_INSTALL_PREFIX}/lib/planc)

# Make all warnings into errors
add_compile_options(-Wall -Werror)

# Other targets depend on target under src.
add_subdirectory(src)

if (UCG_BUILD_TOOLS MATCHES "ON")
    add_subdirectory(tools)
endif()

if (UCG_BUILD_TESTS MATCHES "ON")
    enable_testing()
    add_subdirectory(tests)
endif()