#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
#

cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0048)
    set(SUPPORT_CMAKE3 "ON")
else()
    set(SUPPORT_CMAKE3 "OFF")
endif()

if(NOT HPCKIT_VERSION)
    set(HPCKIT_VERSION "26.0.0")
endif()

string(REGEX MATCH "^(.+)\\.(.+)\\.(.+)$" VERSION_MATCH ${HPCKIT_VERSION})

set(HPCKIT_MAJOR ${CMAKE_MATCH_1})
set(HPCKIT_MINOR ${CMAKE_MATCH_2})
set(HPCKIT_PATCH ${CMAKE_MATCH_3})

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
endif ()

if (UCG_BUILD_TESTS MATCHES "ON")
    enable_testing()
    add_subdirectory(tests)
endif ()
