#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
#

cmake_minimum_required(VERSION 2.8.12)

if(POLICY CMP0048)
    set(SUPPORT_CMAKE3 "ON")
else()
    set(SUPPORT_CMAKE3 "OFF")
endif()

if(SUPPORT_CMAKE3 MATCHES "ON")
    cmake_policy(SET CMP0048 NEW)
    if(NOT HPCKIT_VERSION)
        project(xucg VERSION 1.0.0)
    else()
        project(xucg VERSION ${HPCKIT_VERSION})
    endif()
else()
    project(xucg C CXX)
    if(NOT HPCKIT_VERSION)
        set(PROJECT_VERSION_MAJOR 1)
        set(PROJECT_VERSION_MINOR 0)
        set(PROJECT_VERSION_PATCH 0)
    else()
        set(PROJECT_VERSION ${HPCKIT_VERSION})
    endif()
endif()

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
