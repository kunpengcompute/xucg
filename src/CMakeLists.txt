#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

# Generate version
configure_file("${CMAKE_CURRENT_SOURCE_DIR}/ucg/api/ucg_version.h.in"
               "${CMAKE_CURRENT_SOURCE_DIR}/ucg/api/ucg_version.h")

option(UCG_ENABLE_PROFILE "Enable profiling" OFF)
option(UCG_ENABLE_GCOV "Enable code coverage" OFF)
option(UCG_ENABLE_CHECK_PARAMS "Enable checking parameter" ON)
option(UCG_ENABLE_MT "Enable thread-safe support" OFF)

add_compile_options(-fPIC)
if (CMAKE_BUILD_TYPE MATCHES "Release")
    set(CMAKE_C_FLAGS_RELEASE "-O3 -funwind-tables")
    set(CMAKE_CXX_FLAGS_RELEASE "-O3 -funwind-tables")
    if (CMAKE_HOST_SYSTEM_PROCESSOR MATCHES "aarch64")
        set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -mcpu=tsv110 -march=armv8.2-a")
        set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -mcpu=tsv110 -march=armv8.2-a")
    endif()
    message("-- Set CMAKE_C_FLAGS: '${CMAKE_C_FLAGS_RELEASE}'")
    message("-- Set CMAKE_CXX_FLAGS: '${CMAKE_CXX_FLAGS_RELEASE}'")
endif()

if (CMAKE_BUILD_TYPE MATCHES "Debug")
    add_definitions(-DUCG_ENABLE_DEBUG)
endif()

if (UCG_ENABLE_PROFILE MATCHES "ON")
    add_definitions(-DUCG_ENABLE_PROFILE)
endif()

if (UCG_ENABLE_GCOV MATCHES "ON")
    add_compile_options(-g -fprofile-arcs -ftest-coverage)
    link_libraries(gcov)
endif()

add_definitions(-DUCG_ENABLE_CHECK_PARAMS)
if (UCG_ENABLE_CHECK_PARAMS MATCHES "OFF")
    remove_definitions(-DUCG_ENABLE_CHECK_PARAMS)
endif()

if (UCG_ENABLE_MT MATCHES "ON")
    add_definitions(-DUCG_ENABLE_MT)
endif()

# Offering to underlying module to set link option
set(UCG_LINKER_FLAGS "" CACHE INTERNAL "UCG link options")

add_subdirectory(core)
add_subdirectory(planc)
add_subdirectory(util)

# According to CMP0004, strip the trailing whitespace
string(STRIP ${UCG_LINKER_FLAGS} UCG_LINKER_FLAGS)

# Build libucg.so
add_library(ucg SHARED ucg/api/ucg.h)
set_target_properties(ucg PROPERTIES LINKER_LANGUAGE C)
set_target_properties(ucg
                      PROPERTIES VERSION
                      ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}
                      SOVERSION
                      ${PROJECT_VERSION_MAJOR})
set(UCG_PUBLIC_HEADER
    ${CMAKE_CURRENT_SOURCE_DIR}/ucg/api/ucg.h
    ${CMAKE_CURRENT_SOURCE_DIR}/ucg/api/ucg_version.h)
set_target_properties(ucg PROPERTIES PUBLIC_HEADER "${UCG_PUBLIC_HEADER}")
target_link_libraries(ucg PRIVATE
                      "-Wl,--whole-archive"
                      ucg_core
                      ucg_planc
                      ucg_util
                      "-Wl,--no-whole-archive"
                      ${UCG_LINKER_FLAGS})

# Install
install(TARGETS ucg
        PUBLIC_HEADER DESTINATION ${UCG_INSTALL_INCLUDEDIR}/ucg/api
        LIBRARY DESTINATION ${UCG_INSTALL_LIBDIR})