#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

# Build ucg_info
file(GLOB SRCS ./*.c)
add_executable(ucg_info ${SRCS})
target_link_libraries(ucg_info ucg ucs pthread)

if (IS_DIRECTORY ${UCG_BUILD_WITH_UCX})
    target_link_directories(ucg_info PRIVATE ${UCG_BUILD_WITH_UCX}/lib)
endif()

if (UCG_BUILD_PLANC_UCX MATCHES "ON")
    add_definitions(-DUCG_BUILD_PLANC_UCX)
    target_include_directories(ucg_info PRIVATE
                               ${CMAKE_SOURCE_DIR}/src/planc/ucx)
endif()

if (UCG_BUILD_PLANC_HCCL MATCHES "ON")
    add_definitions(-DUCG_BUILD_PLANC_HCCL)
    target_include_directories(ucg_info PRIVATE
                               ${CMAKE_SOURCE_DIR}/src/planc/hccl)
    if (IS_DIRECTORY ${UCG_BUILD_WITH_HCCL})
        target_include_directories(ucg_info PRIVATE ${UCG_BUILD_WITH_HCCL}/include)
        target_link_directories(ucg_info PRIVATE ${UCG_BUILD_WITH_HCCL}/lib64)
    endif()

    target_link_libraries(ucg_info ascendcl)
endif()

# Install
install(TARGETS ucg_info
        RUNTIME DESTINATION ${UCG_INSTALL_BINDIR})