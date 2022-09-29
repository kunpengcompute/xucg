#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

option(UCG_BUILD_PLANC_HCCL "Build plan component hccl" OFF)

if (UCG_BUILD_PLANC_HCCL MATCHES "ON")
    # Find all source files
    file(GLOB_RECURSE SRCS ./*.c)

    # Build libucg_planc_hccl.so
    add_library(ucg_planc_hccl SHARED ${SRCS})
    set_target_properties(ucg_planc_hccl
                          PROPERTIES VERSION
                          ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}
                          SOVERSION
                          ${PROJECT_VERSION_MAJOR})

    if (IS_DIRECTORY ${UCG_BUILD_WITH_HCCL})
        target_include_directories(ucg_planc_hccl PRIVATE ${UCG_BUILD_WITH_HCCL}/include)
        target_link_directories(ucg_planc_hccl PRIVATE ${UCG_BUILD_WITH_HCCL}/lib64)
    endif()
    target_include_directories(ucg_planc_hccl PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(ucg_planc_hccl PRIVATE hccl ascendcl)

    # Install
    install(TARGETS ucg_planc_hccl
            LIBRARY DESTINATION ${UCG_INSTALL_PLANCDIR})
endif()