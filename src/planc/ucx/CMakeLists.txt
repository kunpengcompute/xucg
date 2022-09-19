#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#

option(UCG_BUILD_PLANC_UCX "Build plan component ucx" ON)

if (UCG_BUILD_PLANC_UCX MATCHES "ON")
    # Find all source files
    file(GLOB_RECURSE SRCS ./*.c)

    # Build libucg_planc_ucx.so
    add_library(ucg_planc_ucx SHARED ${SRCS})
    set_target_properties(ucg_planc_ucx
                          PROPERTIES VERSION
                          ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}
                          SOVERSION
                          ${PROJECT_VERSION_MAJOR})

    if (IS_DIRECTORY ${UCG_BUILD_WITH_UCX})
        target_include_directories(ucg_planc_ucx PRIVATE ${UCG_BUILD_WITH_UCX}/include)
        target_link_directories(ucg_planc_ucx PRIVATE ${UCG_BUILD_WITH_UCX}/lib)
    endif()
    target_include_directories(ucg_planc_ucx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
    target_link_libraries(ucg_planc_ucx PRIVATE ucp)

    # Install
    install(TARGETS ucg_planc_ucx
            LIBRARY DESTINATION ${UCG_INSTALL_PLANCDIR})
endif()