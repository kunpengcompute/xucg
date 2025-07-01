#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
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
        if(SUPPORT_CMAKE3 MATCHES "ON")
            target_include_directories(ucg_planc_ucx PRIVATE ${UCG_BUILD_WITH_UCX}/include)
            target_link_directories(ucg_planc_ucx PRIVATE ${UCG_BUILD_WITH_UCX}/lib)
        else()
            include_directories(${UCG_BUILD_WITH_UCX}/include)
            find_library(UCP ucp HINTS ${UCG_BUILD_WITH_UCX}/lib)
        endif()
    endif()
    if(SUPPORT_CMAKE3 MATCHES "ON")
        target_include_directories(ucg_planc_ucx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(ucg_planc_ucx PRIVATE ucp)
    else()
        target_include_directories(ucg_planc_ucx PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
        target_link_libraries(ucg_planc_ucx PRIVATE ${UCP})
    endif()

    # Install
    install(TARGETS ucg_planc_ucx
            LIBRARY DESTINATION ${UCG_INSTALL_PLANCDIR})
endif()