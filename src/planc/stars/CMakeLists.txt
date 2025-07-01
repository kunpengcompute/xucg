#
# Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
#

option(UCG_BUILD_PLANC_STARS "Build plan component stars" OFF)

if (UCG_BUILD_PLANC_STARS MATCHES "ON")
    option(UCG_ENABLE_STARS_STATS "Enable stars stats" OFF)

    add_definitions(-DHAVE_CONFIG_H)

    if (UCG_ENABLE_STARS_STATS MATCHES "ON")
        add_definitions(-DENABLE_STARS_STATS)
    endif()

    set(STARS_INC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../../3rdparty/stars)
    set(PLANC_STARS_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    set(UCG_SRC_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../../)

    # Find all source files
    file(GLOB_RECURSE SRCS ./*.c)

    # Build libucg_planc_stars.so
    add_library(ucg_planc_stars SHARED ${SRCS})
    set_target_properties(ucg_planc_stars
                          PROPERTIES VERSION
                          ${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}
                          SOVERSION
                          ${PROJECT_VERSION_MAJOR})

    # Make all warnings into errors
    target_compile_options(ucg_planc_stars PUBLIC
                           -fPIC
                           -Wall
                           -Werror
                           -Wno-unused-variable)


    if (IS_DIRECTORY ${UCG_BUILD_RDMA_CORE_PREFIX})
        target_include_directories(ucg_planc_stars PRIVATE ${UCG_BUILD_RDMA_CORE_PREFIX}/include)
        target_link_directories(ucg_planc_stars PRIVATE ${UCG_BUILD_RDMA_CORE_PREFIX}/lib)
        message(RDMA_CORE_PREFIX:${UCG_BUILD_RDMA_CORE_PREFIX})
    endif()

    target_include_directories(ucg_planc_stars PUBLIC
                               .
                               ${STARS_INC_DIR}
                               ${UCG_BUILD_WITH_UCX}/include
                               ${UCG_SRC_DIR}
                               ${UCG_SRC_DIR}/planc/ucx
                               ${PLANC_STARS_SRC_DIR}/algorithm
                               ${PLANC_STARS_SRC_DIR}/comm/base
                               ${PLANC_STARS_SRC_DIR}/comm/base/arch
                               ${PLANC_STARS_SRC_DIR}/comm/protocol
                               ${PLANC_STARS_SRC_DIR}/comm/transport
                               ${PLANC_STARS_SRC_DIR}/comm/transport/base
                               ${PLANC_STARS_SRC_DIR}/comm/transport/ib/base
                               ${PLANC_STARS_SRC_DIR}/comm/transport/ib/rc
                               ${PLANC_STARS_SRC_DIR}/comm/transport/ib/rc/base
                               ${PLANC_STARS_SRC_DIR}/comm/transport/sm/sdma)

    target_link_directories(ucg_planc_stars PRIVATE
                            ${UCG_BUILD_WITH_UCX}/lib)

    target_link_libraries(ucg_planc_stars PRIVATE numa ibverbs hns ucp)

    # Install
    install(TARGETS ucg_planc_stars
            LIBRARY DESTINATION ${UCG_INSTALL_PLANCDIR})
endif()
