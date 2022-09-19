#
# Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
#
# Dependencies of test_planc.cpp

# Build planc fake.
file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/planc_fake.c
     "#include \"planc/ucg_planc.h\"
      ucg_planc_t UCG_PLANC_OBJNAME(fake) = {
          .super.name = \"fake\",
      };
      ucg_planc_t UCG_PLANC_OBJNAME(fake2) = {
        .super.name = \"fake2\",
    };")

add_library(ucg_planc_fake SHARED planc_fake.c)
add_library(ucg_planc_fake2 SHARED planc_fake.c)
# For test case: load_default_path. libucg.so is under ${CMAKE_BINARY_DIR}/src
add_custom_command(TARGET ucg_planc_fake POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ucg_planc_fake>
                   ${CMAKE_BINARY_DIR}/src/planc)
add_custom_command(TARGET ucg_planc_fake2 POST_BUILD
                   COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:ucg_planc_fake2>
                   ${CMAKE_BINARY_DIR}/src/planc)
# For test case: load_user_define_path
add_definitions(-DUCG_GTEST_PLANC_USER_DEFINE_PATH="${CMAKE_CURRENT_BINARY_DIR}")