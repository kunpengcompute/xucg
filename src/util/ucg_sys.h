/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

#ifndef UCG_SYS_H_
#define UCG_SYS_H_

#include <stddef.h>

 /**
  * @return Generate a random number in the rage 0..RAND_MAX
  */
int ucg_rand();

 /**
  * @return Huge page size on the system, or -1 if unsupported.
  */
size_t ucg_get_huge_page_size();

 /**
  * @return Regular page size on the system
  */
size_t ucg_get_page_size();

#endif