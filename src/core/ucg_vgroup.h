/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_VGROUP_H_
#define UCG_VGROUP_H_

#include "ucg_def.h"

/**
 * @brief Virtual group
 *
 * The rank number of the virtual group always starts from 0, and max rank is
 * (size - 1). All collective operation can design algorithms based on this
 * assumption, which simplifies the implementation.
 */
typedef struct ucg_vgroup {
    ucg_rank_t myrank;
    uint32_t size;
    /** Convert vgroup rank to group rank. */
    ucg_rank_map_t rank_map;
    /** Original group. */
    ucg_group_t *group;
} ucg_vgroup_t;

#endif