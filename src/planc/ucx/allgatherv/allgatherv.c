/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allgatherv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx allgatherv"

static ucg_plan_attr_t ucg_planc_ucx_allgatherv_plan_attr[] = {
    {ucg_planc_ucx_allgatherv_neighbor_prepare,
     1, "Neighbor exchange", PLAN_DOMAIN},

    {ucg_planc_ucx_allgatherv_ring_prepare,
     2, "Ring", PLAN_DOMAIN},

    {ucg_planc_ucx_allgatherv_ring_hpl_prepare,
     3, "Ring-HPL", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_ALLGATHERV,
                             ucg_planc_ucx_allgatherv_plan_attr);

UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_ALLGATHERV, NULL, 0)

void ucg_planc_ucx_allgatherv_set_plan_attr(ucg_vgroup_t *vgroup,
                                            ucg_plan_attr_t *default_plan_attr)
{
    ucg_plan_attr_t *attr;
    for (attr = default_plan_attr; !UCG_PLAN_ATTR_IS_LAST(attr); ++attr) {
        ucg_plan_range_t range = {0, UCG_PLAN_RANGE_MAX};
        attr->range = range;
        attr->score = UCG_PLANC_UCX_DEFAULT_SCORE;
    }

    ucg_group_t *group = vgroup->group;
    int32_t ppn = group->topo->ppn;
    int32_t node_cnt = group->size / ppn;
    if (ppn == UCG_TOPO_PPX_UNBALANCED) {
        return;
    }
    const int32_t score = UCG_PLANC_UCX_DEFAULT_SCORE + 1;
    if (node_cnt <= 4) {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 8192, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 4096, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 2048, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 2048, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 16, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 16, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 262144, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, UCG_PLAN_RANGE_MAX, score);
        }
    } else if (node_cnt <= 8) {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 8192, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 4096, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 2048, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 2048, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 65536, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 65536, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 65536, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 65536, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 262144, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 524288, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, UCG_PLAN_RANGE_MAX, score);
        }
    } else {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 8192, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 128, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 4096, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 131072, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 131072, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 16384, 65536, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 65536, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 262144, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 524288, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 2, 0, 512, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 512, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 2, 1024, UCG_PLAN_RANGE_MAX, score);
        }
    }
    return;
}