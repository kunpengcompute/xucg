/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */


#include "bcast.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx bcast"

static ucg_plan_attr_t ucg_planc_ucx_bcast_plan_attr[] = {
    {ucg_planc_ucx_bcast_bntree_prepare,
     1, "Binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_na_bntree_prepare,
     2, "Node-aware binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_na_kntree_and_bntree_prepare,
     3, "Node-aware k-nomial tree and binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_na_kntree_prepare,
     4, "Node-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_na_inc_prepare,
     5, "Node-aware in-network-computing", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_ring_prepare,
     6, "Ring", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_nta_kntree_prepare,
     7, "Net-topo-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_van_de_geijn_prepare,
     8, "van de Geijn(scatter+allgather)", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_kntree_prepare,
     10, "K-nomial tree", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_BCAST,
                             ucg_planc_ucx_bcast_plan_attr);

static ucg_config_field_t bcast_config_table[] = {
    {"BCAST_KNTREE_DEGREE", "8",
     "Configure the k value in kntree algo for bcast",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, kntree_degree),
     UCG_CONFIG_TYPE_INT},

    {"BCAST_NTA_KNTREE_INTER_DEGREE", "2",
     "Configure the k value between subnets in net-topo-aware kntree algo for bcast",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, nta_kntree_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"BCAST_NTA_KNTREE_INTRA_DEGREE", "2",
     "Configure the k value in a subnet in net-topo-aware kntree algo for bcast",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, nta_kntree_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {"BCAST_NA_KNTREE_INTER_DEGREE", "8",
     "Configure the k value between nodes in node-aware kntree algo for bcast",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, na_kntree_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"BCAST_NA_KNTREE_INTRA_DEGREE", "2",
     "Configure the k value in a node in node-aware kntree algo for bcast",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, na_kntree_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {"BCAST_ROOT_ADJUST", "n",
     "Adjustment of non-zero root processes",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, root_adjust),
     UCG_CONFIG_TYPE_BOOL},

    {NULL}
};
UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_BCAST, bcast_config_table,
                                    sizeof(ucg_planc_ucx_bcast_config_t))

void ucg_planc_ucx_bcast_set_plan_attr(ucg_vgroup_t *vgroup,
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
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 64, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 64, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 32768, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 131072, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 131072, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 256, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 256, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 8, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 8, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 1024, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 32768, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 128, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, UCG_PLAN_RANGE_MAX, score);
        }
    } else if (node_cnt <= 8) {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 3, 8, 2048, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 2048, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 32768, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 32768, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 131072, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, UCG_PLAN_RANGE_MAX, score);
        }
    } else {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 2048, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 2048, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 32768, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 262144, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 524288, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 32768, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 1, 131072, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 524288, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 3, 0, 8, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 8, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 128, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 10, 16384, UCG_PLAN_RANGE_MAX, score);
        }
    }
    return;
}