/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "allreduce.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx allreduce"

static ucg_plan_attr_t ucg_planc_ucx_allreduce_plan_attr[] = {
    {ucg_planc_ucx_allreduce_rd_prepare,
     1, "Recursive doubling", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_na_rd_and_bntree_prepare,
     2, "Node-aware recursive doubling and binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_sa_rd_and_bntree_prepare,
     3, "Socket-aware recursive doubling and binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_ring_prepare,
     4, "Ring", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_na_rd_and_kntree_prepare,
     5, "Node-aware recursive doubling and k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_sa_rd_and_kntree_prepare,
     6, "Socket-aware recursive doubling and k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_na_kntree_prepare,
     7, "Node-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_sa_kntree_prepare,
     8, "Socket-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_na_inc_prepare,
     9, "Node-aware in-network-computing", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_sa_inc_prepare,
     10, "Socket-aware in-network-computing", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_rabenseifner_prepare,
     12, "Rabenseifner", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_na_rabenseifner_prepare,
     13, "Node-aware rabenseifner", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_sa_rabenseifner_prepare,
     14, "Socket-aware rabenseifner", PLAN_DOMAIN},

    {ucg_planc_ucx_allreduce_nta_kntree_prepare,
     15, "Net-topo-aware k-nomial tree", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_ALLREDUCE,
                             ucg_planc_ucx_allreduce_plan_attr);

static ucg_config_field_t allreduce_config_table[] = {
    {"ALLREDUCE_FANIN_INTER_DEGREE", "8",
     "Configure the k value between nodes in node-aware fanin kntree algo for allreduce",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, fanin_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"ALLREDUCE_FANOUT_INTER_DEGREE", "8",
     "Configure the k value between nodes in node-aware fanout kntree algo for allreduce",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, fanout_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"ALLREDUCE_FANIN_INTRA_DEGREE", "4",
     "Configure the k value in a node in node-aware fanin kntree algo for allreduce",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, fanin_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {"ALLREDUCE_FANOUT_INTRA_DEGREE", "2",
     "Configure the k value in a node in node-aware fanout kntree algo for allreduce",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, fanout_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {"ALLREDUCE_NTA_KNTREE_INTER_DEGREE", "8",
     "Configure the k value between subnets in net-topo-aware kntree algo for allreduce",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, nta_kntree_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"ALLREDUCE_NTA_KNTREE_INTRA_DEGREE", "8",
     "Configure the k value in a subnet in net-topo-aware kntree algo for allreduce",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, nta_kntree_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {NULL}
};
UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_ALLREDUCE, allreduce_config_table,
                                    sizeof(ucg_planc_ucx_allreduce_config_t))

void ucg_planc_ucx_allreduce_set_plan_attr(ucg_vgroup_t *vgroup,
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
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 1024, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 65536, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 65536, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 524288, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 512, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 512, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 8192, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 131072, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 262144, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 524288, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 128, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 8192, 131072, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 131072, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 16, score);
            ucg_plan_attr_array_update(default_plan_attr, 6, 16, 256, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 256, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 524288, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 1024, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 1048576, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 8192, UCG_PLAN_RANGE_MAX, score);
        }
    } else if (node_cnt <= 8) {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 5, 0, 256, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 256, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 32768, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 128, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 32768, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 128, 2048, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 2048, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 16, score);
            ucg_plan_attr_array_update(default_plan_attr, 8, 16, 64, score);
            ucg_plan_attr_array_update(default_plan_attr, 6, 64, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 1024, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 262144, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 262144, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 1024, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 8192, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 16384, 65536, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 65536, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 524288, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 7, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 6, 128, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 8192, UCG_PLAN_RANGE_MAX, score);
        }
    } else {
        if (ppn <= 4) {
            ucg_plan_attr_array_update(default_plan_attr, 5, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 6, 128, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 1024, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 32768, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 128, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 16384, 524288, score);
            ucg_plan_attr_array_update(default_plan_attr, 4, 524288, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 128, 2048, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 2048, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 16384, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 1024, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 1024, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 4096, 32768, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 32768, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 64) {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 4096, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 4096, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 13, 8192, 16384, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 16384, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, 32, score);
            ucg_plan_attr_array_update(default_plan_attr, 3, 32, 128, score);
            ucg_plan_attr_array_update(default_plan_attr, 6, 128, 8192, score);
            ucg_plan_attr_array_update(default_plan_attr, 14, 8192, 1048576, score);
            ucg_plan_attr_array_update(default_plan_attr, 12, 1048576, UCG_PLAN_RANGE_MAX, score);
        }
    }
    return;
}