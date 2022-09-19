/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "barrier.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"


#define PLAN_DOMAIN "planc ucx barrier"

static ucg_plan_attr_t ucg_planc_ucx_barrier_plan_attr[] = {
    {ucg_planc_ucx_barrier_rd_prepare,
     1, "Recursive doubling", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_na_rd_and_bntree_prepare,
     2, "Node-aware recursive doubling and binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_sa_rd_and_bntree_prepare,
     3, "Socket-aware recursive doubling and binomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_na_rd_and_kntree_prepare,
     4, "Node-aware recursive doubling and k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_sa_rd_and_kntree_prepare,
     5, "Socket-aware recursive doubling and k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_na_kntree_prepare,
     6, "Node-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_sa_kntree_prepare,
     7, "Socket-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_na_inc_prepare,
     8, "Node-aware in-network-computing", PLAN_DOMAIN},

    {ucg_planc_ucx_barrier_sa_inc_prepare,
     9, "Socket-aware in-network-computing", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_BARRIER,
                             ucg_planc_ucx_barrier_plan_attr);

static ucg_config_field_t barrier_config_table[] = {
    {"BARRIER_FANIN_INTER_DEGREE", "8",
     "Configure the k value between nodes in node-aware fanin kntree algo for barrier",
     ucg_offsetof(ucg_planc_ucx_barrier_config_t, fanin_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"BARRIER_FANOUT_INTER_DEGREE", "8",
     "Configure the k value between nodes in node-aware fanout kntree algo for barrier",
     ucg_offsetof(ucg_planc_ucx_barrier_config_t, fanout_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"BARRIER_FANIN_INTRA_DEGREE", "4",
     "Configure the k value in a node in node-aware fanin kntree algo for barrier",
     ucg_offsetof(ucg_planc_ucx_barrier_config_t, fanin_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {"BARRIER_FANOUT_INTRA_DEGREE", "2",
     "Configure the k value in a node in node-aware fanout kntree algo for barrier",
     ucg_offsetof(ucg_planc_ucx_barrier_config_t, fanout_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {NULL}
};

UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_BARRIER, barrier_config_table,
                                    sizeof(ucg_planc_ucx_barrier_config_t))

void ucg_planc_ucx_barrier_set_plan_attr(ucg_vgroup_t *vgroup,
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
        if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 16) {
            ucg_plan_attr_array_update(default_plan_attr, 1, 0, UCG_PLAN_RANGE_MAX, score);
        } else if (ppn <= 32) {
            ucg_plan_attr_array_update(default_plan_attr, 5, 0, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 7, 0, UCG_PLAN_RANGE_MAX, score);
        }
    } else if (node_cnt <= 8) {
        if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 6, 0, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 7, 0, UCG_PLAN_RANGE_MAX, score);
        }
    } else {
        if (ppn <= 8) {
            ucg_plan_attr_array_update(default_plan_attr, 4, 0, UCG_PLAN_RANGE_MAX, score);
        } else {
            ucg_plan_attr_array_update(default_plan_attr, 5, 0, UCG_PLAN_RANGE_MAX, score);
        }
    }
    return;
}