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

static ucg_plan_policy_t barrier_4_1[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_4_4[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_4_8[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_4_16[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_4_32[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_4_64[] = {
    {7,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_4_LG[] = {
    {7,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_1[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_4[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_8[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_16[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_32[] = {
    {7,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_64[] = {
    {7,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_8_LG[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_1[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_4[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_8[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_16[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_32[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_64[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_16_LG[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_1[] = {
    {2,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_4[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_8[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_16[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_32[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_64[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t barrier_LG_LG[] = {
    {6,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t* barrier_plan_policy[] = {
    barrier_4_1,
    barrier_4_4,
    barrier_4_8,
    barrier_4_16,
    barrier_4_32,
    barrier_4_64,
    barrier_4_LG,
    barrier_8_1,
    barrier_8_4,
    barrier_8_8,
    barrier_8_16,
    barrier_8_32,
    barrier_8_64,
    barrier_8_LG,
    barrier_16_1,
    barrier_16_4,
    barrier_16_8,
    barrier_16_16,
    barrier_16_32,
    barrier_16_64,
    barrier_16_LG,
    barrier_LG_1,
    barrier_LG_4,
    barrier_LG_8,
    barrier_LG_16,
    barrier_LG_32,
    barrier_LG_64,
    barrier_LG_LG
};

const ucg_plan_policy_t *ucg_planc_ucx_get_barrier_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level)
{
    int idx = node_level * PPN_LEVEL_NUMS + ppn_level;
    ucg_assert(idx < (sizeof(barrier_plan_policy) / sizeof(barrier_plan_policy[0])));
    ucg_plan_policy_t *policy = barrier_plan_policy[idx];
    return policy;
}