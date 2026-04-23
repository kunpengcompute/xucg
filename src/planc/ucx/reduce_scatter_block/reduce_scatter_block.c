/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "reduce_scatter_block.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx reduce_scatter_block"

static ucg_plan_attr_t ucg_planc_ucx_reduce_scatter_block_plan_attr[] = {
    {ucg_planc_ucx_reduce_scatter_block_linear_prepare,
     1, "linear", PLAN_DOMAIN},

    {ucg_planc_ucx_reduce_scatter_block_ring_prepare,
     2, "ring", PLAN_DOMAIN},


    {NULL},
};

static ucg_config_field_t reduce_scatter_block_config_table[] = {

    {NULL}
};
UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_REDUCE_SCATTER_BLOCK, reduce_scatter_block_config_table,
                                    sizeof(ucg_planc_ucx_reduce_scatter_block_config_t))
                                    
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_REDUCE_SCATTER_BLOCK,
                             ucg_planc_ucx_reduce_scatter_block_plan_attr);

static ucg_plan_policy_t reduce_scatter_block[] = {
    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t reduce_scatter_block_8192[] = {
    {1,  {0, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {8192, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t reduce_scatter_block_16384[] = {
    {1,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t reduce_scatter_block_32768[] = {
    {1,  {0, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {32768, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t reduce_scatter_block_65536[] = {
    {1,  {0, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t reduce_scatter_block_131072[] = {
    {1,  {0, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t reduce_scatter_block_LG_LG[] = {   // >512*16
    {2,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

const ucg_plan_policy_t *ucg_planc_ucx_get_reduce_scatter_block_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level)
{
    ucg_plan_policy_t *policy = NULL;
    if (node_level == NODE_LEVEL_8 && ppn_level == PPN_LEVEL_32) {
        policy = reduce_scatter_block_8192;
    } else if (node_level == NODE_LEVEL_16 && ppn_level == PPN_LEVEL_16) {
        policy = reduce_scatter_block_16384;
    } else if (
        (node_level == NODE_LEVEL_8 && ppn_level == PPN_LEVEL_64) ||
        (node_level == NODE_LEVEL_16 && ppn_level == PPN_LEVEL_32) ||
        (node_level == NODE_LEVEL_16 && ppn_level == PPN_LEVEL_64) ||
        (node_level == NODE_LEVEL_32 && ppn_level == PPN_LEVEL_64)
    ) {
        policy = reduce_scatter_block_32768;
    } else if (
        (node_level == NODE_LEVEL_8 && ppn_level == PPN_LEVEL_16) ||
        (node_level == NODE_LEVEL_32 && ppn_level == PPN_LEVEL_16) ||
        (node_level == NODE_LEVEL_32 && ppn_level == PPN_LEVEL_32) ||
        (node_level == NODE_LEVEL_64 && ppn_level == PPN_LEVEL_16) ||
        (node_level == NODE_LEVEL_64 && ppn_level == PPN_LEVEL_32)
    ) {
        policy = reduce_scatter_block_65536;
    } else if (node_level == NODE_LEVEL_128 && ppn_level == PPN_LEVEL_16) {
        policy = reduce_scatter_block_131072;
    } else if (
        (node_level == NODE_LEVEL_LG && ppn_level == PPN_LEVEL_32) ||
        (node_level == NODE_LEVEL_LG && ppn_level == PPN_LEVEL_64) ||
        (node_level == NODE_LEVEL_LG && ppn_level == PPN_LEVEL_LG)
    ) {
        policy = reduce_scatter_block_LG_LG;
    } else {
        policy = reduce_scatter_block;
    }
    return policy;
}