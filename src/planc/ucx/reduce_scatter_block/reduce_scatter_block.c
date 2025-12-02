/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "reduce_scatter_block.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx reduce_scatter_block"

static ucg_plan_attr_t ucg_planc_ucx_reduce_scatter_block_plan_attr[] = {
    {ucg_planc_ucx_reduce_scatter_block_ring_prepare,
     1, "ring", PLAN_DOMAIN},


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

static ucg_plan_policy_t* reduce_scatter_block_plan_policy[] = {
    reduce_scatter_block,
};

const ucg_plan_policy_t *ucg_planc_ucx_get_reduce_scatter_block_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level)
{
    ucg_plan_policy_t *policy = reduce_scatter_block_plan_policy[0];
    return policy;
}