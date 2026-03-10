/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#include "gatherv.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"

#define PLAN_DOMAIN "planc ucx gatherv"

static ucg_plan_attr_t ucg_planc_ucx_gatherv_plan_attr[] = {
    {ucg_planc_ucx_gatherv_linear_prepare,
     1, "Linear", PLAN_DOMAIN},

    {ucg_planc_ucx_gatherv_na_linear_prepare,
     2, "Node-aware Linear", PLAN_DOMAIN},

    {ucg_planc_ucx_gatherv_kntree_prepare,
     3, "Knomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_gatherv_na_kntree_prepare,
     4, "Node-aware K-nomial tree", PLAN_DOMAIN},

    {NULL},
};

static ucg_config_field_t gatherv_config_table[] = {

    {"GATHERV_KNTREE_DEGREE", "2",
     "Configure the k value in kntree algo for gatherv",
     ucg_offsetof(ucg_planc_ucx_gatherv_config_t, kntree_degree),
     UCG_CONFIG_TYPE_INT},

    {"GATHERV_NA_KNTREE_INTER_DEGREE", "2",
     "Configure the k value between nodes in node-aware kntree algo for gatherv",
     ucg_offsetof(ucg_planc_ucx_gatherv_config_t, na_kntree_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"GATHERV_NA_KNTREE_INTRA_DEGREE", "2",
     "Configure the k value in a node in node-aware kntree algo for gatherv",
     ucg_offsetof(ucg_planc_ucx_gatherv_config_t, na_kntree_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {NULL}
};
UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_GATHERV, gatherv_config_table,
                                    sizeof(ucg_planc_ucx_gatherv_config_t))
                                    
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_GATHERV,
                             ucg_planc_ucx_gatherv_plan_attr);

static ucg_plan_policy_t gatherv[] = {
    {3,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t* gatherv_plan_policy[] = {
    gatherv,
};

const ucg_plan_policy_t *ucg_planc_ucx_get_gatherv_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                               ucg_planc_ucx_ppn_level_t ppn_level)
{
    ucg_plan_policy_t *policy = gatherv_plan_policy[0];
    return policy;
}