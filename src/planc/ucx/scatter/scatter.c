/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025, All rights reserved.
 */

#include "scatter.h"
#include "planc_ucx_plan.h"
#include "planc_ucx_global.h"


#define PLAN_DOMAIN "planc ucx scatter"

static ucg_plan_attr_t ucg_planc_ucx_scatter_plan_attr[] = {
    {ucg_planc_ucx_scatter_linear_prepare,
     1, "Linear", PLAN_DOMAIN},

    {ucg_planc_ucx_scatter_kntree_prepare,
     2, "Knomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_scatter_na_kntree_prepare,
     3, "Node-aware K-nomial tree", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_SCATTER,
                             ucg_planc_ucx_scatter_plan_attr);

static ucg_config_field_t scatter_config_table[] = {
    {"SCATTER_MIN_SEND_BATCH", "auto",
     "Configure the send batch mode minimum boundary in linear algo for scatter",
     ucg_offsetof(ucg_planc_ucx_scatter_config_t, min_bsend),
     UCG_CONFIG_TYPE_MEMUNITS},

    {"SCATTER_MAX_SEND_BATCH", "auto",
     "Configure the send batch mode maximum boundary in linear algo for scatter",
     ucg_offsetof(ucg_planc_ucx_scatter_config_t, max_bsend),
     UCG_CONFIG_TYPE_MEMUNITS},

    {"SCATTER_KNTREE_DEGREE", "2",
     "Configure the k value in kntree algo for scatter",
     ucg_offsetof(ucg_planc_ucx_scatter_config_t, kntree_degree),
     UCG_CONFIG_TYPE_INT},

    {"SCATTER_NA_KNTREE_INTER_DEGREE", "2",
     "Configure the k value between nodes in node-aware kntree algo for scatter",
     ucg_offsetof(ucg_planc_ucx_scatter_config_t, na_kntree_inter_degree),
     UCG_CONFIG_TYPE_INT},

    {"SCATTER_NA_KNTREE_INTRA_DEGREE", "2",
     "Configure the k value in a node in node-aware kntree algo for scatter",
     ucg_offsetof(ucg_planc_ucx_scatter_config_t, na_kntree_intra_degree),
     UCG_CONFIG_TYPE_INT},

    {NULL}
};
UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_SCATTER, scatter_config_table,
                                    sizeof(ucg_planc_ucx_scatter_config_t))

static ucg_plan_policy_t scatter_plan_policy[] = {
    {3,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t scatter_4_16[] = {
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t scatter_8_16[] = {
    {2,  {0, 16385}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

const ucg_plan_policy_t *ucg_planc_ucx_get_scatter_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                                ucg_planc_ucx_ppn_level_t ppn_level)
{
    ucg_plan_policy_t *policy = NULL;
    if (node_level == NODE_LEVEL_8 && ppn_level == PPN_LEVEL_16) {
        policy = scatter_8_16;
    } else if (node_level == NODE_LEVEL_4 && ppn_level == PPN_LEVEL_16) {
        policy = scatter_4_16;
    } else {
        policy = scatter_plan_policy;
    }
    return policy;
}