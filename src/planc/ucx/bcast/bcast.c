/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2024. All rights reserved.
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

    // {ucg_planc_ucx_bcast_nta_kntree_prepare,
    //  7, "Net-topo-aware k-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_van_de_geijn_prepare,
     8, "van de Geijn(scatter+allgather)", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_kntree_prepare,
     10, "K-nomial tree", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_long_prepare,
     11, "Long(scatter+allgather)", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_inc_ring_m_prepare,
     12, "increasing-ring(modified)", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_inc_2_ring_m_prepare,
     13, "increasing-2-ring(modified)", PLAN_DOMAIN},

    {ucg_planc_ucx_bcast_long_m_prepare,
     14, "Long(modified)", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_BCAST,
                             ucg_planc_ucx_bcast_plan_attr);

static ucg_config_field_t bcast_config_table[] = {
    {"BCAST_KNTREE_DEGREE", "4",
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

    {"BCAST_MIN_SEND_BATCH", "auto",
    "Configure the send batch mode minimum boundary in van de geijn algo for bcast",
    ucg_offsetof(ucg_planc_ucx_bcast_config_t, min_bsend),
    UCG_CONFIG_TYPE_MEMUNITS},

    {"BCAST_MAX_SEND_BATCH", "auto",
    "Configure the send batch mode maximum boundary in van de geijn algo for bcast",
    ucg_offsetof(ucg_planc_ucx_bcast_config_t, max_bsend),
    UCG_CONFIG_TYPE_MEMUNITS},

    {"BCAST_DEFAULT_POLICY", "y",
     "Enable default policy\n"
     " - y : use default policy\n"
     " - n : don't use default policy",
     ucg_offsetof(ucg_planc_ucx_bcast_config_t, policy_default),
     UCG_CONFIG_TYPE_BOOL},

    {NULL}
};

UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_BCAST, bcast_config_table,
                                    sizeof(ucg_planc_ucx_bcast_config_t))

static ucg_plan_policy_t bcast_4_1[] = {
    {10, {0, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4,  {8192, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {131072, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_4_4[] = {
    {4,  {0, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {64, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_4_8[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_4_16[] = {
    {10,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t bcast_4_16_default[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t bcast_4_32[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_4_64[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_4_LG[] = {
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t bcast_4_LG_default[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t bcast_8_1[] = {
    {10, {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4,  {128, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {32768, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {10,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_8_4[] = {
    {4,  {0, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {32768, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_8_8[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {262144, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {10, {262144, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_8_16[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_8_32[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_8_64[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_8_LG[] = {
    {4,  {0, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {32768, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_1[] = {
    {10, {0, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {32768, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {10, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_4[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {262144, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {10, {262144, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_8[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_16[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {524288, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_32[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_64[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_16_LG[] = {
    {1,  {0, 8}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4,  {8, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_1[] = {
    {4, {0, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {32768, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {65536, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_4[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_8[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_16[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_32[] = {
    {4,  {0, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {65536, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_64[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t bcast_LG_LG[] = {
    {4,  {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {10, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t* bcast_plan_policy[] = {
    bcast_4_1,
    bcast_4_4,
    bcast_4_8,
    bcast_4_16,
    bcast_4_32,
    bcast_4_64,
    bcast_4_LG,
    bcast_8_1,
    bcast_8_4,
    bcast_8_8,
    bcast_8_16,
    bcast_8_32,
    bcast_8_64,
    bcast_8_LG,
    bcast_16_1,
    bcast_16_4,
    bcast_16_8,
    bcast_16_16,
    bcast_16_32,
    bcast_16_64,
    bcast_16_LG,
    bcast_LG_1,
    bcast_LG_4,
    bcast_LG_8,
    bcast_LG_16,
    bcast_LG_32,
    bcast_LG_64,
    bcast_LG_LG,
};

static ucg_plan_policy_t* bcast_plan_policy_default[] = {
    bcast_4_1,
    bcast_4_4,
    bcast_4_8,
    bcast_4_16_default,
    bcast_4_32,
    bcast_4_64,
    bcast_4_LG_default,
    bcast_8_1,
    bcast_8_4,
    bcast_8_8,
    bcast_8_16,
    bcast_8_32,
    bcast_8_64,
    bcast_8_LG,
    bcast_16_1,
    bcast_16_4,
    bcast_16_8,
    bcast_16_16,
    bcast_16_32,
    bcast_16_64,
    bcast_16_LG,
    bcast_LG_1,
    bcast_LG_4,
    bcast_LG_8,
    bcast_LG_16,
    bcast_LG_32,
    bcast_LG_64,
    bcast_LG_LG,
};

const ucg_plan_policy_t *ucg_planc_ucx_get_bcast_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                             ucg_planc_ucx_ppn_level_t ppn_level,
                                                             ucg_planc_ucx_group_t *ucx_group)
{
    int idx = node_level * PPN_LEVEL_NUMS + ppn_level;
    ucg_assert(idx < (sizeof(bcast_plan_policy) / sizeof(bcast_plan_policy[0])));
    ucg_plan_policy_t *policy;
    ucg_planc_ucx_bcast_config_t *config;
    if (ucg_planc_ucx_context_config_builtin_check(ucx_group->context, UCG_COLL_TYPE_BCAST)) {
        config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, bcast,
                                                             UCG_COLL_TYPE_BCAST);
        if (config->policy_default) {
            policy = bcast_plan_policy_default[idx];
        } else {
            policy = bcast_plan_policy[idx];
        }
    } else {
        policy = bcast_plan_policy_default[idx];
    }
    return policy;
}