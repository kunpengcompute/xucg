/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2025. All rights reserved.
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

    {ucg_planc_ucx_allgatherv_linear_prepare,
     4, "Linear", PLAN_DOMAIN},

    {ucg_planc_ucx_allgatherv_bruck_prepare,
     5, "Bruck", PLAN_DOMAIN},

    {ucg_planc_ucx_allgatherv_na_rolling_prepare,
     6, "Node-aware rolling", PLAN_DOMAIN},

    {NULL},
};
UCG_PLAN_ATTR_REGISTER_TABLE(ucg_planc_ucx, UCG_COLL_TYPE_ALLGATHERV,
                             ucg_planc_ucx_allgatherv_plan_attr);

static ucg_config_field_t allgatherv_config_table[] = {
    {"ALLGATHERV_DEFAULT_POLICY", "y",
     "Enable default policy\n"
     " - y : use default policy\n"
     " - n : don't use default policy",
     ucg_offsetof(ucg_planc_ucx_allgatherv_config_t, policy_default),
     UCG_CONFIG_TYPE_BOOL},

    {NULL}
};

UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_ALLGATHERV, allgatherv_config_table,
                                    sizeof(ucg_planc_ucx_allgatherv_config_t))

static ucg_plan_policy_t allgatherv_4_1[] = {
    {4, {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_4[] = {
    {1, {0, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {8192, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 8192}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_8[] = {
    {1, {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {4096, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_16[] = {
    {2, {8192, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_16_default[] = {
    {1, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {2048, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_32[] = {
    {1, {0, 16}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {16, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 16}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_64[] = {
    {2, {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6, {16384, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {32768, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1, {16384, 32768}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {3, {16384, 262144}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_LG[] = {
    {2, {1024, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_4_LG_default[] = {
    {2, {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_1[] = {
    {4, {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_4[] = {
    {5, {0, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4, {512, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {4096, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6, {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {65536, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {1, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {3, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_8[] = {
    {5, {0, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {1024, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6, {32768, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {32768, 262144}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_16[] = {
    {5, {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6, {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {1, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {3, {16384, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_16_default[] = {
    {1, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {2048, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {65536, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {1, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {3, {16384, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_32[] = {
    {2, {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {3, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_64[] = {
    {2, {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {65536, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {262144, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {262144, 524288}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_8_LG[] = {
    {5, {0, 32}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {32, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {1024, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {1024, 131072}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_1[] = {
    {4, {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_4[] = {
    {5, {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {4096, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_8[] = {
    {5, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {2048, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_16[] = {
    {1, {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {4096, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {131072, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {16384, 131072}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_32[] = {
    {2, {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {16384, 131072}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_64[] = {
    {2, {0, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {65536, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {262144, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3, {262144, 524288}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_16_LG[] = {
    {5, {0, 16}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {16, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {1024, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3, {32768, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {1024, 32768}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_LG_1[] = {
    {5, {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4, {256, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_LG_4[] = {
    {5, {0, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4, {2048, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {4096, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_LG_8[] = {
    {5, {0, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2, {1024, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allgatherv_LG_16[] = {
    {1, {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t* allgatherv_plan_policy[] = {
    allgatherv_4_1,
    allgatherv_4_4,
    allgatherv_4_8,
    allgatherv_4_16,
    allgatherv_4_32,
    allgatherv_4_64,
    allgatherv_4_LG,
    allgatherv_8_1,
    allgatherv_8_4,
    allgatherv_8_8,
    allgatherv_8_16,
    allgatherv_8_32,
    allgatherv_8_64,
    allgatherv_8_LG,
    allgatherv_16_1,
    allgatherv_16_4,
    allgatherv_16_8,
    allgatherv_16_16,
    allgatherv_16_32,
    allgatherv_16_64,
    allgatherv_16_LG,
    allgatherv_LG_1,
    allgatherv_LG_4,
    allgatherv_LG_8,
    allgatherv_16_16, // allgatherv_LG_16
    allgatherv_16_32, // allgatherv_LG_32
    allgatherv_16_64, // allgatherv_LG_64
    allgatherv_16_LG, // allgatherv_LG_LG
};

static ucg_plan_policy_t* allgatherv_plan_policy_default[] = {
    allgatherv_4_1,
    allgatherv_4_4,
    allgatherv_4_8,
    allgatherv_4_16_default,
    allgatherv_4_32,
    allgatherv_4_64,
    allgatherv_4_LG_default,
    allgatherv_8_1,
    allgatherv_8_4,
    allgatherv_8_8,
    allgatherv_8_16_default,
    allgatherv_8_32,
    allgatherv_8_64,
    allgatherv_8_LG,
    allgatherv_16_1,
    allgatherv_16_4,
    allgatherv_16_8,
    allgatherv_16_16,
    allgatherv_16_32,
    allgatherv_16_64,
    allgatherv_16_LG,
    allgatherv_LG_1,
    allgatherv_LG_4,
    allgatherv_LG_8,
    allgatherv_LG_16, // allgatherv_LG_16
    allgatherv_LG_16, // allgatherv_LG_32
    allgatherv_LG_16, // allgatherv_LG_64
    allgatherv_LG_16, // allgatherv_LG_LG
};

const ucg_plan_policy_t *ucg_planc_ucx_get_allgatherv_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                                  ucg_planc_ucx_ppn_level_t ppn_level,
                                                                  ucg_planc_ucx_group_t *ucx_group)
{
    int idx = node_level * PPN_LEVEL_NUMS + ppn_level;
    ucg_assert(idx < (sizeof(allgatherv_plan_policy) / sizeof(allgatherv_plan_policy[0])));
    ucg_plan_policy_t *policy;
    ucg_planc_ucx_allgatherv_config_t *config;
    if (ucg_planc_ucx_context_config_builtin_check(ucx_group->context, UCG_COLL_TYPE_ALLGATHERV)) {
        config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, allgatherv,
                                                             UCG_COLL_TYPE_ALLGATHERV);
        if (config->policy_default) {
            policy = allgatherv_plan_policy_default[idx];
        } else {
            policy = allgatherv_plan_policy[idx];
        }
    } else {
        policy = allgatherv_plan_policy_default[idx];
    }
    return policy;
}
