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

    // {ucg_planc_ucx_allreduce_nta_kntree_prepare,
    //  15, "Net-topo-aware k-nomial tree", PLAN_DOMAIN},

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

    {"ALLREDUCE_DEFAULT_POLICY", "y",
     "Enable default policy\n"
     " - y : use default policy\n"
     " - n : don't use default policy",
     ucg_offsetof(ucg_planc_ucx_allreduce_config_t, policy_default),
     UCG_CONFIG_TYPE_BOOL},

    {NULL}
};
UCG_PLANC_UCX_BUILTIN_ALGO_REGISTER(UCG_COLL_TYPE_ALLREDUCE, allreduce_config_table,
                                    sizeof(ucg_planc_ucx_allreduce_config_t))

static ucg_plan_policy_t allreduce_4_1[] = {
    {1,  {0, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {1024, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_4_4[] = {
    {1,  {0, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {1024, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {4096, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4,  {65536, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {1024, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_4_8[] = {
    {1,  {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {128, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {8192, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4,  {131072, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {128, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_4_16[] = {
    {1,  {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {128, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {128, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allreduce_4_16_default[] = {
    {1,  {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {128, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {8192, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {32768, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {128, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allreduce_4_32[] = {
    {1,  {0, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {64, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {4096, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {256, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_4_64[] = {
    {6,  {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {4096, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_4_LG[] = {
    {6,  {0, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {64, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allreduce_4_LG_default[] = {
    {6,  {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3,  {4096, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allreduce_8_1[] = {
    {2,  {0, 16}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {7,  {16, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {64, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {4096, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {2,  {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,

};
static ucg_plan_policy_t allreduce_8_4[] = {
    {1,  {0, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {1024, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {4096, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {32768, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {1024, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_8_8[] = {
    {1,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1024, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {256, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_8_16[] = {
    {1,  {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {128, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {2048, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {128, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_8_32[] = {
    {5,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1024, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {8192, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {32768, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {262144, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {256, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_8_64[] = {
    {8,  {0, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {64, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {8192, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {32768, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {65536, 262144}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {262144, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {6,  {8192, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_8_LG[] = {
    {2,  {0, 16}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {16, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {512, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3,  {4096, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {16384, 131072}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {131072, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_16_1[] = {
    {7,  {0, 32}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {1,  {32, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {512, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {8192, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t allreduce_16_4[] = {
    {1,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {512, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {1024, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {2048, 32768}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {32768, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {256, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_16_8[] = {
    {1,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {7,  {256, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {512, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1024, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {4,  {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {512, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_16_16[] = {
    {1,  {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {128, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {512, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {128, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_16_32[] = {
    {6,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1024, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {2048, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {4096, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {8192, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {256, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_16_64[] = {
    {8,  {0, 16}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {16, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3,  {4096, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {8192, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {8192, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_16_LG[] = {
    {2,  {0, 32}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {32, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {2,  {2048, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_1[] = {
    {1,  {0, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {1,  {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_4[] = {
    {1,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {256, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {1024, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {4096, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {5,  {256, 262144}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {4,  {262144, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_8[] = {
    {1,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {7,  {256, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1024, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {6,  {1024, 524288}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {4,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_16[] = {
    {3,  {0, 32}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {8,  {32, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {64, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {3,  {128, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {512, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {1024, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {2048, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {256, 524288}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {4,  {524288, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_32[] = {
    {6,  {0, 256}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {256, 1024}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {1024, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {2048, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {4096, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {8192, 524288}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {524288, 1048576}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {1048576, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {4,  {256, 4096}, UCG_PLAN_UCX_PLAN_SCORE_2ND},
    {3,  {4096, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_64[] = {
    {7,  {0, 64}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {5,  {64, 2048}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {2048, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {16384, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};
static ucg_plan_policy_t allreduce_LG_LG[] = {
    {7,  {0, 128}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {5,  {128, 512}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {6,  {512, 4096}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {7,  {4096, 8192}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {13, {8192, 16384}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {14, {16384, 65536}, UCG_PLAN_UCX_PLAN_SCORE_1ST},
    {12, {65536, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_1ST},

    {3,  {8192, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_2ND},

    {1,  {0, UCG_PLAN_RANGE_MAX}, UCG_PLAN_UCX_PLAN_SCORE_3RD},
    UCG_PLAN_LAST_POLICY,
};

static ucg_plan_policy_t* allreduce_plan_policy[] = {
    allreduce_4_1,
    allreduce_4_4,
    allreduce_4_8,
    allreduce_4_16,
    allreduce_4_32,
    allreduce_4_64,
    allreduce_4_LG,
    allreduce_8_1,
    allreduce_8_4,
    allreduce_8_8,
    allreduce_8_16,
    allreduce_8_32,
    allreduce_8_64,
    allreduce_8_LG,
    allreduce_16_1,
    allreduce_16_4,
    allreduce_16_8,
    allreduce_16_16,
    allreduce_16_32,
    allreduce_16_64,
    allreduce_16_LG,
    allreduce_LG_1,
    allreduce_LG_4,
    allreduce_LG_8,
    allreduce_LG_16,
    allreduce_LG_32,
    allreduce_LG_64,
    allreduce_LG_LG,
};

static ucg_plan_policy_t* allreduce_plan_policy_default[] = {
    allreduce_4_1,
    allreduce_4_4,
    allreduce_4_8,
    allreduce_4_16_default,
    allreduce_4_32,
    allreduce_4_64,
    allreduce_4_LG_default,
    allreduce_8_1,
    allreduce_8_4,
    allreduce_8_8,
    allreduce_8_16,
    allreduce_8_32,
    allreduce_8_64,
    allreduce_8_LG,
    allreduce_16_1,
    allreduce_16_4,
    allreduce_16_8,
    allreduce_16_16,
    allreduce_16_32,
    allreduce_16_64,
    allreduce_16_LG,
    allreduce_LG_1,
    allreduce_LG_4,
    allreduce_LG_8,
    allreduce_LG_16,
    allreduce_LG_32,
    allreduce_LG_64,
    allreduce_LG_LG,
};

const ucg_plan_policy_t *ucg_planc_ucx_get_allreduce_plan_policy(ucg_planc_ucx_node_level_t node_level,
                                                                 ucg_planc_ucx_ppn_level_t ppn_level,
                                                                 ucg_planc_ucx_group_t *ucx_group)
{
    int idx = node_level * PPN_LEVEL_NUMS + ppn_level;
    ucg_assert(idx < (sizeof(allreduce_plan_policy) / sizeof(allreduce_plan_policy[0])));
    ucg_plan_policy_t *policy;
    ucg_planc_ucx_allreduce_config_t *config;
    if (ucg_planc_ucx_context_config_builtin_check(ucx_group->context, UCG_COLL_TYPE_ALLREDUCE)) {
        config = UCG_PLANC_UCX_CONTEXT_BUILTIN_CONFIG_BUNDLE(ucx_group->context, allreduce,
                                                             UCG_COLL_TYPE_ALLREDUCE);
        if (config->policy_default) {
            policy = allreduce_plan_policy_default[idx];
        } else {
            policy = allreduce_plan_policy[idx];
        }
    } else {
        policy = allreduce_plan_policy_default[idx];
    }
    return policy;
}