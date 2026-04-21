/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
 */

#ifndef UCG_PLANC_UCX_DEF_H_
#define UCG_PLANC_UCX_DEF_H_

typedef struct ucg_planc_ucx_group ucg_planc_ucx_group_t;
typedef struct ucg_planc_ucx_op ucg_planc_ucx_op_t;
typedef struct ucg_planc_ucx_context ucg_planc_ucx_context_t;

typedef enum ucg_planc_ucx_ppn_level {
    PPN_LEVEL_1,        /* 1 */
    PPN_LEVEL_4,        /* 2-4 */
    PPN_LEVEL_8,        /* 5-8 */
    PPN_LEVEL_16,       /* 9-16 */
    PPN_LEVEL_32,       /* 17-32 */
    PPN_LEVEL_64,       /* 33-64 */
    PPN_LEVEL_LG,       /* >64 */
    /* The new ppn level must be added above */
    PPN_LEVEL_NUMS
} ucg_planc_ucx_ppn_level_t;

typedef enum ucg_planc_ucx_node_level {
    NODE_LEVEL_4,       /* 1-4 */
    NODE_LEVEL_8,       /* 5-8 */
    NODE_LEVEL_16,      /* 9-16 */
    NODE_LEVEL_32,      /* 17-32 */
    NODE_LEVEL_64,      /* 33-64 */
    NODE_LEVEL_128,     /* 65-128 */
    NODE_LEVEL_256,     /* 129-256 */
    NODE_LEVEL_512,     /* 257-512 */
    NODE_LEVEL_LG,      /* >512 */
    /* The new node level must be added above */
    NODE_LEVEL_NUMS
} ucg_planc_ucx_node_level_t;

#endif