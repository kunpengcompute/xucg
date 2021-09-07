/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_TOPO_H
#define UCG_BUILTIN_TOPO_H

#include <ucg/api/ucg.h>

typedef struct ucg_builtin_topo_org {
    unsigned num;
    unsigned idx;
    /* "leader_cnt" need not be same with "num" */
    unsigned leader_cnt;
    unsigned member_cnt;
    ucg_group_member_index_t *leaders;
    ucg_group_member_index_t *members;
} ucg_builtin_topo_org_t;

typedef struct ucg_builtin_topo_local {
    ucg_builtin_topo_org_t socket;
    ucg_builtin_topo_org_t L3cache;
} ucg_builtin_topo_local_t;

typedef struct ucg_builtin_topo_params {
    unsigned num_local_procs;
    unsigned node_cnt;
    ucg_group_member_index_t my_index;
    ucg_group_member_index_t num_procs;
    ucg_group_member_index_t *local_members;
    ucg_group_member_index_t *node_leaders;
    ucg_builtin_topo_local_t local;
} ucg_builtin_topo_params_t;

ucs_status_t ucg_builtin_query_topo(const ucg_group_params_t *group_params,
                                    ucg_builtin_topo_params_t *topo_params);

void ucg_builtin_destroy_topo(ucg_builtin_topo_params_t *topo_params);

#endif