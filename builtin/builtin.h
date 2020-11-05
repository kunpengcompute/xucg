/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#ifndef UCG_BUILTIN_H
#define UCG_BUILTIN_H

#include "./plan/builtin_plan.h"
#include "./ops/builtin_ops.h"
#include <ucs/datastruct/ptr_array.h>

#define UCG_ALLOC_CHECK(_size, _name) ({ \
    void *ptr = ucs_malloc((_size), (_name)); \
    if (ptr == NULL) { \
        return UCS_ERR_NO_MEMORY; \
    } \
    ptr; \
})

typedef struct ucg_builtin_config {
    ucg_builtin_binomial_tree_config_t bmtree;
    ucg_builtin_recursive_config_t     recursive;

    unsigned                       cache_size;
    size_t                         short_max_tx;
    size_t                         bcopy_max_tx;
    unsigned                       mem_reg_opt_cnt;
    unsigned                       large_datatype_threshold;

    unsigned                       bcopy_to_zcopy_opt;
    double                         bcast_algorithm;
    double                         allreduce_algorithm;
    double                         barrier_algorithm;
    
    unsigned                       pipelining;

    unsigned                       max_msg_list_size;

    unsigned                       priority;
} ucg_builtin_config_t;

typedef struct ucg_builtin_ctx {
    ucs_ptr_array_t       group_by_id;
    uint8_t               am_id;
    ucg_builtin_config_t  config;

    ucg_context_h         context;
} ucg_builtin_ctx_t;

typedef struct ucg_builtin_planner_ctx {
    ucg_group_h               group;
    ucg_group_params_t       *group_params;
    ucg_group_id_t            group_id;
    ucg_builtin_ctx_t        *context;
    ucg_builtin_config_t     *config;
    uint16_t                  am_id;
    ucs_list_link_t           send_head;    /* request list for (re)send */
    ucs_list_link_t           plan_head;    /* for resource release */
    
    ucg_builtin_comp_slot_t   slots[UCG_BUILTIN_MAX_CONCURRENT_OPS];
} ucg_builtin_planner_ctx_t;

extern ucg_mpi_reduce_callback_t ucg_builtin_mpi_reduce_cb;
extern void *ucg_builtin_mpi_in_place;

#endif