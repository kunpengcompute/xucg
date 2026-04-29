/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2023. All rights reserved.
 */

#ifndef UCG_CONTEXT_H_
#define UCG_CONTEXT_H_

#include "ucg/api/ucg.h"

#include "util/ucg_parser.h"
#include "util/ucg_list.h"
#include "util/ucg_lock.h"
#include "util/ucg_mpool.h"
#include "planc/ucg_planc_def.h"

#include "ucg_def.h"

/** Get address of process */
#define UCG_PROC_ADDR(_info, _planc_idx) \
    (void*)((uint8_t*)(_info) + (_info)->addr_desc[(_planc_idx)].offset)

/** Get address length of process */
#define UCG_PROC_ADDR_LEN(_info, _planc_idx) (_info)->addr_desc[(_planc_idx)].len

typedef struct ucg_config {
    char *env_prefix;
    ucg_config_names_array_t planc;
    int32_t use_mt_mutex;
    int32_t num_planc_cfg;
    ucg_planc_config_h *planc_cfg;
} ucg_config_t;

typedef struct ucg_resource_planc {
    ucg_planc_t *planc;
    ucg_planc_context_h ctx;
} ucg_resource_planc_t;

typedef struct ucg_proc_info_array {
    uint32_t count; /* number of process information */
    uint32_t stride; /* size of process information */
    uint8_t *info; /* point to process information array */
} ucg_proc_info_array_t;

typedef struct ucg_context {
    int32_t num_planc_rscs;
    ucg_resource_planc_t *planc_rscs;
    ucg_list_link_t plist; /* progress list */
    ucg_oob_group_t oob_group;
    ucg_get_location_cb_t get_location;
    ucg_get_proc_info_cb_t get_proc_info;
    ucg_thread_mode_t thread_mode;
    /* Ensure thread-safe of all resources in the context. */
    ucg_lock_t mt_lock;
    /* pool of @ref ucg_plan_meta_op_t */
    ucg_mpool_t meta_op_mp;
    /* list of shared memory pool */
    ucg_list_link_t shmem_mp_list;
    ucg_list_link_t shmem_segment_list;
    /* list fo staging area memory pool */
    ucg_list_link_t staging_area_mp_list;
} ucg_context_t;

/**
 * @brief Get address of process.
 *
 * @param [in] context      UCG Context.
 * @param [in] rank         Context rank.
 * @param [in] planc        Plan component.
 * @param [in] proc_info    Start address ptr of proc info.
 */
void* ucg_context_get_proc_addr(ucg_context_t *context, ucg_rank_t rank, ucg_planc_t *planc, ucg_proc_info_t **proc_info);

/**
 * @brief Get location of process.
 *
 * @param [in] context      UCG Context.
 * @param [in] rank         Context rank.
 * @param [in] location     Location of the rank.
 */
ucg_status_t ucg_context_get_location(ucg_context_t *context, ucg_rank_t rank,
                                      ucg_location_t *location);

/**
 * @brief Get my context rank.
 *
 * @param [in] context      UCG Context.
 * @return ucg_rank_t
 */
static inline ucg_rank_t ucg_context_myrank(ucg_context_t *context)
{
    return context->oob_group.myrank;
}

/**
 * @brief Get context group size.
 *
 * @param [in] context      UCG Context.
 * @return uint32_t
 */
static inline uint32_t ucg_context_size(ucg_context_t *context)
{
    return context->oob_group.size;
}

static inline void ucg_context_lock(ucg_context_t *context)
{
    return ucg_lock_enter(&context->mt_lock);
}

static inline void ucg_context_unlock(ucg_context_t *context)
{
    return ucg_lock_leave(&context->mt_lock);
}

#endif