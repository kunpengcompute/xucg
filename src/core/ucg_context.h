/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2022-2022. All rights reserved.
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

/** Get process information */
#define UCG_PROC_INFO(_context, _rank) \
    (ucg_proc_info_t*)((_context)->procs.info + (_rank) * (_context)->procs.stride)

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

typedef struct ucg_addr_desc {
    uint32_t len;
    uint32_t offset;
} ucg_addr_desc_t;

/**
 * process information layout:
 * --------------------------------------------------------------------------
 * |...|num_addr_desc|addr_desc[0]|...|addr_desc[N]|address[0]|...|address[N]
 * --------------------------------------------------------------------------
 * addr_desc[i].offset is the offset of address[i] in the layout.
 */
typedef struct ucg_proc_info {
    // total size of packed information
    uint32_t size;
    ucg_location_t location;
    uint32_t num_addr_desc;
    // description of address of all planc
    ucg_addr_desc_t addr_desc[0];
    // address of planc
} ucg_proc_info_t;

typedef struct ucg_proc_info_array {
    uint32_t count;  /* number of process information */
    uint32_t stride; /* size of process information */
    uint8_t *info;   /* point to process information array */
} ucg_proc_info_array_t;

typedef struct ucg_context {
    ucg_proc_info_array_t procs;
    int32_t num_planc_rscs;
    ucg_resource_planc_t *planc_rscs;
    ucg_list_link_t plist; /* progress list */
    ucg_oob_group_t oob_group;
    ucg_get_location_cb_t get_location;
    ucg_thread_mode_t thread_mode;
    /* Ensure thread-safe of all resources in the context. */
    ucg_lock_t mt_lock;
    /* pool of @ref ucg_plan_meta_op_t */
    ucg_mpool_t meta_op_mp;
} ucg_context_t;

/**
 * @brief Get address of process.
 *
 * @param [in] context      UCG Context.
 * @param [in] rank         Context rank.
 * @param [in] planc        Plan component.
 */
void* ucg_context_get_proc_addr(ucg_context_t *context, ucg_rank_t rank, ucg_planc_t *planc);

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