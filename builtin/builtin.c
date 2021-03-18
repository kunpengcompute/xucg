/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */
#include "builtin.h"

#include <ucg/base/ucg_component.h>
#include <ucg/base/ucg_group.h>
#include <ucg/base/ucg_context.h>

#include <ucg/api/ucg_mpi.h>
#include <ucs/debug/memtrack.h>
#include <ucs/profile/profile.h>
#include <ucs/datastruct/ptr_array.h>
#include <string.h>

#define UCG_BUILTIN_CONFIG_PREFIX "BUILTIN_"

#define CACHE_SIZE 1000
#define RECURSIVE_FACTOR 2
#define DEFAULT_INTER_KVALUE 8
#define DEFAULT_INTRA_KVALUE 2
#define DATATYPE_ALIGN 16

ucg_mpi_reduce_callback_t ucg_builtin_mpi_reduce_cb;
void *ucg_builtin_mpi_in_place;
static ucg_mpi_op_is_commute_callback_t ucg_builtin_op_is_commute_cb;

static ucs_config_field_t ucg_builtin_config_table[] = {

    {"BMTREE_", "", NULL, ucs_offsetof(ucg_builtin_config_t, bmtree),
     UCS_CONFIG_TYPE_TABLE(ucg_builtin_binomial_tree_config_table)},

    {"BCAST_ALGORITHM", "0", "Bcast algorithm",
     ucs_offsetof(ucg_builtin_config_t, bcast_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"ALLREDUCE_ALGORITHM", "0", "Allreduce algorithm",
     ucs_offsetof(ucg_builtin_config_t, allreduce_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"BARRIER_ALGORITHM", "0", "Barrier algorithm",
     ucs_offsetof(ucg_builtin_config_t, barrier_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"MAX_MSG_LIST_SIZE", "40", "Largest loop count of msg process function",
     ucs_offsetof(ucg_builtin_config_t, max_msg_list_size), UCS_CONFIG_TYPE_UINT},

    {"MEM_REG_OPT_CNT", "10", "Operation counter before registering the memory",
     ucs_offsetof(ucg_builtin_config_t, mem_reg_opt_cnt), UCS_CONFIG_TYPE_ULUNITS},

    {"BCOPY_TO_ZCOPY_OPT", "1", "Switch for optimization from bcopy to zcopy",
     ucs_offsetof(ucg_builtin_config_t, bcopy_to_zcopy_opt), UCS_CONFIG_TYPE_UINT},

    // max_short_max threshold change from 256 to 200 to avoid hang problem within rc_x device.
    /* max_am_inline size may be different(dc is 2046 or 186) on mlx dc&rc devices when ppn > 32,
       this may result in erroneous result or hang problem because of mixture use of am_short_one
       and am_short_max between sender and receiver. */
    {"SHORT_MAX_TX_SIZE", "176", "Largest send operation to use short messages",
     ucs_offsetof(ucg_builtin_config_t, short_max_tx), UCS_CONFIG_TYPE_MEMUNITS},

    {"BCOPY_MAX_TX_SIZE", "32768", "Largest send operation to use buffer copy",
     ucs_offsetof(ucg_builtin_config_t, bcopy_max_tx), UCS_CONFIG_TYPE_MEMUNITS},

    {"LARGE_DATATYPE_THRESHOLD", "32", "Large datatype threshold",
     ucs_offsetof(ucg_builtin_config_t, large_datatype_threshold), UCS_CONFIG_TYPE_UINT},

    {"PLANNER_PRIORITY", "1", "The priority of planner",
     ucs_offsetof(ucg_builtin_config_t, priority), UCS_CONFIG_TYPE_UINT},

    {NULL}
};

struct ucg_builtin_algorithm ucg_builtin_algo_config = {
    .bmtree       = 1,
    .kmtree       = 0,
    .kmtree_intra = 0,
    .recursive    = 1,
    .bruck        = 1,
    .topo         = 0,
    .topo_level   = UCG_GROUP_HIERARCHY_LEVEL_NODE,
    .ring         = 0,
    .pipeline     = 0,
    .feature_flag = UCG_ALGORITHM_SUPPORT_COMMON_FEATURE,
};

ucs_mpool_ops_t ucg_builtin_plan_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = ucs_empty_function,
    .obj_cleanup   = ucs_empty_function
};

/*
 *
 */
static void ucg_builtin_free(void **p)
{
    if (*p != NULL) {
        ucs_free(*p);
        *p = NULL;
    }
}

static enum ucg_builtin_plan_topology_type ucg_builtin_choose_type(enum ucg_collective_modifiers flags)
{
    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE) {
        return UCG_PLAN_TREE_FANOUT;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION) {
        return UCG_PLAN_TREE_FANIN;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE) {
        if (ucg_builtin_algo_config.recursive) {
            return UCG_PLAN_RECURSIVE;
        } else if (ucg_builtin_algo_config.ring) {
            return UCG_PLAN_RING;
        } else {
            return UCG_PLAN_TREE_FANIN_FANOUT;
        }
    }

    if (flags & ucg_predefined_modifiers[UCG_PRIMITIVE_ALLTOALL]) {
        return UCG_PLAN_BRUCK;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_ALLGATHER) {
        if (ucg_builtin_algo_config.bruck) {
            return UCG_PLAN_BRUCK;
        } else {
            return UCG_PLAN_RECURSIVE;
        }
    }

    return UCG_PLAN_TREE_FANIN_FANOUT;
}

static void ucg_builtin_adapt_plan_config(ucg_builtin_config_t *config)
{
    config->cache_size = CACHE_SIZE;
    config->pipelining = 0;
    config->recursive.factor = RECURSIVE_FACTOR;

    /* K-nomial tree algorithm require all K vaule is bigger than 1 */
    if (config->bmtree.degree_inter_fanout <= 1 || config->bmtree.degree_inter_fanin <= 1 ||
        config->bmtree.degree_intra_fanout <= 1 || config->bmtree.degree_intra_fanin <= 1) {
        ucs_warn("K-nomial tree algorithm require all K vaule is bigger than one, switch to default parameter sets");
        config->bmtree.degree_inter_fanout = DEFAULT_INTER_KVALUE;
        config->bmtree.degree_inter_fanin  = DEFAULT_INTER_KVALUE;
        config->bmtree.degree_intra_fanout = DEFAULT_INTRA_KVALUE;
        config->bmtree.degree_intra_fanin  = DEFAULT_INTRA_KVALUE;
    }

    ucs_info("bcast %u allreduce %u barrier %u "
             "inter_fanout %u inter_fanin %u intra_fanout %u intra_fanin %u",
             (unsigned)config->bcast_algorithm, (unsigned)config->allreduce_algorithm,
             (unsigned)config->barrier_algorithm, config->bmtree.degree_inter_fanout, config->bmtree.degree_inter_fanin,
             config->bmtree.degree_intra_fanout, config->bmtree.degree_intra_fanin);

    return;
}

static ucs_status_t ucg_builtin_create_planner_ctx(ucg_builtin_ctx_t *bctx,
                                                   ucg_group_id_t group_id,
                                                   ucg_builtin_planner_ctx_t **planner_ctx_p)
{
    ucg_builtin_planner_ctx_t *planner_ctx = ucs_calloc(1, sizeof(ucg_builtin_planner_ctx_t), "planner ctx");
    if (planner_ctx == NULL) {
        return UCS_ERR_NO_MEMORY;
    }

    planner_ctx->group_id        = group_id;
    planner_ctx->context         = bctx;
    planner_ctx->config          = &bctx->config;
    planner_ctx->am_id           = bctx->am_id;
    ucs_list_head_init(&planner_ctx->send_head);
    ucs_list_head_init(&planner_ctx->plan_head);

    int i;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; ++i) {
        ucg_builtin_comp_slot_t *slot = planner_ctx->slots + i;
        ucs_list_head_init(&slot->msg_head);
        slot->mp = NULL;
        slot->cb = NULL;
    }
    ucg_builtin_adapt_plan_config(planner_ctx->config);
    ucs_ptr_array_set(&bctx->group_by_id, planner_ctx->group_id, planner_ctx);

    *planner_ctx_p = planner_ctx;
    return UCS_OK;
}

static ucs_status_t ucg_builtin_passive_create(ucg_builtin_ctx_t *bctx,
                                               ucg_group_id_t group_id,
                                               ucg_builtin_planner_ctx_t **planner_ctx_p)
{
    return ucg_builtin_create_planner_ctx(bctx, group_id, planner_ctx_p);
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_builtin_am_handler,
                 (arg, data, length, am_flags),
                 void *arg, void *data, size_t length, unsigned am_flags)
{
    ucs_status_t status;
    ucg_builtin_header_t* header  = data;
    ucg_builtin_comp_slot_t *slot = NULL;
    ucg_group_id_t group_id       = header->group_id;
    ucg_builtin_ctx_t *ctx        = (ucg_builtin_ctx_t*)arg;
    ucs_assert(length >= sizeof(header));
    ucg_builtin_planner_ctx_t *planner_ctx;
    if (ucs_unlikely(!ucs_ptr_array_lookup(&ctx->group_by_id, group_id, planner_ctx))) {
        ucs_debug("passive create, group_id %u", group_id);
        status = ucg_builtin_passive_create(ctx, group_id, &planner_ctx);
        ucs_assert_always(status == UCS_OK);
    }
    slot = planner_ctx->slots + header->coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS;
    /* Consume the message if it fits the current collective and step index */
    if (ucs_likely(slot->cb && (header->local_id == slot->local_id))) {
        /* Make sure the packet indeed belongs to the collective currently on */
        ucs_debug("ucg_builtin_am_handler CB: coll_id %u step_idx %u cb %p pending %u",
                  header->coll_id, header->step_idx, slot->cb, slot->req.pending);

        if ((slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) &&
            (slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND)) {
            /* Zcopy recv before sending finished, store msg */
            if (slot->req.pending > slot->req.step->fragments_recv) {
                if (++slot->req.step->zcopy.num_store > slot->req.step->fragments_recv) {
                    /* recv msg from step - step index = step now index + 256, store msg without count */
                    slot->req.step->zcopy.num_store--;
                }
                goto am_handler_store;
            }
            if (slot->req.step->zcopy.num_store > 0) {
                slot->req.step->zcopy.num_store = 0;
                (void) ucg_builtin_msg_process(slot, &slot->req);
            }
        }

        if ((slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND) &&
            slot->req.recv_comp) {
            goto am_handler_store;
        }

        if (slot->req.step->phase->is_swap) {
            ucg_builtin_swap_net_recv(data + sizeof(ucg_builtin_header_t),
                                      length - sizeof(ucg_builtin_header_t),
                                      header->remote_offset, &slot->req);
        }

        /* The packet arrived "on time" - process it */
        UCS_PROFILE_CODE("ucg_builtin_am_handler_cb") {
            (void) slot->cb(&slot->req, header->remote_offset,
                            data + sizeof(ucg_builtin_header_t),
                            length - sizeof(ucg_builtin_header_t));
        }
        return UCS_OK;
    }

    /* Store the message - use RX_headroom for @ref ucg_builtin_comp_desc_t */
    ucg_builtin_comp_desc_t* desc = NULL;
am_handler_store:
    if (am_flags & UCT_CB_PARAM_FLAG_DESC) {
        desc = (ucg_builtin_comp_desc_t*)((char*)data -
                offsetof(ucg_builtin_comp_desc_t, header));
        desc->release_desc = uct_iface_release_desc;
        status = UCS_INPROGRESS;
    } else {
        /* Cannot use existing descriptor - must allocate my own... */
        if (ucs_likely(slot->mp != NULL)) {
            desc = (ucg_builtin_comp_desc_t*)ucs_mpool_get_inline(slot->mp);
        } else {
            /* Passively created planner_ctx's slot->mp is NULL.
             * This doesn't happen verf often, there's no need to use a memory pool.*/
            uint32_t malloc_size = ucs_offsetof(ucg_builtin_comp_desc_t, header) + length;
            desc = (ucg_builtin_comp_desc_t*)ucs_calloc(1, malloc_size, "ucg desc");
        }
        /* Failure return may result in hang up */
        ucs_assert_always(desc != NULL);
        memcpy(&desc->header, data, length);
        desc->release_desc = (slot->mp != NULL) ? ucs_mpool_put_inline : free;
        status = UCS_OK;
    }

    ucs_debug("ucg_builtin_am_handler STORE: group_id %u coll_id %u(%u) step_idx %u(%u)",
              header->group_id, header->coll_id, slot->coll_id, header->step_idx, slot->step_idx);

    desc->super.flags = am_flags;
    desc->super.length = length - sizeof(ucg_builtin_header_t);
    ucs_list_add_tail(&slot->msg_head, &desc->super.tag_list[0]);
    return status;
}

void ucg_builtin_msg_dump(void *arg, uct_am_trace_type_t type,
                          uint8_t id, const void *data, size_t length,
                          char *buffer, size_t max)
{
    const ucg_builtin_header_t *header = (const ucg_builtin_header_t*)data;
    snprintf(buffer, max, "COLLECTIVE [coll_id %u step_idx %u offset %lu length %lu]",
             (unsigned)header->coll_id, (unsigned)header->step_idx,
             (uint64_t)header->remote_offset, length - sizeof(*header));
}



static void ucg_builtin_clean_phases(ucg_builtin_plan_t *plan)
{
    int i;
    for (i = 0; i < plan->phs_cnt; i++) {
        ucg_builtin_free((void **)&plan->phss[i].recv_cache_buffer);
        ucg_builtin_free((void **)&plan->phss[i].ucp_eps);
    }

#if ENABLE_DEBUG_DATA
    ucg_builtin_free((void **)&plan->phss[0].indexes);
#endif
}

static void ucg_builtin_release_comp_desc(ucg_builtin_comp_desc_t *desc)
{
    if (desc->super.flags == UCT_CB_PARAM_FLAG_DESC) {
        uct_iface_release_desc(desc);
    } else {
        ucs_mpool_put_inline(desc);
    }
}

void ucg_builtin_plan_decision_in_unsupport_allreduce_case_check_msg_size(const size_t msg_size)
{
    if (msg_size < UCG_GROUP_MED_MSG_SIZE) {
        /* Node-aware Recursive */
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE,
                                          &ucg_builtin_algo_config);
    } else {
        /* Ring */
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RING, &ucg_builtin_algo_config);
    }
}

void ucg_builtin_plan_decision_in_unsupport_allreduce_case(const size_t msg_size,
                                                           const ucg_group_params_t *group_params,
                                                           const enum ucg_collective_modifiers modifiers,
                                                           const ucg_collective_params_t *coll_params)
{
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        if (coll_params->send.op_ext && !ucg_builtin_op_is_commute_cb(coll_params->send.op_ext)) {
            /* Ring */
            ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RING, &ucg_builtin_algo_config);
            ucs_debug("non-commutative operation, select Ring.");
        } else {
            ucg_builtin_plan_decision_in_unsupport_allreduce_case_check_msg_size(msg_size);
        }
    }
}

void ucg_builtin_plan_decision_in_unsupport_bcast_case(const size_t msg_size,
                                                       const ucg_group_params_t *group_params,
                                                       const enum ucg_collective_modifiers modifiers,
                                                       const ucg_collective_params_t *coll_params)
{
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BCAST]) {
        /* Node-aware Binomial tree (DEFAULT) */
        ucg_builtin_bcast_algo_switch(UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE, &ucg_builtin_algo_config);
    }
}

void ucg_builtin_plan_decision_in_unsupport_barrier_case(const size_t msg_size,
                                                         const ucg_group_params_t *group_params,
                                                         const enum ucg_collective_modifiers modifiers,
                                                         const ucg_collective_params_t *coll_params)
{
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        /* Node-aware Recursive (DEFAULT) */
        ucg_builtin_barrier_algo_switch(UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE, &ucg_builtin_algo_config);
    }
}

/* change algorithm in unsupport case */
void ucg_builtin_plan_decision_in_unsupport_case(const size_t msg_size,
                                                 const ucg_group_params_t *group_params,
                                                 const enum ucg_collective_modifiers modifiers,
                                                 const ucg_collective_params_t *coll_params)
{
    /* choose algorithm due to message size */
    ucg_builtin_plan_decision_in_unsupport_allreduce_case(msg_size, group_params, modifiers, coll_params);
    ucg_builtin_plan_decision_in_unsupport_bcast_case(msg_size, group_params, modifiers, coll_params);
    ucg_builtin_plan_decision_in_unsupport_barrier_case(msg_size, group_params, modifiers, coll_params);
}

void ucg_builtin_plan_decision_in_noncommutative_largedata_case_recusive(const size_t msg_size, enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision)
{
    /* Recusive */
    if (allreduce_algo_decision != NULL) {
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_RECURSIVE;
    }
    ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_builtin_algo_config);
    ucs_debug("non-commutative operation, select recurisive");
}

void ucg_builtin_plan_decision_in_noncommutative_largedata_case_ring(const size_t msg_size, enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision)
{
    /* Ring */
    if (allreduce_algo_decision != NULL) {
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_RING;
    }
    ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RING, &ucg_builtin_algo_config);
    ucs_debug("non-commutative operation, select Ring.");
}

void ucg_builtin_plan_decision_in_noncommutative_largedata_case(const size_t msg_size, enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision)
{
    if (msg_size < UCG_GROUP_MED_MSG_SIZE) {
        ucg_builtin_plan_decision_in_noncommutative_largedata_case_recusive(msg_size, allreduce_algo_decision);
    } else {
        ucg_builtin_plan_decision_in_noncommutative_largedata_case_ring(msg_size, allreduce_algo_decision);
    }
}

void ucg_builtin_plan_decision_in_noncommutative_many_counts_case()
{
    ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_builtin_algo_config);
    ucs_debug("non-commutative operation with more than one send count, select recurisive");
}

void ucg_builtin_allreduce_decision_fixed(const size_t msg_size,
                                          const ucg_group_params_t *group_params,
                                          const ucg_collective_params_t *coll_params,
                                          const unsigned large_datatype_threshold,
                                          const int is_unbalanced_ppn,
                                          enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision)
{
    unsigned is_large_datatype = (coll_params->send.dt_len > large_datatype_threshold);
    unsigned is_non_commutative = (coll_params->send.op_ext && !ucg_builtin_op_is_commute_cb(coll_params->send.op_ext));
    if (is_large_datatype || is_non_commutative) {
        ucg_builtin_plan_decision_in_noncommutative_largedata_case(msg_size, allreduce_algo_decision);
    } else if (msg_size >= UCG_GROUP_MED_MSG_SIZE) {
        /* Ring */
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_RING;
        ucg_builtin_allreduce_algo_switch(*allreduce_algo_decision, &ucg_builtin_algo_config);
    } else if (is_unbalanced_ppn) {
        /* Node-aware Recursive */
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE;
        ucg_builtin_allreduce_algo_switch(*allreduce_algo_decision, &ucg_builtin_algo_config);
    } else {
        /* Node-aware Kinomial tree (DEFAULT) */
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE;
        ucg_builtin_allreduce_algo_switch(*allreduce_algo_decision, &ucg_builtin_algo_config);
    }
}

void plan_decision_fixed(const size_t msg_size,
                         const ucg_group_params_t *group_params,
                         const enum ucg_collective_modifiers modifiers,
                         const ucg_collective_params_t *coll_params,
                         const unsigned large_datatype_threshold,
                         const int is_unbalanced_ppn,
                         enum ucg_builtin_bcast_algorithm *bcast_algo_decision,
                         enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision,
                         enum ucg_builtin_barrier_algorithm *barrier_algo_decision)
{
    *bcast_algo_decision = UCG_ALGORITHM_BCAST_AUTO_DECISION;
    *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION;
    *barrier_algo_decision = UCG_ALGORITHM_BARRIER_AUTO_DECISION;
    /* choose algorithm due to message size */
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        ucg_builtin_allreduce_decision_fixed(msg_size, group_params, coll_params, large_datatype_threshold,
                                             is_unbalanced_ppn, allreduce_algo_decision);
    }
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BCAST]) {
        /* Node-aware Binomial tree (DEFAULT) */
        *bcast_algo_decision = UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE;
        ucg_builtin_bcast_algo_switch(*bcast_algo_decision, &ucg_builtin_algo_config);
    }
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        /* Node-aware Recursive (DEFAULT) */
        if (is_unbalanced_ppn) {
            /* Node-aware Recursive */
            *barrier_algo_decision = UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE;
            ucg_builtin_barrier_algo_switch(*barrier_algo_decision, &ucg_builtin_algo_config);
        } else {
            /* Node-aware Kinomial tree (DEFAULT) */
            *barrier_algo_decision = UCG_ALGORITHM_BARRIER_NODE_AWARE_KMTREE;
            ucg_builtin_barrier_algo_switch(*barrier_algo_decision, &ucg_builtin_algo_config);
        }
    }
}

void ucg_builtin_fillin_algo(struct ucg_builtin_algorithm *algo,
                             unsigned bmtree,
                             unsigned kmtree,
                             unsigned kmtree_intra,
                             unsigned recursive,
                             unsigned topo,
                             unsigned ring)
{
    algo->bmtree = bmtree;
    algo->kmtree = kmtree;
    algo->kmtree_intra = kmtree_intra;
    algo->recursive = recursive;
    algo->topo = topo;
    algo->ring = ring;
}

static void ucg_builtin_init_algo(struct ucg_builtin_algorithm *algo)
{
    ucg_builtin_fillin_algo(algo, 1, 0, 0, 1, 0, 0);
    algo->bruck        = 1,
    algo->topo_level   = UCG_GROUP_HIERARCHY_LEVEL_NODE,
    algo->pipeline     = 0;
    algo->feature_flag = UCG_ALGORITHM_SUPPORT_COMMON_FEATURE;
}

void ucg_builtin_bcast_algo_switch(const enum ucg_builtin_bcast_algorithm bcast_algo_decision,
                                   struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
    algo->bruck = 1;
    switch (bcast_algo_decision) {
        case UCG_ALGORITHM_BCAST_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK;
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK;
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 0, 0, 1, 0);
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0);
            break;
        default:
            ucg_builtin_bcast_algo_switch(UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE, algo);
            break;
    }
}

void ucg_builtin_barrier_algo_switch(const enum ucg_builtin_barrier_algorithm barrier_algo_decision,
                                     struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->bruck = 1;
    switch (barrier_algo_decision) {
        case UCG_ALGORITHM_BARRIER_RECURSIVE:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 1, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        default:
            ucg_builtin_barrier_algo_switch(UCG_ALGORITHM_BARRIER_NODE_AWARE_KMTREE, algo);
            break;
    }
}

void ucg_builtin_allreduce_algo_switch(const enum ucg_builtin_allreduce_algorithm allreduce_algo_decision,
                                       struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->bruck = 1;
    switch (allreduce_algo_decision) {
        case UCG_ALGORITHM_ALLREDUCE_RECURSIVE:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 1, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK |
                                  UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS |
                                  UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_ALLREDUCE_RING:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 0, 1);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN |
                                  UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK |
                                  UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS |
                                  UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        default:
            ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE, algo);
            break;
    }
}

void ucg_builtin_check_algorithm_param_size(ucg_builtin_config_t *config)
{
    if (((int)config->allreduce_algorithm >= UCG_ALGORITHM_ALLREDUCE_LAST) || ((int)config->allreduce_algorithm < UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION)) {
        ucs_info("Param UCX_BUILTIN_ALLREDUCE_ALGORITHM=%d is invalid parameter, switch to default algorithm.", (int)config->allreduce_algorithm);
    }
    if (((int)config->bcast_algorithm >= UCG_ALGORITHM_BCAST_LAST) || ((int)config->bcast_algorithm < UCG_ALGORITHM_BCAST_AUTO_DECISION)) {
        ucs_info("Param UCX_BUILTIN_BCAST_ALGORITHM=%d is invalid parameter, switch to default algorithm.", (int)config->bcast_algorithm);
    }
    if (((int)config->barrier_algorithm >= UCG_ALGORITHM_BARRIER_LAST) || ((int)config->barrier_algorithm < UCG_ALGORITHM_BARRIER_AUTO_DECISION)) {
        ucs_info("Param UCX_BUILTIN_BARRIER_ALGORITHM=%d is invalid parameter, switch to default algorithm.", (int)config->barrier_algorithm);
    }
}

void ucg_builtin_check_algorithm_param_type(ucg_builtin_config_t *config)
{
    if ((config->allreduce_algorithm - (int)config->allreduce_algorithm) != 0) {
        ucs_info("Param UCX_BUILTIN_ALLREDUCE_ALGORITHM=%lf is not unsigned integer, switch to unsigned integer '%d'.", config->allreduce_algorithm, (int)config->allreduce_algorithm);
    }
    if ((config->bcast_algorithm - (int)config->bcast_algorithm) != 0) {
        ucs_info("Param UCX_BUILTIN_BCAST_ALGORITHM=%lf is not unsigned integer, switch to unsigned integer '%d'.", config->bcast_algorithm, (int)config->bcast_algorithm);
    }
    if ((config->barrier_algorithm - (int)config->barrier_algorithm) != 0) {
        ucs_info("Param UCX_BUILTIN_BARRIER_ALGORITHM=%lf is not unsigned integer, switch to unsigned integer '%d'.", config->barrier_algorithm, (int)config->barrier_algorithm);
    }
}

enum choose_ops_mask ucg_builtin_plan_choose_ops(ucg_builtin_config_t *config,
                                                 enum ucg_collective_modifiers ops_type_choose)
{
    ucg_builtin_check_algorithm_param_type(config);
    ucg_builtin_check_algorithm_param_size(config);

    enum ucg_builtin_bcast_algorithm bcast_algo_decision =
        (enum ucg_builtin_bcast_algorithm)config->bcast_algorithm;
    enum ucg_builtin_allreduce_algorithm allreduce_algo_decision =
        (enum ucg_builtin_allreduce_algorithm)config->allreduce_algorithm;
    enum ucg_builtin_barrier_algorithm barrier_algo_decision =
        (enum ucg_builtin_barrier_algorithm)config->barrier_algorithm;
    enum choose_ops_mask result = OPS_AUTO_DECISION;

    if (!(bcast_algo_decision | allreduce_algo_decision | barrier_algo_decision)) {
        return OPS_AUTO_DECISION;
    }

    if (ops_type_choose == ucg_predefined_modifiers[UCG_PRIMITIVE_BCAST]) {
        if (bcast_algo_decision >= UCG_ALGORITHM_BCAST_LAST || bcast_algo_decision <= UCG_ALGORITHM_BCAST_AUTO_DECISION) {
            return OPS_AUTO_DECISION;
        }
        result = OPS_BCAST;
    }

    if (ops_type_choose == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        if (allreduce_algo_decision >= UCG_ALGORITHM_ALLREDUCE_LAST || allreduce_algo_decision <= UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION) {
            return OPS_AUTO_DECISION;
        }
        result = OPS_ALLREDUCE;
    }

    if (ops_type_choose == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        if (barrier_algo_decision >= UCG_ALGORITHM_BARRIER_LAST || barrier_algo_decision <= UCG_ALGORITHM_BARRIER_AUTO_DECISION) {
            return OPS_AUTO_DECISION;
        }
        result = OPS_BARRIER;
    }

    return result;
}

ucs_status_t ucg_builtin_check_continuous_number(const ucg_group_params_t *group_params,
                                                 enum ucg_group_member_distance domain_distance,
                                                 unsigned *discont_flag)
{
    if (domain_distance == UCG_GROUP_MEMBER_DISTANCE_SOCKET) {
        *discont_flag = !group_params->topo_args.rank_continuous_in_sock;
    } else {
        *discont_flag = !group_params->topo_args.rank_continuous_in_node;
    }

    return UCS_OK;
}

void choose_distance_from_topo_aware_level(enum ucg_group_member_distance *domain_distance)
{
    switch (ucg_builtin_algo_config.topo_level) {
        case UCG_GROUP_HIERARCHY_LEVEL_NODE:
            *domain_distance = UCG_GROUP_MEMBER_DISTANCE_HOST;
            break;
        case UCG_GROUP_HIERARCHY_LEVEL_SOCKET:
            *domain_distance = UCG_GROUP_MEMBER_DISTANCE_SOCKET;
            break;
        case UCG_GROUP_HIERARCHY_LEVEL_L3CACHE:
            *domain_distance = UCG_GROUP_MEMBER_DISTANCE_L3CACHE;
            break;
        default:
            break;
    }
}

void ucg_builtin_non_commutative_operation(const ucg_group_params_t *group_params, const ucg_collective_params_t *coll_params, struct ucg_builtin_algorithm *algo, const size_t msg_size)
{
    if (coll_params->send.op_ext && !ucg_builtin_op_is_commute_cb(coll_params->send.op_ext) &&
        !(algo->feature_flag & UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS)) {
        if (coll_params->send.count > 1) {
            ucg_builtin_plan_decision_in_noncommutative_many_counts_case();
            ucs_info("Current algorithm does not support many counts non-commutative operation, and switch to Recursive doubling which may have unexpected performance");
        } else {
            ucg_builtin_plan_decision_in_noncommutative_largedata_case(msg_size, NULL);
            ucs_info("Current algorithm does not support non commutative operation, and switch to Recursive doubling or Ring Algorithm which may have unexpected performance");
        }
    }
}

int ucg_is_noncontig_allreduce(const ucg_params_t *ucg_params,
                               const ucg_collective_params_t *coll_params)
{
    ucp_datatype_t ucp_datatype;

    if (coll_params->type.modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE] &&
        coll_params->send.count > 0 && coll_params->send.dt_len > 0) {
        ucg_params->mpi_dt_convert(coll_params->send.dt_ext, &ucp_datatype);
        if (!UCP_DT_IS_CONTIG(ucp_datatype)) {
            ucs_debug("allreduce non-contiguous datatype");
            return 1;
        }
    }

    return 0;
}

static inline int ucg_is_noncommutative_allreduce(const ucg_params_t *group_params,
                                                  const ucg_collective_params_t *coll_params)
{
    return coll_params->type.modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE] &&
           coll_params->send.op_ext && !group_params->op_is_commute_f(coll_params->send.op_ext);
}

#define UCT_MIN_SHORT_ONE_LEN 80
#define UCT_MIN_BCOPY_ONE_LEN 1000
int ucg_is_segmented_allreduce(const ucg_collective_params_t *coll_params)
{
    int count = coll_params->send.count;
    size_t dt_len = coll_params->send.dt_len;

    if (coll_params->type.modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        if (dt_len > UCT_MIN_BCOPY_ONE_LEN) {
            return 1;
        }

        if (dt_len > UCT_MIN_SHORT_ONE_LEN && (dt_len * count) < UCG_GROUP_MED_MSG_SIZE) {
            return 1;
        }
    }

    return 0;
}

/*
   Deal with all unsupport special case.
*/
static ucs_status_t ucg_builtin_change_unsupport_algo(struct ucg_builtin_algorithm *algo,
                                               const ucg_group_params_t *group_params,
                                               const ucg_params_t *ucg_params,
                                               const size_t msg_size,
                                               const ucg_collective_params_t *coll_params,
                                               const enum ucg_collective_modifiers ops_type_choose,
                                               enum choose_ops_mask ops_choose,
                                               ucg_builtin_config_t *config)
{
    ucs_status_t status;

    /* Currently, only algorithm 1 supports non-contiguous datatype for allreduce */
    if (ucg_is_noncontig_allreduce(ucg_params, coll_params)) {
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_builtin_algo_config);
        ucs_info("allreduce non-contiguous datatype, select algo%d:recursive", UCG_ALGORITHM_ALLREDUCE_RECURSIVE);
        return UCS_OK;
    }

    /* Currently, only algorithm 1 supports non-commutative op for allreduce */
    if (ucg_is_noncommutative_allreduce(ucg_params, coll_params)) {
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_builtin_algo_config);
        ucs_info("non-commutative allreduce, select algo%d:recursive", UCG_ALGORITHM_ALLREDUCE_RECURSIVE);
        return UCS_OK;
    }

    /* Special Case 1 : bind-to none */
    if (!(algo->feature_flag & UCG_ALGORITHM_SUPPORT_BIND_TO_NONE) && (group_params->is_bind_to_none)) {
        ucg_builtin_plan_decision_in_unsupport_case(msg_size, group_params, ops_type_choose, coll_params);
        ucs_info("Current algorithm don't support bind-to none case, switch to default algorithm");
    }

    /* Special Case 2 : unbalance ppn */
    unsigned is_ppn_unbalance = 0;
    status = ucg_builtin_check_ppn(group_params, &is_ppn_unbalance);
    if (status != UCS_OK) {
        return status;
    }

    if (is_ppn_unbalance && (!(algo->feature_flag & UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN))) {
        ucg_builtin_plan_decision_in_unsupport_case(msg_size, group_params, ops_type_choose, coll_params);
        ucs_info("Current algorithm don't support ppn unbalance case, switch to default algorithm");
    }

    /* Special Case 3 : discontinuous rank */
    unsigned is_discontinuous_rank = 0;
    enum ucg_group_member_distance domain_distance = UCG_GROUP_MEMBER_DISTANCE_HOST;
    choose_distance_from_topo_aware_level(&domain_distance);
    if (status != UCS_OK) {
        return status;
    }
    status = ucg_builtin_check_continuous_number(group_params, domain_distance, &is_discontinuous_rank);
    if (status != UCS_OK) {
        return status;
    }

    if (is_discontinuous_rank && (!(algo->feature_flag & UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK))) {
        ucg_builtin_plan_decision_in_unsupport_case(msg_size, group_params, ops_type_choose, coll_params);
        ucs_info("Current algorithm demand rank number is continous. Switch default algorithm whose performance may be not the best");
    }

    if (ops_choose == OPS_ALLREDUCE) {
        /* Special Case 4 : non-commutative operation */
        ucg_builtin_non_commutative_operation(group_params, coll_params, algo, msg_size);

        /* Special Case 5 : large datatype */
        if (coll_params->send.dt_len > config->large_datatype_threshold &&
            !(algo->feature_flag & UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE)) {
                ucg_builtin_plan_decision_in_noncommutative_largedata_case(msg_size, NULL);
                ucs_info("Current algorithm does not support large datatype, and switch to Recursive doubling or Ring Algorithm which may have unexpected performance");
        }
    }

    /* The allreduce result is wrong when phase->segmented=1 and using ring algorithm, must avoid it */
    if (ucg_builtin_algo_config.ring && ucg_is_segmented_allreduce(coll_params)) {
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_builtin_algo_config);
        ucs_info("ring algorithm does not support segmented phase, select recursive algorithm");
        return UCS_OK;
    }

    return status;
}

void ucg_builtin_log_algo()
{
    ucs_info("bmtree %u kmtree %u kmtree_intra %u recur %u bruck %u topo %u level %u ring %u pipe %u",
             ucg_builtin_algo_config.bmtree, ucg_builtin_algo_config.kmtree, ucg_builtin_algo_config.kmtree_intra, ucg_builtin_algo_config.recursive, ucg_builtin_algo_config.bruck,
             ucg_builtin_algo_config.topo, (unsigned)ucg_builtin_algo_config.topo_level, ucg_builtin_algo_config.ring, ucg_builtin_algo_config.pipeline);
}

ucs_status_t ucg_builtin_algorithm_decision(const ucg_builtin_planner_ctx_t *ctx,
                                            const ucg_collective_params_t *coll_params)
{
    size_t msg_size = coll_params->send.count * coll_params->send.dt_len;
    const ucg_group_params_t *group_params = ctx->group_params;
    const ucg_collective_type_t *coll = &coll_params->type;
    enum ucg_collective_modifiers ops_type_choose = coll->modifiers;
    ucg_builtin_config_t *config = ctx->config;

    enum ucg_builtin_bcast_algorithm bcast_algo_decision =
        (enum ucg_builtin_bcast_algorithm)config->bcast_algorithm;
    enum ucg_builtin_allreduce_algorithm allreduce_algo_decision =
        (enum ucg_builtin_allreduce_algorithm)config->allreduce_algorithm;
    enum ucg_builtin_barrier_algorithm barrier_algo_decision =
        (enum ucg_builtin_barrier_algorithm)config->barrier_algorithm;

    ucs_status_t status;

    /* default algorithm choosen:
       Bcast :     3
       Allreduce : small message : 2
                   big   message : 4
       Barrier   : 2
    */
    enum choose_ops_mask ops_choose = ucg_builtin_plan_choose_ops(config, ops_type_choose);
    ucs_info("choose ops: %d, bcast mode: %u, allreduce mode: %u, barrier mode: %u",
             ops_choose, bcast_algo_decision, allreduce_algo_decision, barrier_algo_decision);

    /* unblanced ppn or not */
    unsigned is_ppn_unbalance = 0;
    status = ucg_builtin_check_ppn(group_params, &is_ppn_unbalance);
    if (status != UCS_OK) {
        ucs_error("Error in check ppn");
        return status;
    }
    ucs_info("ppn unbalance: %u", is_ppn_unbalance);

    switch (ops_choose) {
        case OPS_AUTO_DECISION:
            /* Auto algorithm decision: according to is_ppn_unbalance/data/msg_size etc */
            plan_decision_fixed(msg_size, group_params, ops_type_choose, coll_params, config->large_datatype_threshold, is_ppn_unbalance,
                                &bcast_algo_decision, &allreduce_algo_decision, &barrier_algo_decision);
            break;

        case OPS_BCAST:
            ucg_builtin_bcast_algo_switch(bcast_algo_decision, &ucg_builtin_algo_config);
            allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION;
            barrier_algo_decision = UCG_ALGORITHM_BARRIER_AUTO_DECISION;
            break;

        case OPS_ALLREDUCE:
            ucg_builtin_allreduce_algo_switch(allreduce_algo_decision, &ucg_builtin_algo_config);
            bcast_algo_decision = UCG_ALGORITHM_BCAST_AUTO_DECISION;
            barrier_algo_decision = UCG_ALGORITHM_BARRIER_AUTO_DECISION;
            break;

        case OPS_BARRIER:
            ucg_builtin_barrier_algo_switch(barrier_algo_decision, &ucg_builtin_algo_config);
            bcast_algo_decision = UCG_ALGORITHM_BCAST_AUTO_DECISION;
            allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION;
            break;

        default:
            break;
    }

    /* One API to deal with all special case */
    status = ucg_builtin_change_unsupport_algo(&ucg_builtin_algo_config,
                                               group_params,
                                               &ctx->context->context->params,
                                               msg_size,
                                               coll_params,
                                               ops_type_choose,
                                               ops_choose,
                                               config);
    ucg_builtin_log_algo();

    return status;
}

static void ucg_builtin_set_phase_thresh_max_short(ucg_builtin_planner_ctx_t *ctx,
                                                   ucg_builtin_plan_phase_t *phase)
{
    if (phase->ep_attr->cap.am.max_short < sizeof(ucg_builtin_header_t)) {
        phase->send_thresh.max_short_one = 0;
    } else {
        phase->send_thresh.max_short_one = phase->ep_attr->cap.am.max_short - sizeof(ucg_builtin_header_t);
    }

    if (phase->send_thresh.max_short_one == 0) {
        phase->send_thresh.max_short_max = 0;
    } else {
        phase->send_thresh.max_short_max = ctx->config->short_max_tx;
    }

    if (phase->send_thresh.max_short_one > phase->send_thresh.max_short_max) {
        phase->send_thresh.max_short_one = phase->send_thresh.max_short_max;
    }

    phase->send_thresh.max_short_one -= phase->send_thresh.max_short_one % DATATYPE_ALIGN;
}

static void ucg_builtin_set_phase_thresh_max_bcopy_zcopy(ucg_builtin_planner_ctx_t *ctx,
                                                         ucg_builtin_plan_phase_t *phase)
{
    phase->send_thresh.max_bcopy_one = phase->ep_attr->cap.am.max_bcopy - sizeof(ucg_builtin_header_t);
    phase->send_thresh.max_bcopy_max = ctx->config->bcopy_max_tx;
    if (phase->md_attr->cap.max_reg && (phase->md_attr->cap.flags & UCT_MD_FLAG_NEED_MEMH)) {
        if (phase->send_thresh.max_bcopy_one > phase->send_thresh.max_bcopy_max) {
            phase->send_thresh.max_bcopy_one = phase->send_thresh.max_bcopy_max;
        }
        phase->send_thresh.max_zcopy_one = phase->ep_attr->cap.am.max_zcopy - sizeof(ucg_builtin_header_t);
    } else {
        phase->send_thresh.max_zcopy_one = phase->send_thresh.max_bcopy_max = SIZE_MAX;
    }

    phase->send_thresh.max_bcopy_one -= phase->send_thresh.max_bcopy_one % DATATYPE_ALIGN;
    phase->send_thresh.max_zcopy_one -= phase->send_thresh.max_zcopy_one % DATATYPE_ALIGN;
}

void ucg_builtin_set_phase_thresholds(ucg_builtin_planner_ctx_t *ctx,
                                      ucg_builtin_plan_phase_t *phase)
{
    ucg_builtin_set_phase_thresh_max_short(ctx, phase);
    ucg_builtin_set_phase_thresh_max_bcopy_zcopy(ctx, phase);

    phase->send_thresh.md_attr_cap_max_reg = (phase->md_attr->cap.flags & UCT_MD_FLAG_NEED_MEMH) ? phase->md_attr->cap.max_reg : 0;
    phase->send_thresh.initialized = 1;

    if (!phase->recv_thresh.initialized) {
        phase->recv_thresh.max_short_one = phase->send_thresh.max_short_one;
        phase->recv_thresh.max_short_max = phase->send_thresh.max_short_max;
        phase->recv_thresh.max_bcopy_one = phase->send_thresh.max_bcopy_one;
        phase->recv_thresh.max_bcopy_max = phase->send_thresh.max_bcopy_max;
        phase->recv_thresh.max_zcopy_one = phase->send_thresh.max_zcopy_one;
        phase->recv_thresh.md_attr_cap_max_reg = phase->send_thresh.md_attr_cap_max_reg;
        phase->recv_thresh.initialized = 1;
    }
}

void ucg_builtin_log_phase_info(ucg_builtin_plan_phase_t *phase, ucg_group_member_index_t idx)
{
    ucs_debug("phase create: %p, dest %" PRIu64 ", short_one %zu, short_max %zu, bcopy_one %zu, bcopy_max %zu, zcopy_one %zu, max_reg %zu",
               phase, idx, phase->send_thresh.max_short_one, phase->send_thresh.max_short_max, phase->send_thresh.max_bcopy_one, phase->send_thresh.max_bcopy_max, phase->send_thresh.max_zcopy_one, phase->md_attr->cap.max_reg);
}

ucs_status_t ucg_builtin_connect(ucg_builtin_planner_ctx_t *ctx,
                                 ucg_group_member_index_t idx,
                                 ucg_builtin_plan_phase_t *phase,
                                 unsigned phase_ep_index)
{
    ucg_group_ep_t ucg_ep = {0};
    ucs_status_t status = ucg_group_connect(ctx->group, idx, &ucg_ep);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    if (ucs_unlikely(ucg_ep.am_ep == NULL)) {
        /* status is UCS_OK, but am_ep is NULL, means it's "debugging" */
        phase->send_thresh.max_short_one = SIZE_MAX;
        return UCS_OK;
    }

    phase->ep_attr = ucg_ep.am_iface_attr;
    phase->md      = ucg_ep.md;
    phase->md_attr = ucg_ep.md_attr;

    if (phase->ucp_eps == NULL) {
        phase->ucp_eps = UCG_ALLOC_CHECK(sizeof(ucp_ep_h) * phase->ep_cnt, "ucp_eps");
    }

    if (phase_ep_index == UCG_BUILTIN_CONNECT_SINGLE_EP) {
        phase->single_ep  = ucg_ep.am_ep;
        phase->ucp_eps[0] = ucg_ep.ucp_ep;
#if ENABLE_DEBUG_DATA
        phase->indexes[0] = idx;
#endif
    } else {
        /*
         * Only avoid for case of Bruck plan because phase->ep_cnt = 1
         * with 2 endpoints(send + recv) actually
         */
        if (phase->method != UCG_PLAN_METHOD_ALLGATHER_BRUCK &&
            phase->method != UCG_PLAN_METHOD_ALLTOALL_BRUCK &&
            phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
            phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
            ucs_assert(phase_ep_index < phase->ep_cnt);
        }
        phase->multi_eps[phase_ep_index] = ucg_ep.am_ep;
        phase->ucp_eps[phase_ep_index]   = ucg_ep.ucp_ep;
#if ENABLE_DEBUG_DATA
        phase->indexes[phase_ep_index] = idx;
#endif
    }

    /* Set the thresholds */
    ucg_builtin_set_phase_thresholds(ctx, phase);
    ucg_builtin_log_phase_info(phase, idx);

    return status;
}

static int ucg_builtin_check_plan(const ucg_plan_t *plan_p,
                                  const ucg_collective_params_t *params)
{
    ucg_builtin_plan_t *plan = (ucg_builtin_plan_t*)plan_p;
    ucg_builtin_planner_ctx_t *planner_ctx = plan->context;
    ucg_context_h ucg_ctx = planner_ctx->group->context;
    if (params->send.op_ext != NULL
        && !ucg_ctx->params.op_is_commute_f(params->send.op_ext)
        && !(plan->feature & UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS)) {
        return 0;
    }

    if (params->send.dt_len > planner_ctx->config->large_datatype_threshold
        && !(plan->feature & UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE)) {
        return 0;
    }

    return 1;
}

static void ucg_builtin_destroy_plan(ucg_plan_t *plan_p)
{
    ucg_builtin_plan_t *plan = (ucg_builtin_plan_t*)plan_p;
    for (unsigned i = 0; i < plan->phs_cnt; i++) {
        if (plan->phss[i].ucp_eps != NULL) {
            for (unsigned j = 0; j < plan->phss[i].ep_cnt; j++) {
                ucp_ep_close_nb(plan->phss[i].ucp_eps[j], UCP_EP_CLOSE_MODE_FORCE);
                plan->phss[i].ucp_eps[j] = NULL;
            }
        }
    }

    ucg_builtin_clean_phases(plan);
    while (!ucs_list_is_empty(&plan->super.op_head)) {
        ucg_op_t *op = ucs_list_extract_head(&plan->super.op_head, ucg_op_t, list);
        ucg_builtin_op_discard(op);
    }

    ucs_list_del(&plan->list);
    ucs_mpool_cleanup(&plan->op_mp, 1);
    ucg_builtin_free((void **)&plan);

    return;
}

static ucs_status_t ucg_builtin_create(ucg_group_h group,
                                       ucg_planc_ctx_h planc_ctx,
                                       ucg_planner_ctx_h *planner_ctx_p)
{
    ucs_status_t status;
    ucg_builtin_ctx_t *bctx = (ucg_builtin_ctx_t*)planc_ctx;
    ucg_builtin_planner_ctx_t *planner_ctx;
    uint32_t group_id = group->params.group_id;
    if (ucs_likely(!ucs_ptr_array_lookup(&bctx->group_by_id,
                                         group_id,
                                         planner_ctx))) {
        status = ucg_builtin_create_planner_ctx(bctx, group_id, &planner_ctx);
        if (status != UCS_OK) {
            return status;
        }
    }

    /* set group information */
    planner_ctx->group           = group;
    planner_ctx->group_params    = &group->params;

    int i;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; ++i) {
        ucg_builtin_comp_slot_t *slot = planner_ctx->slots + i;
        slot->mp = &group->worker->am_mp;
    }

    *planner_ctx_p = planner_ctx;
    return UCS_OK;
}

void ucg_builtin_destroy(ucg_planner_ctx_h planner_ctx_p)
{
    ucg_builtin_planner_ctx_t *ctx = (ucg_builtin_planner_ctx_t*)planner_ctx_p;
    /* Cleanup left-over messages and outstanding operations */
    int i;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        if (ctx->slots[i].cb != NULL) {
            ucs_warn("Collective operation #%u has been left incomplete (Group #%u)",
                     ctx->slots[i].coll_id, ctx->group_id);
        }

        while (!ucs_list_is_empty(&ctx->slots[i].msg_head)) {
            ucg_builtin_comp_desc_t *desc =
                    ucs_list_extract_head(&ctx->slots[i].msg_head,
                                          ucg_builtin_comp_desc_t, super.tag_list[0]);
            ucs_warn("Collective operation #%u has %u bytes left pending for step #%u (Group #%u)",
                     desc->header.coll_id, desc->super.length, desc->header.step_idx, desc->header.group_id);
            ucg_builtin_release_comp_desc(desc);
        }
    }
    /* Cleanup plans */
    while (!ucs_list_is_empty(&ctx->plan_head)) {
        ucg_builtin_plan_t *plan = ucs_list_head(&ctx->plan_head,
                                                 ucg_builtin_plan_t, list);
        ucg_builtin_destroy_plan(&plan->super);
    }
    if (ctx->context) {
        ucs_ptr_array_remove(&ctx->context->group_by_id, ctx->group_id);
    }
    ucs_free(ctx);
    return;
}

unsigned ucg_builtin_progress(ucg_planner_ctx_h planner_ctx_p)
{
    ucg_builtin_planner_ctx_t *ctx = (ucg_builtin_planner_ctx_t*)planner_ctx_p;
    /*
     * Since calling @ref ucg_builtin_step_execute may place the operation in
     * the same list again, the list of pending sends is moved to a temporary
     * head, then drained - each call "resets" the state of that operation.
     */
    unsigned ret = 0;
    UCS_LIST_HEAD(temp_head);
    ucs_list_splice_tail(&temp_head, &ctx->send_head);
    ucs_list_head_init(&ctx->send_head);
    while (!ucs_list_is_empty(&temp_head)) {
        ucg_builtin_request_t *req = ucs_list_extract_head(&temp_head,
                                                           ucg_builtin_request_t, send_list);
        ucs_status_t status = ucg_builtin_step_execute(req, NULL);
        if (status != UCS_INPROGRESS) {
            ret++;
        }
    }
    return ret;
}

ucs_status_t ucg_builtin_plan(const ucg_planner_ctx_h planner_ctx_p,
                              const ucg_collective_params_t *params,
                              ucg_plan_h *plan_p)
{
    ucs_status_t status;
    ucg_builtin_plan_t *plan = NULL;
    ucg_builtin_planner_ctx_t *ctx = (ucg_builtin_planner_ctx_t*)planner_ctx_p;

    ucg_builtin_init_algo(&ucg_builtin_algo_config);
    status = ucg_builtin_algorithm_decision(ctx, params);
    if (status != UCS_OK) {
        return status;
    }

    const ucg_collective_type_t *coll_type = &params->type;
    enum ucg_builtin_plan_topology_type plan_topo_type;
    plan_topo_type = ucg_builtin_choose_type(coll_type->modifiers);
    ucs_debug("plan topo type: %d", plan_topo_type);

    /* Build the topology according to the requested */
    switch (plan_topo_type) {
        case UCG_PLAN_RECURSIVE:
            status = ucg_builtin_recursive_create(ctx, plan_topo_type, ctx->config,
                                                  ctx->group_params, coll_type, &plan);
            break;

        case UCG_PLAN_RING:
            status = ucg_builtin_ring_create(ctx, plan_topo_type, ctx->config,
                                             ctx->group_params, coll_type, &plan);
            break;

        default:
            status = ucg_builtin_binomial_tree_create(ctx, plan_topo_type, ctx->config,
                                                      ctx->group_params, coll_type, &plan);
            break;
    }

    if (status != UCS_OK) {
        ucg_builtin_free((void **)&plan);
        return status;
    }

    /* Create a memory-pool for operations for this plan */
    size_t op_size = sizeof(ucg_builtin_op_t) + plan->phs_cnt * sizeof(ucg_builtin_op_step_t);
    status = ucs_mpool_init(&plan->op_mp, 0, op_size, 0, UCS_SYS_CACHE_LINE_SIZE,
                            1, UINT_MAX, &ucg_builtin_plan_mpool_ops, "ucg_builtin_plan_mp");
    if (status != UCS_OK) {
        ucg_builtin_free((void **)&plan);
        return status;
    }

    ucs_list_head_init(&plan->super.op_head);
    plan->super.is_noncontig_allreduce = (plan_topo_type != UCG_PLAN_RECURSIVE) ? 0 :
                      ucg_is_noncontig_allreduce(&ctx->group->context->params, params);
    plan->super.is_ring_plan_topo_type = (plan_topo_type == UCG_PLAN_RING);
    plan->convert_f = ctx->group->context->params.mpi_dt_convert;
    plan->dtspan_f  = ctx->group->context->params.mpi_datatype_span;

    plan->super.check     = ucg_builtin_check_plan;
    plan->super.destroy   = ucg_builtin_destroy_plan;
    plan->super.prepare   = ucg_builtin_op_create;

    plan->resend    = &ctx->send_head;
    plan->slots     = ctx->slots;
    plan->am_id     = ctx->am_id;
    plan->context   = ctx;
    ucs_list_add_head(&ctx->plan_head, &plan->list);

    *plan_p = (ucg_plan_t*)plan;
    return UCS_OK;
}

static void ucg_builtin_print(const ucg_plan_t *plan, const ucg_collective_params_t *coll_params)
{
    unsigned major_version, minor_version, release_number;
    ucp_get_version(&major_version, &minor_version, &release_number);
    printf("version: %d.%d\n", major_version, minor_version);

    printf("plan name: %s\n", plan->planner->name);
}

static ucs_status_t ucg_builtin_query(ucg_planner_h *planner_p)
{
    ucg_planner_h planner = ucs_malloc(sizeof(ucg_planner_t), "builtin planner");
    if (planner == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    planner->create  = ucg_builtin_create;
    planner->destroy = ucg_builtin_destroy;
    planner->progress = ucg_builtin_progress;
    planner->plan = ucg_builtin_plan;
    planner->print = ucg_builtin_print;
    planner->plan_component = &ucg_builtin_component;
    strncpy(planner->name, "builtin", UCG_PLANNER_NAME_MAX);
    planner->modifiers_supported = UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |
                                   UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST |
                                   UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER |
                                   UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE;
    ucg_builtin_config_t config;
    ucs_status_t status;
    status = ucs_config_parser_fill_opts(&config,
                                         ucg_builtin_config_table,
                                         UCS_DEFAULT_ENV_PREFIX,
                                         UCG_BUILTIN_CONFIG_PREFIX,
                                         0);
    if (status == UCS_OK) {
        planner->priority = config.priority;
        ucs_config_parser_release_opts(&config, ucg_builtin_config_table);
    } else {
        ucs_warn("Failed to read builtin configuration, set priority to 1");
        planner->priority = 1;
    }
    *planner_p = planner;

    return UCS_OK;
}

static ucs_status_t ucg_builtin_init(ucg_context_h ctx, ucg_planc_ctx_h *planc_ctx_p)
{
    ucg_builtin_ctx_t *bctx = ucs_calloc(1, sizeof(ucg_builtin_ctx_t), "builtin ctx");
    if (bctx == NULL) {
        ucs_error("Failed to allocate builtin context");
        return UCS_ERR_NO_MEMORY;
    }
    bctx->context = ctx;

    ucs_status_t status;
    status = ucs_config_parser_fill_opts(&bctx->config,
                                         ucg_builtin_config_table,
                                         UCS_DEFAULT_ENV_PREFIX,
                                         UCG_BUILTIN_CONFIG_PREFIX,
                                         0);
    if (status != UCS_OK) {
        ucs_error("Failed to read builtin configuration");
        goto err_free_ctx;
    }

    ucs_ptr_array_init(&bctx->group_by_id, "builtin_group_table");
    status = ucg_context_set_am_handler(ucg_builtin_am_handler,
                                        bctx,
                                        ucg_builtin_msg_dump,
                                        &bctx->am_id,
                                        &ctx->last_am_id);
    if (status != UCS_OK) {
        ucs_error("Failed to register builtin am handler");
        goto err_release_config;
    }

    /* builtin component will use the following user-defined information. */
    ucg_builtin_mpi_reduce_cb    = ctx->params.mpi_reduce_f;
    ucg_builtin_mpi_in_place     = ctx->params.mpi_in_place;
    ucg_builtin_op_is_commute_cb = ctx->params.op_is_commute_f;

    *planc_ctx_p = bctx;
    return UCS_OK;

err_release_config:
    ucs_config_parser_release_opts(&bctx->config, ucg_builtin_config_table);
err_free_ctx:
    ucs_free(bctx);
    return status;
}

static void ucg_builtin_cleanup(ucg_planc_ctx_h ctx)
{
    ucg_builtin_ctx_t *bctx = (ucg_builtin_ctx_t*)ctx;
    /* Currently, this function is invoked only when the process exits.
     * So it's OK not to unset am handler. */

    ucs_ptr_array_cleanup(&bctx->group_by_id);
    ucs_config_parser_release_opts(&bctx->config, ucg_builtin_config_table);
    ucs_free(bctx);
    return;
}

UCG_PLAN_COMPONENT_DEFINE(ucg_builtin_component, "builtin",
                          ucg_builtin_query, ucg_builtin_init,
                          ucg_builtin_cleanup, UCG_BUILTIN_CONFIG_PREFIX,
                          ucg_builtin_config_table, ucg_builtin_config_t);