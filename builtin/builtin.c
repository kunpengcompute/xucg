/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * Description: Algorithm acceleration component architecture of UCG
 * Notes: See file LICENSE for terms.
 */

#include <string.h>
#include <ucs/debug/memtrack.h>
#include <ucs/profile/profile.h>
#include <ucg/api/ucg_mpi.h>
#include <ucg/base/ucg_group.h>
#include <ucg/api/ucg_plan_component.h>

#include "ops/builtin_ops.h"
#include "plan/builtin_plan.h"
#include "plan/builtin_plan_cache.h"

#define CACHE_SIZE 1000
#define RECURSIVE_FACTOR 2
#define DEFAULT_INTER_KVALUE 8
#define DEFAULT_INTRA_KVALUE 2
#define DATATYPE_ALIGN 16

#define UCG_BUILTIN_SUPPORT_MASK (UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST)

static ucs_config_field_t ucg_builtin_config_table[] = {

    {"BMTREE_", "", NULL, ucs_offsetof(ucg_builtin_config_t, bmtree),
     UCS_CONFIG_TYPE_TABLE(ucg_builtin_binomial_tree_config_table)},
#if ENABLE_UCG_HICOLL
    {"INC_", "", NULL, ucs_offsetof(ucg_builtin_config_t, inc),
    UCS_CONFIG_TYPE_TABLE(ucg_inc_config_table)},

    {"NAP_", "", NULL, ucs_offsetof(ucg_builtin_config_t, NAP),
    UCS_CONFIG_TYPE_TABLE(ucg_builtin_NAP_config_table)},

    {"LADD_THEROTTLED_FACTOR", "0", "throttle factor",
    ucs_offsetof(ucg_builtin_config_t, throttle_factor), UCS_CONFIG_TYPE_UINT},
#endif
    {"BCAST_ALGORITHM", "0", "Bcast algorithm",
     ucs_offsetof(ucg_builtin_config_t, bcast_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"ALLREDUCE_ALGORITHM", "0", "Allreduce algorithm",
     ucs_offsetof(ucg_builtin_config_t, allreduce_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"BARRIER_ALGORITHM", "0", "Barrier algorithm",
     ucs_offsetof(ucg_builtin_config_t, barrier_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"ALLTOALLV_ALGORITHM", "0", "Alltoallv algorithm",
    ucs_offsetof(ucg_builtin_config_t, alltoallv_algorithm), UCS_CONFIG_TYPE_DOUBLE},

    {"TREES_", "", NULL, ucs_offsetof(ucg_builtin_config_t, trees),
    UCS_CONFIG_TYPE_TABLE(ucg_builtin_trees_config_table)},

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

    /* To ensure consistency of allreduce calculation results,you need to enable this flag.
    By default, this function is disabled. If this flag is enabled, the performance of the
    allreduce tree algorithm interface decreases by 5%. */
    {"REDUCE_CONSISTENCY", "n", "reduce consistency flag",
    ucs_offsetof(ucg_builtin_config_t, reduce_consistency), UCS_CONFIG_TYPE_BOOL},
    {NULL}
};

struct ucg_builtin_algorithm ucg_algo = {
    .bmtree       = 1,
    .kmtree       = 0,
    .kmtree_intra = 0,
    .recursive    = 1,
    .bruck        = 1,
    .topo         = 0,
    .topo_level   = UCG_GROUP_HIERARCHY_LEVEL_NODE,
    .ring         = 0,
    .NAP          = 0,
    .pipeline     = 0,
    .feature_flag = UCG_ALGORITHM_SUPPORT_COMMON_FEATURE,
    .binary_block = 0,
    .ladd         = 0,
    .plummer      = 0,
};

struct ucg_builtin_group_ctx {
    ucs_list_link_t           send_head;    /* request list for (re)send */

    ucg_group_h               group;
    const ucg_group_params_t *group_params;
    ucg_group_id_t            group_id;
    uint16_t                  am_id;
    ucs_list_link_t           plan_head;    /* for resource release */
    ucg_builtin_config_t     *config;

    ucg_builtin_comp_slot_t   *slots;
};

typedef struct ucg_builtin_ctx {
    unsigned slots_total;
    ucg_builtin_comp_slot_t **slots;
} ucg_builtin_ctx_t;

static ucg_builtin_comp_slot_t *ucg_builtin_alloc_slot()
{
    ucg_builtin_comp_slot_t *slot =
        ucs_malloc(sizeof(ucg_builtin_comp_slot_t) * UCG_BUILTIN_MAX_CONCURRENT_OPS, "ucg_msg_slot");
    if (slot == NULL) {
        return NULL;
    }

    unsigned i;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        ucs_list_head_init(&slot[i].msg_head);
        slot[i].mp = NULL;
        slot[i].cb = NULL;
        slot[i].coll_id = 0;
        slot[i].step_idx = 0;
    }
    return slot;
}

static void ucg_builtin_free_slot(ucg_builtin_comp_slot_t *slot)
{
    if (!ucs_list_is_empty(&slot->msg_head)) {
        ucs_warn("massage head is not empty!");
    }
    ucs_free(slot);
}

static ucs_status_t ucg_builtin_init_ctx(ucg_builtin_ctx_t **ctx)
{
    /* The applied memory is reclaimed by the operating system. */
    (*ctx) = UCS_ALLOC_CHECK(sizeof(ucg_builtin_ctx_t), "alloc ucg_builtin_ctx_t");

    (*ctx)->slots_total = 0;
    (*ctx)->slots       = NULL;
    return UCS_OK;
}

static ucs_status_t ucg_builtin_extend_slots(ucg_builtin_ctx_t *ctx, unsigned max_size)
{
    if (ctx->slots_total >= max_size) {
        return UCS_OK;
    }
    size_t slots_size = max_size * sizeof(ucg_builtin_comp_slot_t *);
    ucg_builtin_comp_slot_t **new_slots = ucs_realloc(ctx->slots, slots_size, "ucg_msg_slots");
    if (new_slots == NULL) {
        return UCS_ERR_NO_MEMORY;
    }
    ctx->slots = new_slots;
    unsigned i;
    for (i = ctx->slots_total; i < max_size; i++) {
        ctx->slots[i] = ucg_builtin_alloc_slot();
        if (ctx->slots[i] == NULL) {
            goto cleanup;
        }
    }
    ctx->slots_total = max_size;
    return UCS_OK;

cleanup:
    while((i--) > ctx->slots_total) {
        ucg_builtin_free_slot(ctx->slots[i]);
        ctx->slots[i] = NULL;
    }
    return UCS_ERR_NO_MEMORY;
}

static ucg_builtin_ctx_t *ucg_builtin_get_ctx(ucg_worker_h worker)
{
    ucg_builtin_ctx_t **ctx = UCG_WORKER_TO_COMPONENT_CTX(ucg_builtin_component, worker);
    if (*ctx == NULL) {
        ucs_status_t status = ucg_builtin_init_ctx(ctx);
        if (status != UCS_OK) {
            return NULL;
        }
    }
    return (*ctx);
}

static ucg_builtin_comp_slot_t *ucg_builtin_get_slot(ucg_worker_h worker, unsigned group_id)
{
    ucg_builtin_ctx_t *ctx = ucg_builtin_get_ctx(worker);
    if (ctx == NULL) {
        return NULL;
    }
    if (ctx->slots_total <= group_id) {
        return NULL;
    }
    return ctx->slots[group_id];
}

static ucg_builtin_comp_slot_t *ucg_builtin_set_slot(ucg_worker_h worker, unsigned group_id, ucs_mpool_t *group_am_mp)
{
    ucg_builtin_ctx_t *ctx = ucg_builtin_get_ctx(worker);
    if (ctx == NULL) {
        return NULL;
    }
    if (ctx->slots_total <= group_id) {
        ucs_status_t status = ucg_builtin_extend_slots(ctx, group_id + 1);
        if (status != UCS_OK) {
            return NULL;
        }
    }

    unsigned i;
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        ucg_builtin_comp_slot_t *slot = &ctx->slots[group_id][i];
        slot->mp = group_am_mp;

    }
    return ctx->slots[group_id];
}
/*
 * fix white-box review
 */
void ucg_builtin_free(void **p)
{
    if (*p != NULL) {
        ucs_free(*p);
        *p = NULL;
    }
}

static ucs_status_t ucg_builtin_query(unsigned ucg_api_version,
                                      ucg_plan_desc_t **desc_p, unsigned *num_descs_p)
{
    ucs_status_t status              = ucg_plan_single(&ucg_builtin_component,
                                                       desc_p, num_descs_p);
    if (status == UCS_OK) {
        (*desc_p)[0].modifiers_supported = UCG_BUILTIN_SUPPORT_MASK;
        (*desc_p)[0].flags = 0;
    }
    return status;
}

enum ucg_builtin_plan_topology_type ucg_builtin_choose_type(enum ucg_collective_modifiers flags)
{
    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE) {
        return UCG_PLAN_TREE_FANOUT;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION) {
        return UCG_PLAN_TREE_FANIN;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE) {
        if (ucg_algo.recursive) {
            return UCG_PLAN_RECURSIVE;
        } else if (ucg_algo.ring) {
            return UCG_PLAN_RING;
        } else if (ucg_algo.NAP) {
            return UCG_PLAN_NAP;
        } else if (ucg_algo.binary_block) {
            return UCG_PLAN_BINARY_BLOCK;
        } else {
            return UCG_PLAN_TREE_FANIN_FANOUT;
        }
    }

    if (flags & ucg_predefined_modifiers[UCG_PRIMITIVE_ALLTOALL]) {
        return UCG_PLAN_BRUCK;
    }

    if (flags & ucg_predefined_modifiers[UCG_PRIMITIVE_ALLTOALLV]) {
        return ucg_algo.plummer ? UCG_PLAN_ALLTOALLV_PLUMMER : UCG_PLAN_ALLTOALLV_LADD;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_ALLGATHER) {
        return ucg_algo.bruck ? UCG_PLAN_BRUCK : UCG_PLAN_RECURSIVE;
    }

    return UCG_PLAN_TREE_FANIN_FANOUT;
}

static inline void ucg_builtin_release_desc_self(void *desc)
{
    ucs_free(desc);
}

static ucs_status_t ucg_builtin_am_process(ucg_builtin_comp_slot_t *slot, void *data, size_t length,
                                           unsigned am_flags)

{
    ucg_builtin_header_t *header = data;
   /* Consume the message if it fits the current collective and step index */
    if (ucs_likely(slot->cb && (header->local_id == slot->local_id))) {
        /* Make sure the packet indeed belongs to the collective currently on */
        ucs_debug("ucg_builtin_am_handler CB: coll_id %u step_idx %u cb %p pending %u",
                  header->coll_id, header->step_idx, slot->cb, slot->req.pending);

        if ((slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) &&
            (slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND)) {
             /* receive from "multiple" EPs with "multiple" fragments */
            unsigned recv_zcopy_cnt = slot->req.step->fragments_recv * slot->req.step->phase->ep_cnt;
            /* Zcopy recv before sending finished, store msg */
            if (slot->req.pending > recv_zcopy_cnt) {
                if (++slot->req.step->zcopy.num_store > recv_zcopy_cnt) {
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
    ucs_status_t ret;
    ucg_builtin_comp_desc_t* desc = NULL;
am_handler_store:
    if (am_flags & UCT_CB_PARAM_FLAG_DESC) {
        desc = (ucg_builtin_comp_desc_t*)((char*)data -
                offsetof(ucg_builtin_comp_desc_t, header));
        desc->release = uct_iface_release_desc;
        ret = UCS_INPROGRESS;
    } else {
        if (slot->mp == NULL) {
            desc = ucs_malloc(sizeof(ucg_builtin_comp_desc_t) + (length - sizeof(ucg_builtin_header_t)),
                "alloc builtin comp desc");
            if (desc == NULL) {
                /* The UCT layer does not detect other error status codes and only identifies
                   whether the status is UCS_INPROGRESS and then process. We do not need UCT
                   desc, just return UCS_OK. */
                return UCS_OK;
            }
            desc->release = ucg_builtin_release_desc_self;
        } else {
            /* Cannot use existing descriptor - must allocate my own... */
            desc = (ucg_builtin_comp_desc_t*)ucs_mpool_get_inline(slot->mp);
            if (desc == NULL) {
               /* The UCT layer does not detect other error status codes and only identifies
                   whether the status is UCS_INPROGRESS and then process. We do not need UCT
                   desc, just return UCS_OK. */
                return UCS_OK;
            }
            desc->release = ucs_mpool_put_inline;
        }
        memcpy(&desc->header, data, length);
        ret = UCS_OK;
    }

    ucs_debug("ucg_builtin_am_handler STORE: group_id %u coll_id %u(%u) step_idx %u(%u)",
              header->group_id, header->coll_id, slot->coll_id, header->step_idx, slot->step_idx);

    desc->super.flags = am_flags;
    desc->super.length = length - sizeof(ucg_builtin_header_t);
    ucs_list_add_tail(&slot->msg_head, &desc->super.tag_list[0]);
    return ret;
}

UCS_PROFILE_FUNC(ucs_status_t, ucg_builtin_am_handler,
                 (arg, data, length, am_flags),
                 void *arg, void *data, size_t length, unsigned am_flags)
{
    ucg_worker_h worker           = (ucg_worker_h)arg;
    ucg_builtin_header_t *header  = data;
    ucg_builtin_comp_slot_t *slot = NULL;
    ucg_group_id_t group_id       = header->group_id;
    ucs_assert(length >= sizeof(header));

    slot = ucg_builtin_get_slot(worker, group_id);
    if (slot == NULL) {
        slot = ucg_builtin_set_slot(worker, group_id, NULL);
        if (slot == NULL) {
            ucs_fatal("Message abandoned, collection operation cannot be performed.");
        }
    }
    return ucg_builtin_am_process(&slot[header->coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS], data, length, am_flags);
}

void ucg_builtin_msg_dump(ucp_worker_h worker, uct_am_trace_type_t type,
                          uint8_t id, const void *data, size_t length,
                          char *buffer, size_t max)
{
    const ucg_builtin_header_t *header = (const ucg_builtin_header_t*)data;
    snprintf(buffer, max, "COLLECTIVE [coll_id %u step_idx %u offset %lu length %lu]",
             (unsigned)header->coll_id, (unsigned)header->step_idx,
             (uint64_t)header->remote_offset, length - sizeof(*header));
}

static ucs_status_t ucg_builtin_init_plan_config(ucg_plan_component_t *plan_component)
{
    ucg_builtin_config_t *config = (ucg_builtin_config_t*)plan_component->plan_config;
    config->cache_size = CACHE_SIZE;
    config->pipelining = 0;
    config->recursive.factor = RECURSIVE_FACTOR;

    /* K-nomial tree algorithm require all K value is bigger than 1 */
    if (config->bmtree.degree_inter_fanout <= 1 || config->bmtree.degree_inter_fanin <= 1 ||
        config->bmtree.degree_intra_fanout <= 1 || config->bmtree.degree_intra_fanin <= 1) {
        ucs_info("K-nomial tree algorithm require all K value is bigger than one, switch to default parameter sets");
        config->bmtree.degree_inter_fanout = DEFAULT_INTER_KVALUE;
        config->bmtree.degree_inter_fanin  = DEFAULT_INTER_KVALUE;
        config->bmtree.degree_intra_fanout = DEFAULT_INTRA_KVALUE;
        config->bmtree.degree_intra_fanin  = DEFAULT_INTRA_KVALUE;
    }

    ucs_info("plan %s bcast %u allreduce %u barrier %u alltoallv %u"
             " inter_fanout %u inter_fanin %u intra_fanout %u intra_fanin %u",
             plan_component->name, (unsigned)config->bcast_algorithm, (unsigned)config->allreduce_algorithm,
             (unsigned)config->barrier_algorithm, (unsigned)config->alltoallv_algorithm, config->bmtree.degree_inter_fanout,
             config->bmtree.degree_inter_fanin, config->bmtree.degree_intra_fanout, config->bmtree.degree_intra_fanin);

    return UCS_OK;
}

static ucs_status_t ucg_builtin_create(ucg_plan_component_t *plan_component,
                                       ucg_worker_h worker,
                                       ucg_group_h group,
                                       unsigned base_am_id,
                                       ucg_group_id_t group_id,
                                       ucs_mpool_t *group_am_mp,
                                       const ucg_group_params_t *group_params)
{
    /* Fill in the information in the per-group context */
    ucg_builtin_group_ctx_t *gctx =
            UCG_GROUP_TO_COMPONENT_CTX(ucg_builtin_component, group);
    ucg_builtin_mpi_reduce_cb     = group_params->mpi_reduce_f;
    gctx->group                   = group;
    gctx->group_id                = group_id;
    gctx->group_params            = group_params;
    gctx->config                  = plan_component->plan_config;
    gctx->am_id                   = base_am_id;
    ucs_list_head_init(&gctx->send_head);
    ucs_list_head_init(&gctx->plan_head);

    gctx->slots = ucg_builtin_set_slot(worker, group_id, group_am_mp);
    if (gctx->slots == NULL) {
        return UCS_ERR_NO_RESOURCE;
    }

    if (ucg_builtin_pcache_init(group)) {
        ucs_error("plan cache init fail");
        return UCS_ERR_NO_MEMORY;
    }

    return ucg_builtin_init_plan_config(plan_component);
}

static void ucg_builtin_clean_phases(ucg_builtin_plan_t *plan)
{
    int i;
    for (i = 0; i < plan->phs_cnt; i++) {
        ucg_builtin_free((void **)&plan->phss[i].recv_cache_buffer);
        ucg_builtin_free((void **)&plan->phss[i].ucp_eps);
        ucg_builtin_free((void **)&plan->phss[i].ep_thresh);
    }

#if ENABLE_DEBUG_DATA
    ucg_builtin_free((void **)&plan->phss[0].indexes);
#endif
}

ucs_status_t ucg_builtin_remove_ep(ucp_ep_h *ep, ucg_group_h group)
{
    ucp_ep_ext_gen_t *ep_ext = NULL;
    ucp_ep_ext_gen_t *tmp = NULL;
    ucs_list_for_each_safe(ep_ext, tmp, &group->worker->all_eps, ep_list) {
        ucp_ep_h tmp_ep = (ucp_ep_h)ucs_strided_elem_get(ep_ext, 1, 0);
        if (tmp_ep == *ep) {
            ucp_ep_disconnected(tmp_ep, 1);
            ucs_list_del(&ep_ext->ep_list);
            break;
        }
    }
    return UCS_OK;
}

ucs_status_t ucg_builtin_destroy_plan(ucg_builtin_plan_t *plan, ucg_group_h group)
{
    for (unsigned i = 0; i < plan->phs_cnt; i++) {
        if (plan->phss[i].ucp_eps != NULL) {
            for (unsigned j = 0; j < plan->phss[i].ep_cnt; j++) {
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

    return UCS_OK;
}

static void ucg_builtin_destroy(ucg_group_h group)
{
    ucg_builtin_group_ctx_t *gctx = UCG_GROUP_TO_COMPONENT_CTX(ucg_builtin_component, group);
    unsigned i;
    ucg_builtin_pcache_destroy(group);
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        if (gctx->slots[i].cb != NULL) {
            ucs_debug("Collective operation #%u has been left incomplete (Group #%u)",
                      gctx->slots[i].coll_id, gctx->group_id);
        }

        while (!ucs_list_is_empty(&gctx->slots[i].msg_head)) {
            ucg_builtin_comp_desc_t *desc =
                    ucs_list_extract_head(&gctx->slots[i].msg_head,
                                          ucg_builtin_comp_desc_t, super.tag_list[0]);
            ucs_debug("Collective operation #%u has %u bytes left pending for step #%u (Group #%u)",
                      desc->header.coll_id, desc->super.length, desc->header.step_idx, desc->header.group_id);
            desc->release(desc);
            desc = NULL;
        }
    }

    while (!ucs_list_is_empty(&gctx->plan_head)) {
        ucg_builtin_plan_t *plan = ucs_list_head(&gctx->plan_head,
                                                 ucg_builtin_plan_t, list);
        ucs_status_t status = ucg_builtin_destroy_plan(plan, group);
        if (ucs_unlikely(status != UCS_OK)) {
            return;
        }
    }
}

static unsigned ucg_builtin_progress(ucg_group_h group)
{
    ucg_builtin_group_ctx_t *gctx =
            UCG_GROUP_TO_COMPONENT_CTX(ucg_builtin_component, group);
    if (ucs_likely(ucs_list_is_empty(&gctx->send_head))) {
        return 0;
    }

    /*
     * Since calling @ref ucg_builtin_step_execute may place the operation in
     * the same list again, the list of pending sends is moved to a temporary
     * head, then drained - each call "resets" the state of that operation.
     */
    unsigned ret = 0;
    UCS_LIST_HEAD(temp_head);
    ucs_list_splice_tail(&temp_head, &gctx->send_head);
    ucs_list_head_init(&gctx->send_head);
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

ucs_mpool_ops_t ucg_builtin_plan_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = ucs_empty_function,
    .obj_cleanup   = ucs_empty_function
};

void ucg_builtin_fillin_algo(struct ucg_builtin_algorithm *algo,
                             unsigned bmtree,
                             unsigned kmtree,
                             unsigned kmtree_intra,
                             unsigned recursive,
                             unsigned topo,
                             unsigned ring,
                             unsigned NAP,
                             unsigned binary_block)
{
    algo->bmtree = bmtree;
    algo->kmtree = kmtree;
    algo->kmtree_intra = kmtree_intra;
    algo->recursive = recursive;
    algo->topo = topo;
    algo->ring = ring;
    algo->NAP  = NAP;
    algo->binary_block = binary_block;
}

static void ucg_builtin_init_algo(struct ucg_builtin_algorithm *algo)
{
    ucg_builtin_fillin_algo(algo, 1, 0, 0, 1, 0, 0, 0, 0);
    algo->bruck        = 1,
    algo->topo_level   = UCG_GROUP_HIERARCHY_LEVEL_NODE,
    algo->pipeline     = 0;
    algo->feature_flag = UCG_ALGORITHM_SUPPORT_COMMON_FEATURE;
    algo->inc          = 0;
    algo->ladd         = 0;
    algo->plummer      = 0;
}

void ucg_builtin_bcast_algo_switch(const enum ucg_builtin_bcast_algorithm bcast_algo_decision,
                                           struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
    algo->bruck = 1;
    switch (bcast_algo_decision) {
        case UCG_ALGORITHM_BCAST_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 0, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 0, 0, 1, 0, 0, 0);
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0, 0, 0);
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_INC:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->inc = 1;
            break;
        default:
            ucg_builtin_bcast_algo_switch(UCG_ALGORITHM_BCAST_NODE_AWARE_KMTREE_AND_BMTREE, algo);
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
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 1, 0, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_INC:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->inc = 1;
            break;
        case UCG_ALGORITHM_BARRIER_SOCKET_AWARE_INC:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            algo->inc = 1;
            break;
        case UCG_ALGORITHM_BARRIER_NAP:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
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
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 1, 0, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE |
                                  UCG_ALGORITHM_SUPPORT_ALLREDUCE_RARE_FEATURE |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_ALLREDUCE_RING:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 0, 1, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE |
                                  UCG_ALGORITHM_SUPPORT_ALLREDUCE_RARE_FEATURE |
                                  UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_KMTREE:
            ucg_builtin_fillin_algo(algo, 1, 1, 1, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_INC:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->inc = 1;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_INC:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0, 0, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            algo->inc = 1;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NAP:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 1, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_RABENSEIFNER_BINARY_BLOCK:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 0, 0, 0, 1);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RABENSEIFNER_BINARY_BLOCK:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 1, 0, 0, 1);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RABENSEIFNER_BINARY_BLOCK:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 1, 0, 0, 1);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        default:
            ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE, algo);
            break;
    }
}

void ucg_builtin_alltoallv_algo_switch(const enum ucg_builtin_alltoallv_algorithm alltoallv_algo_decision,
                                       struct ucg_builtin_algorithm *algo)
{
    algo->topo_level   = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
    algo->bruck = 0;
    switch (alltoallv_algo_decision) {
        case UCG_ALGORITHM_ALLTOALLV_LADD:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 0, 0, 0, 0);
            algo->ladd = 1;
            break;
        case UCG_ALGORITHM_ALLTOALLV_NODE_AWARE_PLUMMER:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 1, 0, 0, 0);
            algo->plummer = 1;
            break;
        default:
            ucg_builtin_alltoallv_algo_switch(UCG_ALGORITHM_ALLTOALLV_LADD, algo);
            break;
    }
}

enum ucg_group_member_distance ucg_builtin_get_distance(const ucg_group_params_t *group_params,
                                               ucg_group_member_index_t rank1,
                                               ucg_group_member_index_t rank2)
{
    return group_params->mpi_rank_distance(group_params->cb_group_obj, rank1, rank2);
}

ucs_status_t ucg_builtin_check_continuous_number(const ucg_group_params_t *group_params,
                                                 enum ucg_group_member_distance domain_distance,
                                                 unsigned *discont_flag)
{
    if (domain_distance == UCG_GROUP_MEMBER_DISTANCE_SOCKET) {
        *discont_flag = group_params->topo_args.srank_uncontinue;
    } else {
        *discont_flag = group_params->topo_args.nrank_uncontinue;
    }
    return UCS_OK;
}

void choose_distance_from_topo_aware_level(enum ucg_group_member_distance *domain_distance)
{
    switch (ucg_algo.topo_level) {
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

int ucg_builtin_op_can_reuse(const ucg_plan_t *plan, const ucg_op_t *op,
                             const ucg_collective_params_t *params)
{
    ucp_datatype_t send_dtype = UCP_DATATYPE_CONTIG;
    ucg_builtin_plan_t *builtin_plan = (ucg_builtin_plan_t *)plan;
    ucg_builtin_op_t *builtin_op = (ucg_builtin_op_t *)op;

    if (builtin_op->send_dt != NULL) {
        return 0;
    }

    /* Alltoallv does not consider op reuse. */
    if (params->type.modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLTOALLV])  {
        return 0;
    }
    if (params->send.count > 0) {
        builtin_plan->convert_f(params->send.dt_ext, &send_dtype);
        if (!UCG_DT_IS_CONTIG(params, send_dtype)) {
            return 0;
        }
    }

    return 1;
}

void ucg_builtin_log_algo()
{
    ucs_info("bmtree %u kmtree %u kmtree_intra %u recur %u bruck %u topo %u "
             "level %u ring %u pipe %u nap %u binary_block %u ladd %u plummer %u ",ucg_algo.bmtree, ucg_algo.kmtree,
             ucg_algo.kmtree_intra, ucg_algo.recursive, ucg_algo.bruck, ucg_algo.topo, (unsigned)ucg_algo.topo_level,
             ucg_algo.ring, ucg_algo.pipeline, ucg_algo.NAP, ucg_algo.binary_block, ucg_algo.ladd, ucg_algo.plummer);
}

static void ucg_builtin_plan_create(ucg_builtin_plan_t *plan,
                                    enum ucg_builtin_plan_topology_type plan_topo_type,
                                    ucg_collective_params_t *coll_params,
                                    ucg_builtin_group_ctx_t *builtin_ctx)
{
    plan->convert_f = builtin_ctx->group_params->mpi_dt_convert;
    plan->dtspan_f = builtin_ctx->group_params->mpi_datatype_span;
    plan->resend = &builtin_ctx->send_head;
    plan->slots = &builtin_ctx->slots[0];
    plan->am_id = builtin_ctx->am_id;
}

STATIC_GTEST void ucg_builtin_set_algo(coll_type_t ctype, int algo_id, ucg_builtin_algo_t *algo)
{
    ucg_builtin_init_algo(algo);
    switch (ctype) {
        case COLL_TYPE_BARRIER:
            ucg_builtin_barrier_algo_switch(algo_id, algo);
            break;

        case COLL_TYPE_BCAST:
            ucg_builtin_bcast_algo_switch(algo_id, algo);
            break;

        case COLL_TYPE_ALLREDUCE:
            ucg_builtin_allreduce_algo_switch(algo_id, algo);
            break;

        case COLL_TYPE_ALLTOALLV:
            ucg_builtin_alltoallv_algo_switch(algo_id, algo);
            break;

        default:
            ucs_error("invalid collective type %d", ctype);
            break;
    }

    ucg_builtin_log_algo();
}

static ucs_status_t ucg_builtin_plan(ucg_group_h group, int algo_id,
                                     ucg_collective_params_t *coll_params,
                                     ucg_plan_t **plan_p)
{
    ucg_builtin_group_ctx_t *builtin_ctx = UCG_GROUP_TO_COMPONENT_CTX(ucg_builtin_component, group);
    ucg_plan_component_t *plan_component = &ucg_builtin_component;
    ucg_collective_type_t *coll_type = &coll_params->type;
    enum ucg_builtin_plan_topology_type plan_topo_type;
    ucg_builtin_plan_t *plan = NULL;
    ucs_status_t status;
    ucg_builtin_set_algo(coll_params->coll_type, algo_id, &ucg_algo);
    plan_topo_type = ucg_builtin_choose_type(coll_type->modifiers);

    ucs_debug("plan topo type: %d", plan_topo_type);

    /* Build the topology according to the requested */
    switch (plan_topo_type) {
        case UCG_PLAN_RECURSIVE:
            status = ucg_builtin_recursive_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                                  builtin_ctx->group_params, coll_type, &plan);
            break;

        case UCG_PLAN_RING:
            status = ucg_builtin_ring_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                             builtin_ctx->group_params, coll_type, &plan);
            break;

        case UCG_PLAN_BINARY_BLOCK:
            status = ucg_builtin_binary_block_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                             builtin_ctx->group_params, coll_type, &plan);
            break;
#if ENABLE_UCG_HICOLL
        case UCG_PLAN_NAP:
            status = ucg_builtin_NAP_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                             builtin_ctx->group_params, coll_type, &plan);
            break;

        case UCG_PLAN_ALLTOALLV_LADD:
            status = ucg_builtin_throttled_scatter_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                             builtin_ctx->group_params, coll_params, &plan);
            break;

        case UCG_PLAN_ALLTOALLV_PLUMMER:
            status = ucg_builtin_Plummer_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                             builtin_ctx->group_params, coll_type, coll_params, &plan);
            break;
#endif
        default:
            status = ucg_builtin_binomial_tree_create(builtin_ctx, plan_topo_type, plan_component->plan_config,
                                                      builtin_ctx->group_params, coll_type, &plan);
            break;
    }

    if (status != UCS_OK) {
        ucg_builtin_free((void **)&plan);
        return status;
    }

    ucs_list_head_init(&plan->super.op_head);

    /* Create a memory-pool for operations for this plan */
    size_t op_size = sizeof(ucg_builtin_op_t) + plan->phs_cnt * sizeof(ucg_builtin_op_step_t);
    status = ucs_mpool_init(&plan->op_mp, 0, op_size, 0, UCS_SYS_CACHE_LINE_SIZE,
                            1, UINT_MAX, &ucg_builtin_plan_mpool_ops, "ucg_builtin_plan_mp");
    if (status != UCS_OK) {
        ucg_builtin_free((void **)&plan);
        return status;
    }

    ucs_list_add_head(&builtin_ctx->plan_head, &plan->list);
    ucg_builtin_plan_create(plan, plan_topo_type, coll_params, builtin_ctx);
    plan->ucg_algo = ucg_algo;
    *plan_p         = (ucg_plan_t*)plan;
    return UCS_OK;
}

STATIC_GTEST void ucg_builtin_print(ucg_plan_t *plan, const ucg_collective_params_t *coll_params)
{
    unsigned major_version, minor_version, release_number;
    ucp_get_version(&major_version, &minor_version, &release_number);
    printf("version: %d.%d\n", major_version, minor_version);

    printf("plan name: %s\n", plan->planner->name);
}

STATIC_GTEST void  ucg_builtin_set_phase_thresh_max_short(ucg_builtin_group_ctx_t *ctx,
                                             ucg_builtin_plan_phase_t *phase)
{

    phase->send_thresh.max_short_one = (phase->ep_attr->cap.am.max_short < sizeof(ucg_builtin_header_t)) ?
        0 : (phase->ep_attr->cap.am.max_short - sizeof(ucg_builtin_header_t));

    phase->send_thresh.max_short_max = (phase->send_thresh.max_short_one == 0) ? 0 : ctx->config->short_max_tx;

    if (phase->send_thresh.max_short_one > phase->send_thresh.max_short_max) {
        phase->send_thresh.max_short_one = phase->send_thresh.max_short_max;
    }

    phase->send_thresh.max_short_one -= phase->send_thresh.max_short_one % DATATYPE_ALIGN;
}

void  ucg_builtin_set_phase_thresh_max_bcopy_zcopy(ucg_builtin_group_ctx_t *ctx,
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

void  ucg_builtin_set_phase_thresholds(ucg_builtin_group_ctx_t *ctx,
                                       ucg_builtin_plan_phase_t *phase)
{
    ucg_builtin_set_phase_thresh_max_short(ctx, phase);
    ucg_builtin_set_phase_thresh_max_bcopy_zcopy(ctx, phase);

    phase->send_thresh.md_attr_cap_max_reg = (phase->md_attr->cap.flags & UCT_MD_FLAG_NEED_MEMH) ? phase->md_attr->cap.max_reg : 0;
    phase->send_thresh.initialized = 1;

    if (!phase->recv_thresh.initialized) {
        phase->recv_thresh = phase->send_thresh;
        phase->recv_thresh.initialized = 1;
    }
}

void ucg_builtin_log_phase_info(ucg_builtin_plan_phase_t *phase, ucg_group_member_index_t idx)
{
    ucs_info("phase create: %p, dest %" PRIu64 ", short_one %zu, short_max %zu, bcopy_one %zu, bcopy_max %zu, zcopy_one %zu, max_reg %zu",
               phase, idx, phase->send_thresh.max_short_one, phase->send_thresh.max_short_max, phase->send_thresh.max_bcopy_one, phase->send_thresh.max_bcopy_max, phase->send_thresh.max_zcopy_one, phase->md_attr->cap.max_reg);
}

ucs_status_t ucg_builtin_connect(ucg_builtin_group_ctx_t *ctx,
                                 ucg_group_member_index_t idx, ucg_builtin_plan_phase_t *phase,
                                 unsigned phase_ep_index)
{
    uct_ep_h ep;
    ucp_ep_h ucp_ep;
    unsigned alloc_cnt;
    ucs_status_t status = ucg_plan_connect(ctx->group, idx, &ep,
                                           &phase->ep_attr, &phase->md, &phase->md_attr, &ucp_ep);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }
    if (phase->ucp_eps == NULL) {
        alloc_cnt = (phase_ep_index != UCG_BUILTIN_CONNECT_SINGLE_EP && phase_ep_index >= phase->ep_cnt) ?
                    (phase_ep_index + 1) : phase->ep_cnt;
        phase->ucp_eps = UCS_ALLOC_CHECK(sizeof(ucp_ep_h) * alloc_cnt, "ucp_eps");
        phase->ep_thresh = UCS_ALLOC_CHECK(sizeof(ucg_builtin_tl_threshold_t) * alloc_cnt, "uct_ep thresh");
    }
    phase->ucp_eps[(phase_ep_index == UCG_BUILTIN_CONNECT_SINGLE_EP) ? 0 : phase_ep_index] = ucp_ep;

#if ENABLE_DEBUG_DATA
    phase->indexes[(phase_ep_index != UCG_BUILTIN_CONNECT_SINGLE_EP) ?
            phase_ep_index : 0] = idx;
#endif
    if (!ep) {
        phase->send_thresh.max_short_one = SIZE_MAX;
        phase->md = NULL;
        phase->md_attr = NULL;
        return UCS_OK;
    }

    if (phase_ep_index == UCG_BUILTIN_CONNECT_SINGLE_EP) {
        phase->single_ep = ep;
    } else {
        /*
         * Only avoid for case of Bruck plan because phase->ep_cnt = 1
         * with 2 endpoints(send + recv) actually
         */
        if (phase->method != UCG_PLAN_METHOD_ALLGATHER_BRUCK &&
            phase->method != UCG_PLAN_METHOD_ALLTOALL_BRUCK &&
            phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
            phase->method != UCG_PLAN_METHOD_INC &&
            phase->method != UCG_PLAN_METHOD_ALLTOALLV_LADD &&
            phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
            ucs_assert(phase_ep_index < phase->ep_cnt);
        }
        phase->multi_eps[phase_ep_index] = ep;
    }

    /* Set the thresholds */
    ucg_builtin_set_phase_thresholds(ctx, phase);
    phase->ep_thresh[(phase_ep_index != UCG_BUILTIN_CONNECT_SINGLE_EP) ? phase_ep_index : 0] = phase->send_thresh;
    ucg_builtin_log_phase_info(phase, idx);

    return status;
}

ucg_group_member_index_t ucg_builtin_get_local_index(ucg_group_member_index_t global_index,
                                                    const ucg_group_member_index_t *local_members,
                                                    ucg_group_member_index_t member_cnt)
{
    ucg_group_member_index_t local_index = 0;

    ucg_group_member_index_t i;
    for (i = 0; i < member_cnt ; i++) {
        if (local_members[i] == global_index) {
            local_index = i;
            break;
        }
    }
    return local_index;
}

/* Get the -x UCX_BUILTIN_REDUCE_CONSISTENCY config value */
int ucg_is_allreduce_consistency(const ucg_builtin_group_ctx_t *ctx)
{
    return ctx->config->reduce_consistency;
}

UCG_PLAN_COMPONENT_DEFINE(ucg_builtin_component, "builtin",
                          sizeof(ucg_builtin_group_ctx_t), ucg_builtin_query,
                          ucg_builtin_create, ucg_builtin_destroy,
                          ucg_builtin_progress, ucg_builtin_plan,
                          ucg_builtin_op_create, ucg_builtin_op_trigger,
                          ucg_builtin_op_discard, ucg_builtin_print, "BUILTIN_",
                          ucg_builtin_config_table, ucg_builtin_config_t);
