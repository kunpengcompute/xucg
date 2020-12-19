/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <string.h>
#include <ucs/arch/atomic.h>
#include <ucs/profile/profile.h>
#include <ucp/core/ucp_request.inl>
#include <ucg/api/ucg_plan_component.h>

#include <ucg/base/ucg_group.h> // TODO: (alex) remove later

#include "ops/builtin_ops.h"
#include "ops/builtin_comp_step.inl"
#include "plan/builtin_plan.h"

/* Backport to UCX v1.6.0 */
#ifndef UCS_MEMUNITS_INF
#define UCS_MEMUNITS_INF UCS_CONFIG_MEMUNITS_INF
#endif

#ifdef HAVE_UCP_EXTENSIONS
#define CONDITIONAL_NULL ,NULL
#else
#define CONDITIONAL_NULL
#endif

#define UCG_BUILTIN_PARAM_MASK   (UCG_GROUP_PARAM_FIELD_ID           |\
                                  UCG_GROUP_PARAM_FIELD_MEMBER_COUNT |\
                                  UCG_GROUP_PARAM_FIELD_MEMBER_INDEX |\
                                  UCG_GROUP_PARAM_FIELD_DISTANCES)

#define CACHE_SIZE 1000
#define RECURSIVE_FACTOR 2
#define DEFAULT_INTER_KVALUE 8
#define DEFAULT_INTRA_KVALUE 2

#define UCG_BUILTIN_SUPPORT_MASK (UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE |\
                                  UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST)

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
    {"SHORT_MAX_TX_SIZE", "200", "Largest send operation to use short messages",
     ucs_offsetof(ucg_builtin_config_t, short_max_tx), UCS_CONFIG_TYPE_MEMUNITS},

    {"BCOPY_MAX_TX_SIZE", "32768", "Largest send operation to use buffer copy",
     ucs_offsetof(ucg_builtin_config_t, bcopy_max_tx), UCS_CONFIG_TYPE_MEMUNITS},

    {"MEM_REG_OPT_CNT", "10", "Operation counter before registering the memory",
     ucs_offsetof(ucg_builtin_config_t, mem_reg_opt_cnt), UCS_CONFIG_TYPE_UINT},

    {"MEM_RMA_OPT_CNT", "3", "Operation counter before switching to one-sided sends",
     ucs_offsetof(ucg_builtin_config_t, mem_rma_opt_cnt), UCS_CONFIG_TYPE_UINT},

    {"LARGE_DATATYPE_THRESHOLD", "32", "Large datatype threshold",
     ucs_offsetof(ucg_builtin_config_t, large_datatype_threshold), UCS_CONFIG_TYPE_UINT},

    {"RESEND_TIMER_TICK", "100ms", "Resolution for (async) resend timer",
     ucs_offsetof(ucg_builtin_config_t, resend_timer_tick), UCS_CONFIG_TYPE_TIME},

#if ENABLE_FAULT_TOLERANCE
    {"FT_TIMER_TICK", "100ms", "Resolution for (async) fault-tolerance timer",
     ucs_offsetof(ucg_builtin_config_t, ft_timer_tick), UCS_CONFIG_TYPE_TIME},
#endif

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
    .pipeline     = 0,
    .feature_flag = UCG_ALGORITHM_SUPPORT_COMMON_FEATURE,
};

struct ucg_builtin_group_ctx {
    /*
     * The following is the key structure of a group - an array of outstanding
     * collective operations, one slot per operation. Messages for future ops
     * may be stored in a slot before the operation actually starts.
     *
     * TODO: support more than this amount of concurrent operations...
     */
    ucg_builtin_comp_slot_t   slots[UCG_BUILTIN_MAX_CONCURRENT_OPS];

    /* Mostly control-path, from here on */
    ucg_builtin_ctx_t        *bctx;          /**< global context */
    ucg_group_h               group;         /**< group handle */
    ucp_worker_h              worker;        /**< group's worker */
    ucs_queue_head_t          resend_head;
    const ucg_group_params_t *group_params;  /**< the original group parameters */
    ucg_group_member_index_t  host_proc_cnt; /**< Number of intra-node processes */
    ucg_group_id_t            group_id;      /**< Group identifier */
    ucs_list_link_t           plan_head;     /**< list of plans (for cleanup) */
    ucs_ptr_array_t           faults;        /**< flexible array of faulty members */
    int                       timer_id;      /**< Async. progress timer ID */
#if ENABLE_FAULT_TOLERANCE
    int                       ft_timer_id;   /**< Fault-tolerance timer ID */
#endif
} UCS_V_ALIGNED(UCS_SYS_CACHE_LINE_SIZE);

extern ucg_plan_component_t ucg_builtin_component;

// TODO: (alex) salvage anything?
//static ucs_status_t
//ucg_builtin_choose_topology(enum ucg_collective_modifiers flags,
//                            ucg_group_member_index_t group_size,
//                            ucg_builtin_plan_topology_t *topology)
//{
//    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE) {
//        /* MPI_Bcast / MPI_Scatter */
//        topology->type = UCG_PLAN_TREE_FANOUT;
//        return UCS_OK;
//    }
//
//    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION) {
//        /* MPI_Reduce / MPI_Gather */
//        // TODO: Alex - test operand/operator support
//        topology->type = UCG_PLAN_TREE_FANIN;
//        return UCS_OK;
//    }
//
//    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE) {
//        /* MPI_Allreduce */
//        if (ucs_popcount(group_size) > 1) {
//            /* Not a power of two */
//            topology->type = UCG_PLAN_TREE_FANIN_FANOUT;
//        } else {
//            topology->type = UCG_PLAN_RECURSIVE;
//        }
//        return UCS_OK;
//    }
//
//    /* MPI_Alltoall */
//    ucs_assert(flags == 0);
//    if (ucs_popcount(group_size) == 1) {
//        topology->type = UCG_PLAN_ALLTOALL_BRUCK;
//    } else {
//        topology->type = UCG_PLAN_PAIRWISE;
//    }
//    return UCS_OK;
//}

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
        } else {
            return UCG_PLAN_TREE_FANIN_FANOUT;
        }
    }

    if (flags == 0 /* ucg_predefined_modifiers[UCG_PRIMITIVE_ALLTOALL] */) {
        return UCG_PLAN_BRUCK;
    }

    if (flags & UCG_GROUP_COLLECTIVE_MODIFIER_ALLGATHER) {
        if (ucg_algo.bruck) {
            return UCG_PLAN_BRUCK;
        } else {
            return UCG_PLAN_RECURSIVE;
        }
    }

    return UCG_PLAN_TREE_FANIN_FANOUT;
}

/*
void ucg_builtin_swap_net_recv(char *netdata, size_t length, size_t offset,
                               ucg_builtin_request_t *req)
{
    ucg_builtin_op_step_t *step = req->step;
    ucp_dt_generic_t *gen_dt = req->op->recv_dt;
    void *state_pack = step->bcopy.pack_state_recv.dt.generic.state;
    void *state_unpack = step->bcopy.unpack_state.dt.generic.state;
    char *recv_buffer = (char *)step->recv_buffer;
    char *tmp_buffer = NULL;

    ucs_debug("swap netdata:%p length:%lu and recv_buffer:%p offset:%lu",
              netdata, length, recv_buffer, offset);

    tmp_buffer = (char *)ucs_malloc(length, "temp swap buffer");
    if (tmp_buffer == NULL) {
        ucs_fatal("no memory for malloc, length:%lu", length);
    }

    memcpy(tmp_buffer, netdata, length);
    if (gen_dt != NULL) {
        gen_dt->ops.pack(state_pack, offset, netdata, length);
        gen_dt->ops.unpack(state_unpack, offset, tmp_buffer, length);
    } else {
        memcpy(netdata, recv_buffer + offset, length);
        memcpy(recv_buffer + offset, tmp_buffer, length);
    }

    free(tmp_buffer);
}
*/

UCS_PROFILE_FUNC(ucs_status_t, ucg_builtin_am_handler,
                 (ctx, data, length, am_flags),
                 void *ctx, void *data, size_t length, unsigned am_flags)
{
    ucg_builtin_ctx_t *bctx      = ctx;
    ucg_builtin_header_t* header = data;
    ucs_assert(length >= sizeof(header));
    ucs_assert(header != 0); /* since group_id >= UCG_GROUP_FIRST_GROUP_ID */

    /* Find the Group context, based on the ID received in the header */
    ucg_group_id_t group_id = header->group_id;
    ucs_assert(group_id != 0);
    ucs_assert(bctx->group_by_id.size > group_id);

    ucs_status_t status;
    ucp_recv_desc_t *rdesc;
    ucs_ptr_array_t *msg_array;
    ucg_builtin_comp_slot_t *slot;

    ucg_builtin_group_ctx_t *gctx = (void*)bctx->group_by_id.start[group_id];
    if (ucs_likely(!__ucs_ptr_array_is_free(gctx))) {
        /* Find the slot to be used, based on the ID received in the header */
        ucg_coll_id_t coll_id = header->msg.coll_id;
        slot = &gctx->slots[coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS];
        ucs_assert((slot->req.expecting.coll_id != coll_id) ||
                   (slot->req.expecting.step_idx <= header->msg.step_idx));

        /* Consume the message if it fits the current collective and step index */
        if (ucs_likely(header->msg.local_id == slot->req.expecting.local_id)) {




            // TODO: (alex) I want to move this to a different location in the code
            /*
            if ((slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) &&
                (slot->req.step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND)) {
                // Zcopy recv before sending finished, store msg
                if (slot->req.pending > slot->req.step->fragments_recv) {
                    if (++slot->req.step->zcopy.num_store > slot->req.step->fragments_recv) {
                        // recv msg from step - step index = step now index + 256, store msg without count
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

            size_t real_length = length - sizeof(ucg_builtin_header_t);
            char *header_tmp = (char *)header;
            char *recv_buffer_tmp = (char *)slot->req.step->recv_buffer;

            if (slot->req.step->phase->is_swap) {
                ucg_builtin_swap_net_recv(data + sizeof(ucg_builtin_header_t),
                                          length - sizeof(ucg_builtin_header_t),
                                          header->remote_offset, &slot->req);
            } */






            /* Make sure the packet indeed belongs to the collective currently on */
            data    = header + 1;
            length -= sizeof(ucg_builtin_header_t);

            ucs_trace_req("ucg_builtin_am_handler CB: coll_id %u step_idx %u pending %u",
                          header->msg.coll_id, header->msg.step_idx, slot->req.pending);

            ucg_builtin_step_recv_cb(&slot->req, header->remote_offset, data, length);

            return UCS_OK;
        }

        msg_array = &slot->messages;
        ucs_trace_req("ucg_builtin_am_handler STORE: group_id %u "
                      "coll_id %u expected_id %u step_idx %u expected_idx %u",
                      header->group_id, header->msg.coll_id, slot->req.expecting.coll_id,
                      header->msg.step_idx, slot->req.expecting.step_idx);

#ifdef HAVE_UCT_COLLECTIVES
        /* In case of a stride - the stored length is actually longer */
        if (am_flags & UCT_CB_PARAM_FLAG_STRIDE) {
            length = sizeof(ucg_builtin_header_t) +
                    (length - sizeof(ucg_builtin_header_t)) *
                    (gctx->group_params->member_count - 1);
        }
#endif
    } else {
        if (!ucs_ptr_array_lookup(&bctx->unexpected, group_id, msg_array)) {
            msg_array = UCS_ALLOC_CHECK(sizeof(*msg_array), "unexpected group");
            ucs_ptr_array_init(msg_array, "unexpected group messages");
            ucs_ptr_array_set(&bctx->unexpected, group_id, msg_array);
        }

        /*
         * Note: because this message arrived before the corresponding local
         *       group has been created, we don't know what is the number
         *       of processes in the group - so we don't know how many items
         *       to expect in this vector (we only know the size of one item).
         *       TODO: resolve this corner case... use elem->pending?
         */
#ifdef HAVE_UCT_COLLECTIVES
        ucs_assert_always((am_flags & UCT_CB_PARAM_FLAG_STRIDE) == 0);
#endif
    }

    uint64_t data_offset = (ucs_unlikely(am_flags & UCT_CB_PARAM_FLAG_SHIFTED)) ?
            ((uint64_t*)header)[-1] : 0;

    status = ucp_recv_desc_init(bctx->worker, data, length, (int)data_offset,
                                am_flags, 0, 0, 0, &rdesc);

    /* Store the message (if the relevant step has not been reached) */
    if (ucs_likely(!UCS_STATUS_IS_ERR(status))) {
        (void) ucs_ptr_array_insert(msg_array, rdesc);
    }

    return status;
}

static void ucg_builtin_msg_dump(void *arg, uct_am_trace_type_t type,
                                 uint8_t id, const void *data, size_t length,
                                 char *buffer,
                          size_t max)
{
    const ucg_builtin_header_t *header = (const ucg_builtin_header_t*)data;
    snprintf(buffer, max, "COLLECTIVE [coll_id %u step_idx %u offset %lu length %lu]",
             (unsigned)header->msg.coll_id, (unsigned)header->msg.step_idx,
             (uint64_t)header->remote_offset, length - sizeof(*header));
}

static ucs_status_t ucg_builtin_query(ucg_plan_desc_t *descs,
                                      unsigned *desc_cnt_p)
{
    /* Return a simple description of the "Builtin" module */
    ucs_status_t status = ucg_plan_single(&ucg_builtin_component,
                                          descs, desc_cnt_p);

    if (descs) {
        descs->modifiers_supported = (unsigned)-1; /* supports ANY collective */
        descs->flags = 0;
    }

    return status;
}

static ucs_status_t ucg_builtin_init_plan_config(ucg_builtin_config_t *config)
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

    return UCS_OK;
}

static ucg_group_member_index_t
ucg_builtin_calc_host_proc_cnt(const ucg_group_params_t *group_params)
{
    ucg_group_member_index_t index, count = 0;

    for (index = 0; index < group_params->member_count; index++) {
        if (group_params->distance[index] <= UCG_GROUP_MEMBER_DISTANCE_HOST) {
            count++;
        }
    }

    return count;
}

void ucg_builtin_req_enqueue_resend(ucg_builtin_group_ctx_t *gctx,
                                    ucg_builtin_request_t *req)
{
    UCS_ASYNC_BLOCK(&gctx->worker->async);

    ucs_queue_push(&gctx->resend_head, &req->resend_queue);

    UCS_ASYNC_UNBLOCK(&gctx->worker->async);
}

static UCS_F_ALWAYS_INLINE
void ucg_builtin_async_resend(ucg_builtin_group_ctx_t *gctx)
{
    ucs_queue_head_t resent;
    ucg_builtin_request_t *req;

    ucs_queue_head_init(&resent);
    ucs_queue_splice(&resent, &gctx->resend_head);

    ucs_queue_for_each_extract(req, &resent, resend_queue, 1==1) {
        (void) ucg_builtin_step_execute(req, req->step->am_header);
    }
}

static void ucg_builtin_async_check(int id, ucs_event_set_types_t events, void *arg)
{

    ucg_builtin_group_ctx_t *gctx = arg;

    if (ucs_likely(ucs_queue_is_empty(&gctx->resend_head))) {
        return;
    }

    ucg_builtin_async_resend(gctx);
}

#if ENABLE_FAULT_TOLERANCE
static void ucg_builtin_async_ft(int id, ucs_event_set_types_t events, void *arg)
{
    if ((status == UCS_INPROGRESS) &&
            !(req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING)) {
        ucg_builtin_plan_phase_t *phase = req->step->phase;
        if (phase->ep_cnt == 1) {
            ucg_ft_start(group, phase->indexes[0], phase->single_ep, &phase->handles[0]);
        } else {
            unsigned peer_idx = 0;
            while (peer_idx < phase->ep_cnt) {
                ucg_ft_start(group, phase->indexes[peer_idx],
                        phase->multi_eps[peer_idx], &phase->handles[peer_idx]);
                peer_idx++;
            }
        }

        req->step->flags |= UCG_BUILTIN_OP_STEP_FLAG_FT_ONGOING;
    }
}
#endif

static unsigned ucg_builtin_op_progress(ucg_coll_h op)
{
    ucg_builtin_op_t *builtin_op        = (ucg_builtin_op_t*)op;
    ucg_builtin_op_step_t *current_step = *(builtin_op->current);
    uct_iface_progress_func_t progress  = current_step->uct_progress;

    unsigned ret = progress(current_step->uct_iface);
    if (ucs_likely(ret > 0)) {
        return ret;
    }

    ucg_builtin_group_ctx_t *gctx = builtin_op->gctx;
    if (!ucs_queue_is_empty(&gctx->resend_head)) {
        UCS_ASYNC_BLOCK(&gctx->worker->async);

        ucg_builtin_async_resend(builtin_op->gctx);

        UCS_ASYNC_UNBLOCK(&gctx->worker->async);
        return 1;
    }

    return 0;
}

static ucs_status_t ucg_builtin_init(ucg_plan_ctx_h pctx,
                                     ucg_plan_params_t *params,
                                     ucg_plan_config_t *config)
{
    ucg_builtin_ctx_t *bctx = pctx;
    bctx->am_id             = *params->am_id;
    ++*params->am_id;

#if ENABLE_FAULT_TOLERANCE
    if (ucg_params.fault.mode > UCG_FAULT_IS_FATAL) {
        return UCS_ERR_UNSUPPORTED;
    }
#endif

    ucs_status_t status = ucs_config_parser_clone_opts(config, &bctx->config,
                                                       ucg_builtin_config_table);
    if (status != UCS_OK) {
        return status;
    }

    ucs_ptr_array_init(&bctx->group_by_id, "builtin_group_table");
    ucs_ptr_array_init(&bctx->unexpected, "builtin_unexpected_table");

    return ucg_builtin_init_plan_config(&bctx->config) &&
            ucg_context_set_am_handler(pctx, bctx->am_id,
                                       ucg_builtin_am_handler,
                                       ucg_builtin_msg_dump);
}

static void ucg_builtin_finalize(ucg_plan_ctx_h pctx)
{
    ucg_builtin_ctx_t *bctx = pctx;
    ucs_ptr_array_cleanup(&bctx->group_by_id);
}

static ucs_status_t ucg_builtin_create(ucg_plan_ctx_h pctx,
                                       ucg_group_ctx_h ctx,
                                       ucg_group_h group,
                                       const ucg_group_params_t *params)
{
    ucs_status_t status;

    ucp_worker_h worker        = ucg_plan_get_group_worker(group);
    ucs_async_context_t *async = &worker->async;

    if (!ucs_test_all_flags(params->field_mask, UCG_BUILTIN_PARAM_MASK)) {
        ucs_error("UCG Planner \"Builtin\" is missing some group parameters");
        return UCS_ERR_INVALID_PARAM;
    }

    /* Fill in the information in the per-group context */
    ucg_builtin_ctx_t* bctx       = pctx;
    ucg_builtin_group_ctx_t *gctx = ctx;
    ucg_group_id_t group_id       = params->id;
    gctx->group_id                = group_id;
    gctx->group                   = group;
    gctx->worker                  = worker;
    gctx->group_params            = params;
    gctx->host_proc_cnt           = ucg_builtin_calc_host_proc_cnt(params);
    gctx->bctx                    = bctx;
    bctx->worker                  = worker;

    ucs_list_head_init(&gctx->plan_head);
    ucs_queue_head_init(&gctx->resend_head);
    ucs_ptr_array_set(&bctx->group_by_id, group_id, gctx);
    ucs_assert_always(((uintptr_t)gctx % UCS_SYS_CACHE_LINE_SIZE) == 0);

    ucs_time_t interval = ucs_time_from_sec(bctx->config.resend_timer_tick);
    status = ucg_context_set_async_timer(async, ucg_builtin_async_check, gctx,
                                         interval, &gctx->timer_id);
    if (status != UCS_OK) {
        return status;
    }

#if ENABLE_FAULT_TOLERANCE
    if (ucg_params.fault.mode > UCG_FAULT_IS_FATAL) {
        interval = ucs_time_from_sec(bctx->config.ft_timer_tick);
        status = ucg_context_set_async_timer(async, ucg_builtin_async_ft, gctx,
                                             interval, &gctx->ft_timer_id);
        if (status != UCS_OK) {
            return status;
        }
    }
#endif

    /* Initialize collective operation slots, incl. already pending messages */
    unsigned i;
    ucs_ptr_array_t *by_group_id, *by_slot_id;
    int is_msg_pending = ucs_ptr_array_lookup(&bctx->unexpected,
                                              group_id,
                                              by_group_id);

    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        ucg_builtin_comp_slot_t *slot = &gctx->slots[i];

        if (is_msg_pending &&
            (ucs_ptr_array_lookup(by_group_id, i, by_slot_id))) {
            ucs_ptr_array_remove(by_group_id, i);
            memcpy(&slot->messages, by_slot_id, sizeof(*by_slot_id));
            ucs_free(by_slot_id);
        } else {
            ucs_ptr_array_init(&slot->messages, "builtin messages");
        }

        slot->req.expecting.local_id = 0;
        slot->req.am_id              = bctx->am_id;
    }

    if (is_msg_pending) {
        ucs_ptr_array_remove(&bctx->unexpected, group_id);
        ucs_ptr_array_cleanup(by_group_id);
        ucs_free(by_group_id);
    }

    return UCS_OK;
}

static void ucg_builtin_clean_phases(ucg_builtin_plan_t *plan)
{
    int i;
    for (i = 0; i < plan->phs_cnt; i++) {
        if (plan->phss[i].recv_cache_buffer) {
            ucs_free(plan->phss[i].recv_cache_buffer);
        }

        if (plan->phss[i].ucp_eps) {
            ucs_free(plan->phss[i].ucp_eps);
        }
    }

#if ENABLE_DEBUG_DATA
    ucs_free((void **)&plan->phss[0].indexes);
#endif
}

static void ucg_builtin_destroy_plan(ucg_builtin_plan_t *plan)
{
    ucs_list_link_t *op_head = &plan->super.op_head;
    while (!ucs_list_is_empty(op_head)) {
        ucg_builtin_op_discard(ucs_list_extract_head(op_head, ucg_op_t, list));
    }

#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
    ucg_step_idx_t i;
    for (i = 0; i < plan->phs_cnt; i++) {
        ucs_free(plan->phss[i].indexes);
    }
#endif

#if ENABLE_MT
    ucs_recursive_spinlock_destroy(&plan->super.lock);
#endif

    ucs_list_del(&plan->list);
    ucg_builtin_clean_phases(plan);
    ucs_mpool_cleanup(&plan->op_mp, 1);
    ucs_free(plan);
}

static void ucg_builtin_destroy(ucg_group_ctx_h ctx)
{
    unsigned i, j;
    ucg_builtin_group_ctx_t *gctx = ctx;

    ucg_context_unset_async_timer(&gctx->worker->async, gctx->timer_id);

    /* Cleanup left-over messages and outstanding operations */
    for (i = 0; i < UCG_BUILTIN_MAX_CONCURRENT_OPS; i++) {
        ucg_builtin_comp_slot_t *slot = &gctx->slots[i];
        if (slot->req.expecting.local_id != 0) {
            ucs_warn("Collective operation #%u has been left incomplete (Group #%u)",
                    gctx->slots[i].req.expecting.coll_id, gctx->group_id);
        }

        ucp_recv_desc_t *rdesc;
        ucs_ptr_array_for_each(rdesc, j, &slot->messages) {
            ucs_warn("Collective operation #%u still has a pending message for"
                     "step #%u (Group #%u)",
                     ((ucg_builtin_header_t*)(rdesc + 1))->msg.coll_id,
                     ((ucg_builtin_header_t*)(rdesc + 1))->msg.step_idx,
                     ((ucg_builtin_header_t*)(rdesc + 1))->group_id);
#ifdef HAVE_UCP_EXTENSIONS
            /* No UCT interface information, we can't release if it's shared */
            if (!(rdesc->flags & UCP_RECV_DESC_FLAG_UCT_DESC_SHARED))
#endif
            ucp_recv_desc_release(rdesc CONDITIONAL_NULL);
            ucs_ptr_array_remove(&slot->messages, j);
        }
        ucs_ptr_array_cleanup(&slot->messages);
    }

    /* Cleanup plans created for this group */
    while (!ucs_list_is_empty(&gctx->plan_head)) {
        ucg_builtin_destroy_plan(ucs_list_extract_head(&gctx->plan_head,
                                                       ucg_builtin_plan_t,
                                                       list));
    }

    /* Remove the group from the global storage array */
    ucg_builtin_ctx_t *bctx = gctx->bctx;
    ucs_ptr_array_remove(&bctx->group_by_id, gctx->group_id);

    /* Note: gctx is freed as part of the group object itself */
}

void ucg_builtin_release_comp_desc(ucg_builtin_comp_desc_t *desc)
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
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE, &ucg_algo);
    } else {
        /* Ring */
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RING, &ucg_algo);
    }
}

#include <ucg/api/ucg_mpi.h> // TODO: (alex) try to avoid this include here...

void ucg_builtin_plan_decision_in_unsupport_allreduce_case(const size_t msg_size,
                                                           const ucg_group_params_t *group_params,
                                                           const enum ucg_collective_modifiers modifiers,
                                                           const ucg_collective_params_t *coll_params)
{
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
        if (coll_params->recv.op && !ucg_global_params.reduce_op.is_commutative_f(coll_params->recv.op)) {
            /* Ring */
            ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RING, &ucg_algo);
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
        ucg_builtin_bcast_algo_switch(UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE, &ucg_algo);
    }
}

void ucg_builtin_plan_decision_in_unsupport_barrier_case(const size_t msg_size,
                                                         const ucg_group_params_t *group_params,
                                                         const enum ucg_collective_modifiers modifiers,
                                                         const ucg_collective_params_t *coll_params)
{
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        /* Node-aware Recursive (DEFAULT) */
        ucg_builtin_barrier_algo_switch(UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE, &ucg_algo);
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
    ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_algo);
    ucs_debug("non-commutative operation, select recurisive");
}

void ucg_builtin_plan_decision_in_noncommutative_largedata_case_ring(const size_t msg_size, enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision)
{
    /* Ring */
    if (allreduce_algo_decision != NULL) {
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_RING;
    }
    ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RING, &ucg_algo);
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
    ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_algo);
    ucs_debug("non-commutative operation with more than one send count, select recurisive");
}

// TODO: (alex) remove this later...
extern ucs_status_t ucg_builtin_convert_datatype(void *param_datatype, ucp_datatype_t *ucp_datatype);

void ucg_builtin_allreduce_decision_fixed(const size_t msg_size,
                                          const ucg_group_params_t *group_params,
                                          const ucg_collective_params_t *coll_params,
                                          const unsigned large_datatype_threshold,
                                          const int is_unbalanced_ppn,
                                          enum ucg_builtin_allreduce_algorithm *allreduce_algo_decision)
{
    ucp_datatype_t send_dt;
    ucg_global_params.datatype.convert(coll_params->send.dtype, &send_dt); //TODO: check success
    unsigned is_large_datatype = (ucp_dt_length(send_dt, 1, NULL, NULL) >
                                  large_datatype_threshold);
    unsigned is_non_commutative = coll_params->send.op
        && !ucg_global_params.reduce_op.is_commutative_f(coll_params->send.op);
    if (is_large_datatype || is_non_commutative) {
        ucg_builtin_plan_decision_in_noncommutative_largedata_case(msg_size, allreduce_algo_decision);
    } else if (msg_size >= UCG_GROUP_MED_MSG_SIZE) {
        /* Ring */
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_RING;
        ucg_builtin_allreduce_algo_switch(*allreduce_algo_decision, &ucg_algo);
    } else if (is_unbalanced_ppn) {
        /* Node-aware Recursive */
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE;
        ucg_builtin_allreduce_algo_switch(*allreduce_algo_decision, &ucg_algo);
    } else {
        /* Node-aware Kinomial tree (DEFAULT) */
        *allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_KMTREE;
        ucg_builtin_allreduce_algo_switch(*allreduce_algo_decision, &ucg_algo);
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
        ucg_builtin_bcast_algo_switch(*bcast_algo_decision, &ucg_algo);
    }
    if (modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_BARRIER]) {
        /* Node-aware Recursive (DEFAULT) */
        if (is_unbalanced_ppn) {
            /* Node-aware Recursive */
            *barrier_algo_decision = UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE;
            ucg_builtin_barrier_algo_switch(*barrier_algo_decision, &ucg_algo);
        } else {
            /* Node-aware Kinomial tree (DEFAULT) */
            *barrier_algo_decision = UCG_ALGORITHM_BARRIER_NODE_AWARE_KMTREE;
            ucg_builtin_barrier_algo_switch(*barrier_algo_decision, &ucg_algo);
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

ucs_status_t ucg_builtin_init_algo(struct ucg_builtin_algorithm *algo)
{
    ucg_builtin_fillin_algo(algo, 1, 0, 0, 1, 0, 0);
    algo->bruck        = 1,
    algo->topo_level   = UCG_GROUP_HIERARCHY_LEVEL_NODE,
    algo->pipeline     = 0;
    algo->feature_flag = UCG_ALGORITHM_SUPPORT_COMMON_FEATURE;
    return UCS_OK;
}

ucs_status_t ucg_builtin_bcast_algo_switch(const enum ucg_builtin_bcast_algorithm bcast_algo_decision,
                                           struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
    algo->bruck = 1;
    switch (bcast_algo_decision) {
        case UCG_ALGORITHM_BCAST_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            break;
        case UCG_ALGORITHM_BCAST_NODE_AWARE_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
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
    return UCS_OK;
}

ucs_status_t ucg_builtin_barrier_algo_switch(const enum ucg_builtin_barrier_algorithm barrier_algo_decision,
                                             struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->bruck = 1;
    switch (barrier_algo_decision) {
        case UCG_ALGORITHM_BARRIER_RECURSIVE:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 1, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_BARRIER_NODE_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
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
    return UCS_OK;
}

ucs_status_t ucg_builtin_allreduce_algo_switch(const enum ucg_builtin_allreduce_algorithm allreduce_algo_decision,
                                               struct ucg_builtin_algorithm *algo)
{
    algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
    algo->bruck = 1;
    switch (allreduce_algo_decision) {
        case UCG_ALGORITHM_ALLREDUCE_RECURSIVE:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 1, 0, 0);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_ALLREDUCE_RARE_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_NODE_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_NODE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
            break;
        case UCG_ALGORITHM_ALLREDUCE_SOCKET_AWARE_RECURSIVE_AND_BMTREE:
            ucg_builtin_fillin_algo(algo, 1, 0, 0, 0, 1, 0);
            algo->topo_level = UCG_GROUP_HIERARCHY_LEVEL_SOCKET;
            break;
        case UCG_ALGORITHM_ALLREDUCE_RING:
            ucg_builtin_fillin_algo(algo, 0, 0, 0, 0, 0, 1);
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_RANK_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_ALLREDUCE_RARE_FEATURE;
            algo->feature_flag |= UCG_ALGORITHM_SUPPORT_BIND_TO_NONE;
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
    return UCS_OK;
}

void ucg_builtin_check_algorithm_param_size(ucg_builtin_config_t *config)
{
    if (((int)config->allreduce_algorithm >= UCG_ALGORITHM_ALLREDUCE_LAST) || ((int)config->allreduce_algorithm < UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION)) {
        ucs_warn("Param UCX_BUILTIN_ALLREDUCE_ALGORITHM=%d is invalid parameter, switch to default algorithm.", (int)config->allreduce_algorithm);
    }
    if (((int)config->bcast_algorithm >= UCG_ALGORITHM_BCAST_LAST) || ((int)config->bcast_algorithm < UCG_ALGORITHM_BCAST_AUTO_DECISION)) {
        ucs_warn("Param UCX_BUILTIN_BCAST_ALGORITHM=%d is invalid parameter, switch to default algorithm.", (int)config->bcast_algorithm);
    }
    if (((int)config->barrier_algorithm >= UCG_ALGORITHM_BARRIER_LAST) || ((int)config->barrier_algorithm < UCG_ALGORITHM_BARRIER_AUTO_DECISION)) {
        ucs_warn("Param UCX_BUILTIN_BARRIER_ALGORITHM=%d is invalid parameter, switch to default algorithm.", (int)config->barrier_algorithm);
    }
}

void ucg_builtin_check_algorithm_param_type(ucg_builtin_config_t *config)
{
    if ((config->allreduce_algorithm - (int)config->allreduce_algorithm) != 0) {
        ucs_warn("Param UCX_BUILTIN_ALLREDUCE_ALGORITHM=%lf is not unsigned integer, switch to unsigned integer '%d'.", config->allreduce_algorithm, (int)config->allreduce_algorithm);
    }
    if ((config->bcast_algorithm - (int)config->bcast_algorithm) != 0) {
        ucs_warn("Param UCX_BUILTIN_BCAST_ALGORITHM=%lf is not unsigned integer, switch to unsigned integer '%d'.", config->bcast_algorithm, (int)config->bcast_algorithm);
    }
    if ((config->barrier_algorithm - (int)config->barrier_algorithm) != 0) {
        ucs_warn("Param UCX_BUILTIN_BARRIER_ALGORITHM=%lf is not unsigned integer, switch to unsigned integer '%d'.", config->barrier_algorithm, (int)config->barrier_algorithm);
    }
}

enum choose_ops_mask ucg_builtin_plan_choose_ops(ucg_builtin_config_t *config,
        enum ucg_collective_modifiers ops_type_choose)
{
    ucg_builtin_check_algorithm_param_type(config);
    ucg_builtin_check_algorithm_param_size(config);

    enum ucg_builtin_bcast_algorithm bcast_algo_decision = (enum ucg_builtin_bcast_algorithm)config->bcast_algorithm;
    enum ucg_builtin_allreduce_algorithm allreduce_algo_decision = (enum ucg_builtin_allreduce_algorithm)
            config->allreduce_algorithm;
    enum ucg_builtin_barrier_algorithm barrier_algo_decision = (enum ucg_builtin_barrier_algorithm)
            config->barrier_algorithm;
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

void ucg_builtin_check_continuous_number_by_sort(ucg_group_member_index_t *array,
                                                 unsigned array_len,
                                                 unsigned *discont_flag)
{
    ucg_group_member_index_t member_idx;
    unsigned idx, idx2;
    /* bubble sort */
    for (idx = 0; idx < array_len - 1; idx++) {
        for (idx2 = 0; idx2 < array_len - 1 - idx; idx2++) {
            if (array[idx2] > array[idx2 + 1]) {
                member_idx =  array[idx2 + 1];
                array[idx2 + 1] = array[idx2];
                array[idx2] = member_idx;
            }
        }
    }
    /* discontinous or not */
    for (idx = 0; idx < array_len - 1; idx++) {
        if (array[idx + 1] - array[idx] != 1) {
            *discont_flag = 1;
            break;
        }
    }
}

static void ucg_builtin_prepare_rank_same_unit(const ucg_group_params_t *group_params,
                                               enum ucg_group_member_distance domain_distance,
                                               ucg_group_member_index_t *rank_same_unit)
{
    unsigned idx, member_idx;
    enum ucg_group_member_distance next_distance;
    for (idx = 0, member_idx = 0; member_idx < group_params->member_count; member_idx++) {
        next_distance = group_params->distance[member_idx];
        if (ucs_likely(next_distance <= domain_distance)) {
            rank_same_unit[idx++] = member_idx;
        }
    }
}

ucs_status_t ucg_builtin_check_continuous_number_no_distance_table(const ucg_group_params_t *group_params,
                                                             enum ucg_group_member_distance domain_distance,
                                                             unsigned *discont_flag)
{
    unsigned ppx = ucg_builtin_calculate_ppx(group_params, domain_distance);

    /* store rank number in same unit */
    size_t alloc_size = ppx * sizeof(ucg_group_member_index_t);
    ucg_group_member_index_t *rank_same_unit = (ucg_group_member_index_t*)UCS_ALLOC_CHECK(alloc_size, "rank number");
    memset(rank_same_unit, 0, alloc_size);
    ucg_builtin_prepare_rank_same_unit(group_params, domain_distance, rank_same_unit);

    ucg_builtin_check_continuous_number_by_sort(rank_same_unit, ppx, discont_flag);
    ucs_free(rank_same_unit);
    return UCS_OK;
}

ucs_status_t ucg_builtin_check_continuous_number(const ucg_group_params_t *group_params,
                                                 enum ucg_group_member_distance domain_distance,
                                                 unsigned *discont_flag)
{
    if ((ucg_global_params.job_info.distance_table == NULL) ||
        (group_params->rank_map == NULL)) {
        return ucg_builtin_check_continuous_number_no_distance_table(group_params, domain_distance, discont_flag);
    }

    char domain_distance_ch = (char)domain_distance;
    /* Check the topo distance in each line and find all ranks in the same node
       Make sure the ranks in the same node is continuous. */
    for (unsigned i = 0; i < group_params->member_count; i++) {
        ucg_group_member_index_t global_i = group_params->rank_map[i];
        int last_same_unit_rank = -1;
        for (unsigned j = 0; j < group_params->member_count; j++) {
            ucg_group_member_index_t global_j = group_params->rank_map[j];
            if (ucg_global_params.job_info.distance_table[global_i][global_j] > domain_distance_ch) {
                continue;
            }

            if (last_same_unit_rank != -1 && j - last_same_unit_rank != 1) {
                *discont_flag = 1;
                return UCS_OK;
            }
            last_same_unit_rank = j;
        }
    }
    *discont_flag = 0;
    return UCS_OK;
}

ucs_status_t choose_distance_from_topo_aware_level(enum ucg_group_member_distance *domain_distance)
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
    return UCS_OK;
}

void ucg_builtin_non_commutative_operation(const ucg_group_params_t *group_params, const ucg_collective_params_t *coll_params, struct ucg_builtin_algorithm *algo, const size_t msg_size)
{
    if (coll_params->recv.op && !ucg_global_params.reduce_op.is_commutative_f(coll_params->recv.op) &&
        (algo->feature_flag & UCG_ALGORITHM_SUPPORT_NON_COMMUTATIVE_OPS)) {
        if (coll_params->send.count > 1) {
            ucg_builtin_plan_decision_in_noncommutative_many_counts_case();
            ucs_warn("Current algorithm does not support many counts non-commutative operation, and switch to Recursive doubling which may have unexpected performance");
        } else {
            ucg_builtin_plan_decision_in_noncommutative_largedata_case(msg_size, NULL);
            ucs_warn("Current algorithm does not support non commutative operation, and switch to Recursive doubling or Ring Algorithm which may have unexpected performance");
        }
    }
}

int ucg_is_noncontig_allreduce(const ucg_group_params_t *group_params,
                               const ucg_collective_params_t *coll_params)
{
    ucp_datatype_t ucp_datatype;

    if ((coll_params->send.type.modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) &&
        (coll_params->send.count > 0)) {
        ucg_global_params.datatype.convert(coll_params->send.dtype, &ucp_datatype);
        if (!UCP_DT_IS_CONTIG(ucp_datatype)) {
            ucs_debug("allreduce non-contiguous datatype");
            return 1;
        }
    }

    return 0;
}

#define UCT_MIN_SHORT_ONE_LEN 80
#define UCT_MIN_BCOPY_ONE_LEN 1000
int ucg_is_segmented_allreduce(const ucg_collective_params_t *coll_params)
{
    ucp_datatype_t send_dt;
    ucs_status_t status = ucg_builtin_convert_datatype(coll_params->send.dtype, &send_dt);
    ucs_assert(status == UCS_OK);
    size_t dt_len = ucp_dt_length(send_dt, coll_params->send.count, NULL, NULL);
    int count = coll_params->send.count;

    if (coll_params->send.type.modifiers == ucg_predefined_modifiers[UCG_PRIMITIVE_ALLREDUCE]) {
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
ucs_status_t ucg_builtin_change_unsupport_algo(struct ucg_builtin_algorithm *algo,
                                               const ucg_group_params_t *group_params,
                                               const size_t msg_size,
                                               const ucg_collective_params_t *coll_params,
                                               const enum ucg_collective_modifiers ops_type_choose,
                                               enum choose_ops_mask ops_choose,
                                               ucg_builtin_config_t *config)
{
    ucs_status_t status;

    /* Currently, only algorithm 1 supports non-contiguous datatype for allreduce */
    if (ucg_is_noncontig_allreduce(group_params, coll_params)) {
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_algo);
        ucs_info("allreduce non-contiguous datatype, select algo%d:recursive", UCG_ALGORITHM_ALLREDUCE_RECURSIVE);
        return UCS_OK;
    }

    /* Special Case 1 : bind-to none */
    if (!(algo->feature_flag & UCG_ALGORITHM_SUPPORT_BIND_TO_NONE) &&
        (ucg_global_params.job_info.bind_to == UCG_GROUP_MEMBER_DISTANCE_NONE)) {
        ucg_builtin_plan_decision_in_unsupport_case(msg_size, group_params, ops_type_choose, coll_params);
        ucs_warn("Current algorithm don't support bind-to none case, switch to default algorithm");
    }

    /* Special Case 2 : unbalance ppn */
    unsigned is_ppn_unbalance = 0;
    status = ucg_builtin_check_ppn(group_params, &is_ppn_unbalance);
    if (status != UCS_OK) {
        return status;
    }

    if (is_ppn_unbalance && (!(algo->feature_flag & UCG_ALGORITHM_SUPPORT_UNBALANCE_PPN))) {
        ucg_builtin_plan_decision_in_unsupport_case(msg_size, group_params, ops_type_choose, coll_params);
        ucs_warn("Current algorithm don't support ppn unbalance case, switch to default algorithm");
    }

    /* Special Case 3 : discontinuous rank */
    unsigned is_discontinuous_rank = 0;
    enum ucg_group_member_distance domain_distance = UCG_GROUP_MEMBER_DISTANCE_HOST;
    status = choose_distance_from_topo_aware_level(&domain_distance);
    if (status != UCS_OK) {
        return status;
    }
    status = ucg_builtin_check_continuous_number(group_params, domain_distance, &is_discontinuous_rank);
    if (status != UCS_OK) {
        return status;
    }

    if (is_discontinuous_rank && (!(algo->feature_flag & UCG_ALGORITHM_SUPPORT_DISCONTINOUS_RANK))) {
        ucg_builtin_plan_decision_in_unsupport_case(msg_size, group_params, ops_type_choose, coll_params);
        ucs_warn("Current algorithm demand rank number is continous. Switch default algorithm whose performance may be not the best");
    }


    if (ops_choose == OPS_ALLREDUCE) {
        /* Special Case 4 : non-commutative operation */
        ucg_builtin_non_commutative_operation(group_params, coll_params, algo, msg_size);

        /* Special Case 5 : large datatype */
        ucp_datatype_t send_dt;
        status = ucg_builtin_convert_datatype(coll_params->send.dtype, &send_dt);
        ucs_assert(status == UCS_OK);
        unsigned dt_len = ucp_dt_length(send_dt, coll_params->send.count, NULL, NULL);
        if (dt_len > config->large_datatype_threshold &&
            !(algo->feature_flag & UCG_ALGORITHM_SUPPORT_LARGE_DATATYPE)) {
                ucg_builtin_plan_decision_in_noncommutative_largedata_case(msg_size, NULL);
                ucs_warn("Current algorithm does not support large datatype, and switch to Recursive doubling or Ring Algorithm which may have unexpected performance");
        }
    }

    /* The allreduce result is wrong when phase->segmented=1 and using ring algorithm, must avoid it */
    if (ucg_algo.ring && ucg_is_segmented_allreduce(coll_params)) {
        ucg_builtin_allreduce_algo_switch(UCG_ALGORITHM_ALLREDUCE_RECURSIVE, &ucg_algo);
        ucs_info("ring algorithm does not support segmented phase, select recursive algorithm");
        return UCS_OK;
    }

    return status;
}

void ucg_builtin_log_algo()
{
    ucs_info("bmtree %u kmtree %u kmtree_intra %u recur %u bruck %u topo %u level %u ring %u pipe %u",
             ucg_algo.bmtree, ucg_algo.kmtree, ucg_algo.kmtree_intra, ucg_algo.recursive, ucg_algo.bruck,
             ucg_algo.topo, (unsigned)ucg_algo.topo_level, ucg_algo.ring, ucg_algo.pipeline);
}

ucs_status_t ucg_builtin_algorithm_decision(const ucg_collective_type_t *coll_type,
                                            const size_t msg_size,
                                            const ucg_group_params_t *group_params,
                                            const ucg_collective_params_t *coll_params,
                                            ucg_builtin_config_t *config)
{
    ucg_collective_type_t *coll = (ucg_collective_type_t *)coll_type;
    enum ucg_collective_modifiers ops_type_choose = coll->modifiers;
    enum ucg_builtin_bcast_algorithm bcast_algo_decision = (enum ucg_builtin_bcast_algorithm)config->bcast_algorithm;
    enum ucg_builtin_allreduce_algorithm allreduce_algo_decision = (enum ucg_builtin_allreduce_algorithm)
            config->allreduce_algorithm;
    enum ucg_builtin_barrier_algorithm barrier_algo_decision = (enum ucg_builtin_barrier_algorithm)
            config->barrier_algorithm;

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
            ucg_builtin_bcast_algo_switch(bcast_algo_decision, &ucg_algo);
            allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION;
            barrier_algo_decision = UCG_ALGORITHM_BARRIER_AUTO_DECISION;
            break;

        case OPS_ALLREDUCE:
            ucg_builtin_allreduce_algo_switch(allreduce_algo_decision, &ucg_algo);
            bcast_algo_decision = UCG_ALGORITHM_BCAST_AUTO_DECISION;
            barrier_algo_decision = UCG_ALGORITHM_BARRIER_AUTO_DECISION;
            break;

        case OPS_BARRIER:
            ucg_builtin_barrier_algo_switch(barrier_algo_decision, &ucg_algo);
            bcast_algo_decision = UCG_ALGORITHM_BCAST_AUTO_DECISION;
            allreduce_algo_decision = UCG_ALGORITHM_ALLREDUCE_AUTO_DECISION;
            break;

        default:
            break;
    }

    /* One API to deal with all special case */
    status = ucg_builtin_change_unsupport_algo(&ucg_algo, group_params, msg_size, coll_params, ops_type_choose, ops_choose, config);
    ucg_builtin_log_algo();

    return UCS_OK;
}

ucs_mpool_ops_t ucg_builtin_plan_mpool_ops = {
    .chunk_alloc   = ucs_mpool_hugetlb_malloc,
    .chunk_release = ucs_mpool_hugetlb_free,
    .obj_init      = ucs_empty_function,
    .obj_cleanup   = ucs_empty_function
};

static ucs_status_t ucg_builtin_plan(ucg_group_ctx_h ctx,
                                     const ucg_collective_params_t *params,
                                     ucg_plan_t **plan_p)
{
    ucs_status_t status;
    ucg_builtin_plan_t *plan = NULL;
    ucg_builtin_group_ctx_t *builtin_ctx = ctx;
    ucg_builtin_config_t *config = &builtin_ctx->bctx->config;
    const ucg_collective_type_t *coll_type = &UCG_PARAM_TYPE(params);
    ucp_datatype_t send_dt;

    status = ucg_builtin_init_algo(&ucg_algo);

    status = ucg_builtin_convert_datatype(params->send.dtype, &send_dt);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }
    unsigned msg_size = ucp_dt_length(send_dt, params->send.count, NULL, NULL);

    status = ucg_builtin_algorithm_decision(coll_type, msg_size, builtin_ctx->group_params, params, config);

    if (status != UCS_OK) {
        return status;
    }

    enum ucg_builtin_plan_topology_type plan_topo_type = ucg_builtin_choose_type(coll_type->modifiers);

    ucs_debug("plan topo type: %d", plan_topo_type);

    /* Build the topology according to the requested */
    switch (plan_topo_type) {
        case UCG_PLAN_RECURSIVE:
            status = ucg_builtin_recursive_create(builtin_ctx, plan_topo_type, config,
                                                  builtin_ctx->group_params, coll_type, &plan);
            break;

        case UCG_PLAN_RING:
            status = ucg_builtin_ring_create(builtin_ctx, plan_topo_type, config,
                                             builtin_ctx->group_params, coll_type, &plan);
            break;

        default:
            status = ucg_builtin_binomial_tree_create(builtin_ctx, plan_topo_type, config,
                                                      builtin_ctx->group_params, coll_type, &plan);
            break;
    }

    if (status != UCS_OK) {
        return status;
    }

    ucs_list_head_init(&plan->super.op_head);

    /* Create a memory-pool for operations for this plan */
    size_t op_size = sizeof(ucg_builtin_op_t) +
                     (plan->phs_cnt + 1) * sizeof(ucg_builtin_op_step_t);
    /* +1 is for key exchange in 0-copy cases, where an extra step is needed */
    status = ucs_mpool_init(&plan->op_mp, 0, op_size, offsetof(ucg_op_t, params),
                            UCS_SYS_CACHE_LINE_SIZE, 1, UINT_MAX,
                            &ucg_builtin_plan_mpool_ops, "ucg_builtin_plan_mp");
    if (status != UCS_OK) {
        return status;
    }

    ucs_list_add_head(&builtin_ctx->plan_head, &plan->list);

    plan->super.is_noncontig_allreduce = (plan_topo_type != UCG_PLAN_RECURSIVE) ? 0 :
            ucg_is_noncontig_allreduce(builtin_ctx->group_params, params);
    plan->super.is_ring_plan_topo_type = (plan_topo_type == UCG_PLAN_RING);

    plan->gctx      = builtin_ctx;
    plan->config    = config;
    plan->am_id     = builtin_ctx->bctx->am_id;
    *plan_p         = (ucg_plan_t*)plan;

    return UCS_OK;
}

void  ucg_builtin_set_phase_thresh_max_short(ucg_builtin_group_ctx_t *ctx,
                                             ucg_builtin_plan_phase_t *phase)
{
    if (phase->iface_attr->cap.am.max_short < sizeof(ucg_builtin_header_t)) {
        phase->send_thresh.max_short_one = 0;
    } else {
        phase->send_thresh.max_short_one = phase->iface_attr->cap.am.max_short - sizeof(ucg_builtin_header_t);
    }

    if (phase->send_thresh.max_short_one == 0) {
        phase->send_thresh.max_short_max = 0;
    } else {
        phase->send_thresh.max_short_max = ctx->bctx->config.short_max_tx;
    }

    if (phase->send_thresh.max_short_one > phase->send_thresh.max_short_max) {
        phase->send_thresh.max_short_one = phase->send_thresh.max_short_max;
    }
}

void ucg_builtin_print_flags(ucg_builtin_op_step_t *step)
{
    int flag;
    size_t buffer_length = step->buffer_length;

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND) != 0);
    if (flag) {
        if (step->flags & (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                           UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                           UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY)) {
            printf("\n\tReceive METHOD:\t\tafter sending");
        } else {
            printf("\n\tReceive METHOD:\t\treceive only");
        }
    }
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1) != 0);
    if (flag) printf("\n\tReceive method:\t\tbefore sending once");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND) != 0);
    if (flag) printf("\n\tReceive method:\t\tonce, before sending");

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT) != 0);
    if (flag) printf("\n\tSend method:\t\tshort");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) != 0);
    if (flag) printf("\n\tSend method:\t\tbcopy");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) != 0);
    if (flag) printf("\n\tSend method:\t\tzcopy");
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) != 0);
    printf("\n\tFRAGMENTED:\t\t%i", flag);
    if (flag) {
        printf("\n\t - Fragment Length: %u", step->fragment_length);
        printf("\n\t - Fragments Total: %lu", step->fragments_total);
    }

    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) != 0);
    printf("\n\tLAST_STEP:\t\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT) != 0);
    printf("\n\tSINGLE_ENDPOINT:\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED) != 0);
    printf("\n\tSTRIDED:\t\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) != 0);
    printf("\n\tPIPELINED:\t\t%i", flag);
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) != 0);
    printf("\n\tTEMP_BUFFER_USED:\t%i", flag);

#ifdef HAVE_UCP_EXTENSIONS
    flag = ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PACKED_DTYPE_MODE) != 0);
    printf("\n\tPACKED_DTYPE_MODE:\t%i", flag);
    if (flag) {
        uct_coll_dtype_mode_t mode = UCT_COLL_DTYPE_MODE_UNPACK_MODE(buffer_length);
        buffer_length              = UCT_COLL_DTYPE_MODE_UNPACK_VALUE(buffer_length);
        ucs_assert(step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT);

        printf("\n\tDatatype mode:\t\t");
        switch (mode) {
        case UCT_COLL_DTYPE_MODE_PADDED:
            printf("Data (may be padded)");
            break;

        case UCT_COLL_DTYPE_MODE_PACKED:
            printf("Packed data");
            break;

        case UCT_COLL_DTYPE_MODE_VAR_COUNT:
            printf("Variable length");
            break;

        case UCT_COLL_DTYPE_MODE_VAR_DTYPE:
            printf("Variable datatypes");
            break;

        case UCT_COLL_DTYPE_MODE_LAST:
            break;
        }
    }
#endif

    printf("\n\tData aggregation:\t");
    switch (step->comp_aggregation) {
    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP:
        printf("none");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE:
        printf("write");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER:
        printf("gather");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE:
        printf("reduce");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE_UNPACKED:
        printf("reduce unpacked");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY:
        printf("remote memory key");
        break;
    }

    printf("\n\tCompletion criteria:\t");
    switch (step->comp_criteria) {
    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND:
        printf("successful send calls");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE:
        printf("single message");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES:
        printf("multiple messages");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY:
        printf("multiple message (zero-copy)");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET:
        printf("multiple fragments");
        break;
    }

    printf("\n\tCompletion action:\t");
    switch (step->comp_action) {
    case UCG_BUILTIN_OP_STEP_COMP_OP:
        printf("finish the operation");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_STEP:
        printf("move to the next step");
        break;

    case UCG_BUILTIN_OP_STEP_COMP_SEND:
        printf("proceed to sending");
        break;
    }

    printf("\n\tStep buffer length:\t%lu", buffer_length);

    printf("\n\n");
}

static void ucg_builtin_print(ucg_plan_t *plan,
                              const ucg_collective_params_t *coll_params)
{
    ucs_status_t status;
    ucg_builtin_plan_t *builtin_plan = (ucg_builtin_plan_t*)plan;
    printf("Planner:       %s\n", builtin_plan->super.planner->name);
    printf("Phases:        %i\n", builtin_plan->phs_cnt);
    printf("P2P Endpoints: %i\n", builtin_plan->ep_cnt);

    printf("Object memory size:\n");
    printf("\tPlan: %lu bytes\n", sizeof(ucg_builtin_plan_t) +
            builtin_plan->phs_cnt * sizeof(ucg_builtin_plan_phase_t) +
            builtin_plan->ep_cnt * sizeof(uct_ep_h));
    printf("\tOperation: %lu bytes (incl. %u steps, %lu per step)\n",
            sizeof(ucg_builtin_op_t) + builtin_plan->phs_cnt *
            sizeof(ucg_builtin_op_step_t), builtin_plan->phs_cnt,
            sizeof(ucg_builtin_op_step_t));

    unsigned phase_idx;
    for (phase_idx = 0; phase_idx < builtin_plan->phs_cnt; phase_idx++) {
        printf("Phase #%i: ", phase_idx);
        printf("the method is ");
        switch (builtin_plan->phss[phase_idx].method) {
        case UCG_PLAN_METHOD_SEND_TERMINAL:
            printf("Send (T), ");
            break;
        case UCG_PLAN_METHOD_SEND_TO_SM_ROOT:
            printf("Send (SM), ");
            break;
        case UCG_PLAN_METHOD_RECV_TERMINAL:
            printf("Recv (T), ");
            break;
        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
            printf("Bcast (W), ");
            break;
        case UCG_PLAN_METHOD_SCATTER_TERMINAL:
            printf("Scatter (T), ");
            break;
        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
            printf("Scatter (W), ");
            break;
        case UCG_PLAN_METHOD_GATHER_TERMINAL:
            printf("Gather (T), ");
            break;
        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
            printf("Gather (W), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
            printf("Reduce (T), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
            printf("Reduce (W), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
            printf("Reduce (R), ");
            break;
        case UCG_PLAN_METHOD_ALLGATHER_BRUCK:
            printf("Allgather (G), ");
            break;
        case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
            printf("Alltoall (B), ");
            break;
        case UCG_PLAN_METHOD_NEIGHBOR:
            printf("Neighbors, ");
            break;

        case UCG_PLAN_METHOD_PAIRWISE:
            printf("Pairwise, ");
            break;
        case UCG_PLAN_METHOD_ALLGATHER_RECURSIVE:
            printf("Allgather (Recursive), ");
            break;
        case UCG_PLAN_METHOD_REDUCE_SCATTER_RING:
            printf("Reduce-scatter (Ring), ");
            break;
        case UCG_PLAN_METHOD_ALLGATHER_RING:
            printf("Allgather (Ring), ");
            break;
        }

#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
        ucg_builtin_plan_phase_t *phase = &builtin_plan->phss[phase_idx];
#ifdef HAVE_UCT_COLLECTIVES
        if ((phase->ep_cnt == 1) &&
            (phase->indexes[0] == UCG_GROUP_MEMBER_INDEX_UNSPECIFIED)) {
            printf("with all same-level peers (collective-aware transport)\n");
        } else
#endif
        {
            uct_ep_h *ep = (phase->ep_cnt == 1) ? &phase->single_ep :
                                                   phase->multi_eps;
            printf("with the following peers: ");

            unsigned peer_idx;
            for (peer_idx = 0;
                 peer_idx < phase->ep_cnt;
                 peer_idx++, ep++) {
                printf("%u,", phase->indexes[peer_idx]);
            }
            printf("\n");
        }
#else
        printf("no peer info (configured without \"--enable-debug-data\")");
#endif

        if (coll_params) {
            enum ucg_builtin_op_step_flags flags = 0;
            if (phase_idx == (builtin_plan->phs_cnt - 1)) {
                flags |= UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
            }

            int zcopy_step;
            ucg_builtin_op_step_t step[2];
            int8_t *temp_buffer              = NULL;
            ucg_builtin_op_init_cb_t init_cb = NULL;
            ucg_builtin_op_fini_cb_t fini_cb = NULL;

            printf("Step #%i (actual index used: %u):", phase_idx,
                    builtin_plan->phss[phase_idx].step_index);

            status = ucg_builtin_step_create(builtin_plan,
                    &builtin_plan->phss[phase_idx], &flags, coll_params,
                    &temp_buffer, 1, 1, 1, &init_cb, &fini_cb,
                    &step[0], &zcopy_step);
            if (status != UCS_OK) {
                printf("failed to create, %s", ucs_status_string(status));
            }
            if (zcopy_step) {
                status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                            coll_params,
                                                            &step[0]);
                if (status != UCS_OK) {
                    printf("failed to create, %s", ucs_status_string(status));
                }
            }

            if (phase_idx == 0) {
                printf("\n\tOP initialization:\t");
                ucg_builtin_print_init_cb_name(init_cb);

                printf("\n\tOP finalization:\t");
                ucg_builtin_print_fini_cb_name(fini_cb);
            }

            ucg_builtin_step_select_packers(coll_params, 1, 1, &step[0]);
            printf("\n\tPacker (if used):\t");
            ucg_builtin_print_pack_cb_name(step[0].bcopy.pack_single_cb);
            ucg_builtin_print_flags(&step[0]);

            if (zcopy_step) {
                ucg_builtin_step_select_packers(coll_params, 1, 1, &step[1]);
                printf("\nExtra step - RMA operation:\n\tPacker (if used):\t");
                ucg_builtin_print_pack_cb_name(step[1].bcopy.pack_single_cb);
                ucg_builtin_print_flags(&step[1]);
            }
        }
    }

    if (coll_params->send.count != 1) {
        ucs_warn("currently, short active messages are assumed for any buffer size");
    }
}

static uct_iface_attr_t mock_ep_attr = {0};
static uct_iface_t mock_iface = {0};
static uct_ep_t mock_ep = { .iface = &mock_iface };


void  ucg_builtin_set_phase_thresh_max_bcopy_zcopy(ucg_builtin_group_ctx_t *ctx,
                                                   ucg_builtin_plan_phase_t *phase)
{
    phase->send_thresh.max_bcopy_one = phase->iface_attr->cap.am.max_bcopy - sizeof(ucg_builtin_header_t);
    phase->send_thresh.max_bcopy_max = ctx->bctx->config.bcopy_max_tx;
    if (phase->md_attr->cap.max_reg) {
        if (phase->send_thresh.max_bcopy_one > phase->send_thresh.max_bcopy_max) {
            phase->send_thresh.max_bcopy_one = phase->send_thresh.max_bcopy_max;
        }
        phase->send_thresh.max_zcopy_one = phase->iface_attr->cap.am.max_zcopy - sizeof(ucg_builtin_header_t);
    } else {
        phase->send_thresh.max_zcopy_one = phase->send_thresh.max_bcopy_max = UCS_MEMUNITS_INF;
    }
}

void  ucg_builtin_set_phase_thresholds(ucg_builtin_group_ctx_t *ctx,
                                       ucg_builtin_plan_phase_t *phase)
{
    ucg_builtin_set_phase_thresh_max_short(ctx, phase);
    ucg_builtin_set_phase_thresh_max_bcopy_zcopy(ctx, phase);

    phase->send_thresh.md_attr_cap_max_reg = phase->md_attr->cap.max_reg;
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
    ucs_debug("phase create: %p, dest %u, short_one %zu, short_max %zu, bcopy_one %zu, bcopy_max %zu, zcopy_one %zu, max_reg %zu",
               phase, idx, phase->send_thresh.max_short_one, phase->send_thresh.max_short_max, phase->send_thresh.max_bcopy_one, phase->send_thresh.max_bcopy_max, phase->send_thresh.max_zcopy_one, phase->md_attr->cap.max_reg);
}


ucs_status_t ucg_builtin_connect(ucg_builtin_group_ctx_t *ctx,
                                 ucg_group_member_index_t idx,
                                 ucg_builtin_plan_phase_t *phase,
                                 unsigned phase_ep_index,
                                 enum ucg_plan_connect_flags flags,
                                 int is_mock)
{
#if ENABLE_FAULT_TOLERANCE || ENABLE_DEBUG_DATA
    if (phase->indexes == NULL) {
        phase->indexes = UCS_ALLOC_CHECK(sizeof(ucg_group_member_index_t) *
                                         phase->ep_cnt, "ucg_phase_indexes");
#if ENABLE_FAULT_TOLERANCE
        phase->handles = UCS_ALLOC_CHECK(sizeof(ucg_ft_h) * phase->ep_cnt,
                                         "ucg_phase_handles");
#endif
    }
    phase->indexes[(phase_ep_index != UCG_BUILTIN_CONNECT_SINGLE_EP) ?
            phase_ep_index : 0] = flags ? UCG_GROUP_MEMBER_INDEX_UNSPECIFIED : idx;
#endif
    if (is_mock) {
        // TODO: allocate mock attributes according to flags (and later free it)
#ifdef HAVE_UCT_COLLECTIVES
        unsigned dtype_support                 = UCS_MASK(UCT_COLL_DTYPE_MODE_LAST);
        mock_ep_attr.cap.flags                 = UCT_IFACE_FLAG_AM_SHORT |
                                                 UCT_IFACE_FLAG_INCAST   |
                                                 UCT_IFACE_FLAG_BCAST;
        mock_ep_attr.cap.am.coll_mode_flags    = dtype_support;
        mock_ep_attr.cap.coll_mode.short_flags = dtype_support;
#else
        mock_ep_attr.cap.flags                 = UCT_IFACE_FLAG_AM_SHORT;
#endif
        mock_ep_attr.cap.am.max_short          = SIZE_MAX;
        phase->host_proc_cnt                   = 0;
        phase->iface_attr                      = &mock_ep_attr;
        phase->md                              = NULL;

        if (phase_ep_index == UCG_BUILTIN_CONNECT_SINGLE_EP) {
            phase->single_ep = &mock_ep;
        } else {
            phase->multi_eps[phase_ep_index] = &mock_ep;
        }

        return UCS_OK;
    }

    uct_ep_h ep;
    ucs_status_t status = ucg_plan_connect(ctx->group, idx, flags,
            &ep, &phase->iface_attr, &phase->md, &phase->md_attr);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

#if ENABLE_FAULT_TOLERANCE
    /* Send information about any faults that may have happened */
    status = ucg_ft_propagate(ctx->group, ctx->group_params, ep);
    if (status != UCS_OK) {
        return status;
    }
#endif

    /* Store the endpoint as part of the phase */
    phase->host_proc_cnt = ctx->host_proc_cnt;
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
            phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
            ucs_assert(phase_ep_index < phase->ep_cnt);
        }
        phase->multi_eps[phase_ep_index] = ep;
    }

    /* Set the thresholds */
    ucg_builtin_set_phase_thresholds(ctx, phase);
    ucg_builtin_log_phase_info(phase, idx);

    return status;
}

ucs_status_t ucg_builtin_single_connection_phase(ucg_builtin_group_ctx_t *ctx,
                                                 ucg_group_member_index_t idx,
                                                 ucg_step_idx_t step_index,
                                                 enum ucg_builtin_plan_method_type method,
                                                 enum ucg_plan_connect_flags flags,
                                                 ucg_builtin_plan_phase_t *phase,
                                                 int is_mock)
{
    phase->ep_cnt     = 1;
    phase->step_index = step_index;
    phase->method     = method;

#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
    phase->indexes = UCS_ALLOC_CHECK(sizeof(idx), "phase indexes");
#endif

    return ucg_builtin_connect(ctx, idx, phase, UCG_BUILTIN_CONNECT_SINGLE_EP,
                               flags, is_mock);
}

static ucs_status_t ucg_builtin_handle_fault(ucg_group_ctx_h gctx,
                                             ucg_group_member_index_t index)
{
    return UCS_ERR_NOT_IMPLEMENTED;
}


UCG_PLAN_COMPONENT_DEFINE(ucg_builtin_component, "builtin",
                          sizeof(ucg_builtin_ctx_t),
                          sizeof(ucg_builtin_group_ctx_t),
                          ucg_builtin_query, ucg_builtin_init,
                          ucg_builtin_finalize, ucg_builtin_create,
                          ucg_builtin_destroy, ucg_builtin_plan,
                          ucg_builtin_op_create, ucg_builtin_op_trigger,
                          ucg_builtin_op_progress, ucg_builtin_op_discard,
                          ucg_builtin_print, ucg_builtin_handle_fault, "BUILTIN_",
                          ucg_builtin_config_table, ucg_builtin_config_t);
