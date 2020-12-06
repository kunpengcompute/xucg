/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include <stddef.h>
#include <ucs/sys/compiler_def.h>
#include <ucp/dt/dt_contig.h>

#include "builtin_ops.h"
#include "builtin_comp_step.inl"

/*
 * Below is a list of possible callback functions for operation initialization.
 */

static void ucg_builtin_init_barrier(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_collective_acquire_barrier(op->super.plan->group);
}

static void ucg_builtin_finalize_barrier(ucg_builtin_op_t *op)
{
    ucg_collective_release_barrier(op->super.plan->group);
}

static void ucg_builtin_init_gather_waypoint(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    memcpy(step->recv_buffer, step->send_buffer, step->buffer_length);
    ucs_assert(step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED);
}

static void ucg_builtin_init_gather_terminal(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buffer_length;
    memcpy(step->recv_buffer + (UCG_PARAM_TYPE(&op->super.params).root * len),
           step->send_buffer, len);
    ucs_assert((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) == 0);
}

static void ucg_builtin_init_reduce(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    memcpy(step->recv_buffer, op->super.params.send.buffer, step->buffer_length);
}

static UCS_F_ALWAYS_INLINE void
ucg_builtin_init_state(ucg_builtin_op_t *op, int option)
{
    ucg_builtin_op_step_t *step = &op->steps[0]; // TODO: (alex) temporarily broken - I want to change this to be per-op....
    const ucg_collective_params_t *params = &op->super.params;

    ucp_dt_generic_t *dt_gen;
    void *state_gen;
    // TODO: assert that this function is only called on non-contig datatypes...

    switch (option) {
        case 0:
            dt_gen = ucp_dt_to_generic(op->recv_dt);
            state_gen = dt_gen->ops.start_unpack(dt_gen->context, step->recv_buffer,
                                                 params->recv.count);

            step->bcopy.unpack_state.dt.generic.state = state_gen;
            break;

        case 1:
            dt_gen = ucp_dt_to_generic(op->send_dt);
            state_gen = dt_gen->ops.start_pack(dt_gen->context, step->send_buffer,
                                               params->send.count);

            step->bcopy.pack_state.dt.generic.state = state_gen;
            break;

        case 2:
            dt_gen = ucp_dt_to_generic(op->recv_dt);
            state_gen = dt_gen->ops.start_pack(dt_gen->context, step->recv_buffer,
                                               params->recv.count);

            step->bcopy.pack_state_recv.dt.generic.state = state_gen;
            break;

        default:
            ucs_warn("ucg_builtin_init_state, invalid option:%d", option);
            break;
    }
}

static UCS_F_ALWAYS_INLINE void
ucg_builtin_finalize_state(ucg_builtin_op_t *op, int option)
{
    ucg_builtin_op_step_t *step = &op->steps[0]; // TODO: (alex) temporarily broken - I want to change this to be per-op....

    ucp_dt_generic_t *dt_gen;
    // TODO: assert that this function is only called on non-contig datatypes...
    switch (option) {
        case 0:
            dt_gen = ucp_dt_to_generic(op->recv_dt);
            dt_gen->ops.finish(step->bcopy.unpack_state.dt.generic.state);
            break;

        case 1:
            dt_gen = ucp_dt_to_generic(op->send_dt);
            dt_gen->ops.finish(step->bcopy.pack_state.dt.generic.state);
            break;

        case 2:
            dt_gen = ucp_dt_to_generic(op->recv_dt);
            dt_gen->ops.finish(step->bcopy.pack_state_recv.dt.generic.state);
            break;

        default:
            ucs_warn("ucg_builtin_finalize_state, invalid option:%d", option);
            break;
    }
}

static void ucg_builtin_init_pack(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_init_state(op, 1);
}

static void ucg_builtin_init_unpack(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_init_state(op, 0);
}

static void ucg_builtin_init_pack_and_unpack(ucg_builtin_op_t *op,
                                             ucg_coll_id_t coll_id)
{
    ucg_builtin_init_state(op, 1);
    ucg_builtin_init_state(op, 0);

    if (op->steps[0].phase->is_swap) {
        ucg_builtin_init_state(op, 2);
    }
}

static void ucg_builtin_finalize_pack(ucg_builtin_op_t *op)
{
    ucg_builtin_finalize_state(op, 1);
}

static void ucg_builtin_finalize_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_finalize_state(op, 0);
}

static void ucg_builtin_finalize_pack_and_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_finalize_state(op, 1);
    ucg_builtin_finalize_state(op, 0);

    if (op->steps[0].phase->is_swap) {
        ucg_builtin_finalize_state(op, 2);
    }
}

/* Alltoall Bruck phase 1/3: shuffle the data */
static void ucg_builtin_init_alltoall(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    int bsize                   = step->buffer_length;
    int my_idx                  = op->super.plan->my_index;
    int nProcs                  = op->super.plan->group_size;
    int ii;

    /* Shuffle data: rank i displaces all data blocks "i blocks" upwards */
    for(ii=0; ii < nProcs; ii++){
        memcpy(step->send_buffer + bsize * ii,
               step->recv_buffer + bsize * ((ii + my_idx) % nProcs),
               bsize);
    }
}

/* Alltoall Bruck phase 2/3: send data
static void ucg_builtin_calc_alltoall(ucg_builtin_request_t *req, uint8_t *send_count,
                                      size_t *base_offset, size_t *item_interval)
{
    int kk, nProcs = req->op->super.plan->group_size;

    // k = ceil( log(nProcs) / log(2) ) communication steps
    //      - For each step k, rank (i+2^k) sends all the data blocks whose k^{th} bits are 1
    for(kk = 0; kk < ceil( log(nProcs) / log(2) ); kk++){
        unsigned bit_k    = UCS_BIT(kk);
        send_count   [kk] = bit_k;
        base_offset  [kk] = bit_k;
        item_interval[kk] = bit_k;
    }
} // TODO: re-apply the calculation in builtin_data.c
*/

/* Alltoall Bruck phase 3/3: shuffle the data */
static void ucg_builtin_finalize_alltoall(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    int bsize                   = step->buffer_length;
    int nProcs                  = op->super.plan->group_size;
    int ii;

    /* Shuffle data: rank i displaces all data blocks up by i+1 blocks and inverts vector */
    for(ii = 0; ii < nProcs; ii++){
        memcpy(step->send_buffer + bsize * ii,
               step->recv_buffer + bsize * (nProcs - 1 - ii),
               bsize);
    }
}

void ucg_builtin_init_scatter(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    ucg_builtin_plan_t *plan    = ucs_derived_of(op->super.plan, ucg_builtin_plan_t);
    void *dst                   = op->steps[plan->phs_cnt - 1].recv_buffer;
    ucg_builtin_op_step_t *step = &op->steps[0];
    void *src                   = step->send_buffer;
    size_t length               = step->buffer_length;
    size_t offset               = length * plan->super.my_index;

    if (dst != src) {
        memcpy(dst, src + offset, length);
    }
}

void ucg_builtin_print_init_cb_name(ucg_builtin_op_init_cb_t init_cb)
{
    if (init_cb == NULL) {
        printf("none");
    } else if (init_cb == ucg_builtin_init_barrier) {
        printf("barrier");
    } else if (init_cb == ucg_builtin_init_reduce) {
        printf("reduce");
    } else if (init_cb == ucg_builtin_init_gather_terminal) {
        printf("gather (terminal)");
    } else if (init_cb == ucg_builtin_init_gather_waypoint) {
        printf("gather (waypoint)");
    } else if (init_cb == ucg_builtin_init_alltoall) {
        printf("alltoall");
    } else if (init_cb == ucg_builtin_init_scatter) {
        printf("scatter");
    } else if (init_cb == ucg_builtin_init_pack) {
        printf("pack");
    } else if (init_cb == ucg_builtin_init_unpack) {
        printf("unpack");
    } else if (init_cb == ucg_builtin_init_pack_and_unpack) {
        printf("pack + unpack");
    } else {
        printf("\n");
        ucs_error("unrecognized operation initialization function");
    }
}

void ucg_builtin_print_fini_cb_name(ucg_builtin_op_fini_cb_t fini_cb)
{
    if (fini_cb == NULL) {
        printf("none");
    } else if (fini_cb == ucg_builtin_finalize_barrier) {
        printf("barrier");
    } else if (fini_cb == ucg_builtin_finalize_alltoall) {
        printf("alltoall");
    } else if (fini_cb == ucg_builtin_finalize_pack) {
        printf("pack");
    } else if (fini_cb == ucg_builtin_finalize_unpack) {
        printf("unpack");
    } else if (fini_cb == ucg_builtin_finalize_pack_and_unpack) {
        printf("pack + unpack");
    } else {
        printf("\n");
        ucs_error("unrecognized operation finalization function");
    }
}

void ucg_builtin_print_pack_cb_name(uct_pack_callback_t pack_single_cb)
{
    if (pack_single_cb == NULL) {
        printf("NONE");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 8)) {
        printf("atomic (8 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 8)) {
        printf("atomic (8 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 16)) {
        printf("atomic (16 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 16)) {
        printf("atomic (16 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 32)) {
        printf("atomic (32 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 32)) {
        printf("atomic (32 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_single_, 64)) {
        printf("atomic (64 bytes, single integer)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 64)) {
        printf("atomic (64 bytes, multiple integers)");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_reducing_, single)) {
        printf("reduction callback");
    } else if (pack_single_cb == UCG_BUILTIN_PACKER_NAME(_, single)) {
        printf("memory copy");
    }
}

/*
 * Note: Change-ID 8da6a5be2e changed the UCT API w.r.t. uct_pending_callback_t:
 *
 * After:  typedef void (*uct_completion_callback_t)(uct_completion_t *self);
 * Before: typedef void (*uct_completion_callback_t)(uct_completion_t *self,
 *                                                   ucs_status_t status);
 */
static void ucg_builtin_step_am_zcopy_comp_step_check_cb(uct_completion_t *self
#ifdef HAVE_UCT_COMP_CB_STATUS_ARG
                                                       , ucs_status_t status)
#else
                                                        )
#endif
{
    ucg_builtin_zcomp_t *zcomp  = ucs_container_of(self, ucg_builtin_zcomp_t, comp);
    ucg_builtin_request_t *req  = zcomp->req;
    ucg_builtin_op_step_t *step = req->step;
    zcomp->comp.count           = step->fragments_total;

    ucg_builtin_comp_step_cb(req);
}

ucs_status_t ucg_builtin_step_zcopy_prep(ucg_builtin_op_step_t *step)
{
    step->zcopy.zcomp.comp.count = step->fragments_total;
    step->zcopy.zcomp.comp.func  = ucg_builtin_step_am_zcopy_comp_step_check_cb;

    /* Register the buffer, creating a memory handle used in zero-copy sends */
    return uct_md_mem_reg(step->uct_md, step->send_buffer, step->buffer_length,
                          UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
}

static ucs_status_t ucg_builtin_optimize_am_bcopy_to_zcopy(ucg_builtin_op_t *op)
{
    /* This function was called because we want to "upgrade" a bcopy-send to
     * zcopy, by way of memory registration (costly, but hopefully worth it) */
    ucs_status_t status;
    ucg_builtin_op_step_t *step;
    ucg_step_idx_t step_idx = 0;
    do {
        step = &op->steps[step_idx++];
        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length)) {
            status = ucg_builtin_step_zcopy_prep(step);
            if (status != UCS_OK) {
                goto bcopy_to_zcopy_cleanup;
            }

            step->flags   &= ~UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
            step->flags   |=  UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
            step->uct_send = step->uct_iface->ops.ep_am_zcopy;

            if (step->comp_criteria ==
                    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES) {
                step->comp_criteria =
                    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY;
            }
        }
    } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    return UCS_OK;

bcopy_to_zcopy_cleanup:
    while (step_idx--) {
        if (step->zcopy.memh) {
            uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
        }
    }
    return status;
}

static ucs_status_t ucg_builtin_optimize_am_to_rma(ucg_builtin_op_t *op)
{
    // TODO: implement, especially important for "symmetric" collectives, where
    //       all ranks run a persistent collective and thus can re-use rkeys...
    return UCS_OK;
}

static ucs_status_t ucg_builtin_no_optimization(ucg_builtin_op_t *op)
{
    return UCS_OK;
}

/*
 * While some buffers are large enough to be registered (as in memory
 * registration) upon first send, others are "buffer-copied" (BCOPY) - unless
 * it is used repeatedly. If an operation is used this many times - its buffers
 * will also be registered, turning it into a zero-copy (ZCOPY) send henceforth.
 */
ucs_status_t ucg_builtin_op_consider_optimization(ucg_builtin_op_t *op,
                                                  ucg_builtin_config_t *config)
{
    ucg_builtin_op_step_t *step;
    ucg_step_idx_t step_idx = 0;

    do {
        step = &op->steps[step_idx++];
        if ((config->bcopy_to_zcopy_opt) &&
            (!op->send_dt) &&
            (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
            (step->phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_ZCOPY) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length)) {
            op->optm_cb = ucg_builtin_optimize_am_bcopy_to_zcopy;
            op->opt_cnt = config->mem_reg_opt_cnt;
            return UCS_OK;
        }

        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) &&
            (step->flags & UCG_GROUP_COLLECTIVE_MODIFIER_SYMMETRIC) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length)) {
            op->optm_cb = ucg_builtin_optimize_am_to_rma;
            op->opt_cnt = config->mem_rma_opt_cnt;
            return UCS_OK;
        }
    } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    /* Note: This function will be called... after opt_cnt wrap-around */
    op->optm_cb = ucg_builtin_no_optimization;
    op->opt_cnt = 0;
    return UCS_OK;
}

static enum ucg_builtin_op_step_flags ucg_builtin_step_method_flags[] = {
    [UCG_PLAN_METHOD_SEND_TERMINAL]    = 0,
    [UCG_PLAN_METHOD_SEND_TO_SM_ROOT]  = 0,
    [UCG_PLAN_METHOD_RECV_TERMINAL]    = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_BCAST_WAYPOINT]   = UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND,
    [UCG_PLAN_METHOD_GATHER_TERMINAL]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_GATHER_WAYPOINT]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1 |
                                         UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED,
    [UCG_PLAN_METHOD_SCATTER_TERMINAL] = UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED,
    [UCG_PLAN_METHOD_SCATTER_WAYPOINT] = UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND |
                                         UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED |
                                         UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED,
    [UCG_PLAN_METHOD_REDUCE_TERMINAL]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_REDUCE_WAYPOINT]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1 |
                                         UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED,
    [UCG_PLAN_METHOD_REDUCE_RECURSIVE] = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_ALLTOALL_BRUCK]   = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_ALLGATHER_BRUCK]  = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND,
    [UCG_PLAN_METHOD_PAIRWISE]         = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND |
                                         UCG_BUILTIN_OP_STEP_FLAG_SEND_STRIDED,
    [UCG_PLAN_METHOD_NEIGHBOR]         = UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND
};

static inline ucs_status_t
ucg_builtin_step_send_flags(ucg_builtin_op_step_t *step,
                            ucg_builtin_plan_phase_t *phase,
                            const ucg_collective_params_t *params,
#ifdef HAVE_UCT_COLLECTIVES
                            uct_coll_dtype_mode_t mode,
#endif
                            size_t dt_len, int is_dt_contig,
                            uint64_t *send_flag)
{
    size_t length      = step->buffer_length;
#ifndef HAVE_UCT_COLLECTIVES
    int supports_short = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_SHORT);
    int supports_bcopy = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_BCOPY);
    int supports_zcopy = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_ZCOPY);
#else
    int supports_short = is_dt_contig &&
                         (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_SHORT) &&
                        ((phase->iface_attr->cap.coll_mode.short_flags & UCS_BIT(mode)) ||
                         (mode == UCT_COLL_DTYPE_MODE_PADDED));
    int supports_bcopy = (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_BCOPY) &&
                        ((phase->iface_attr->cap.coll_mode.bcopy_flags & UCS_BIT(mode)) ||
                         (mode == UCT_COLL_DTYPE_MODE_PADDED));
    int supports_zcopy = is_dt_contig &&
                         (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_AM_ZCOPY) &&
                        ((phase->iface_attr->cap.coll_mode.zcopy_flags & UCS_BIT(mode)) ||
                         (mode == UCT_COLL_DTYPE_MODE_PADDED));

    ucs_assert((mode == UCT_COLL_DTYPE_MODE_PADDED) ||
               (phase->iface_attr->cap.am.coll_mode_flags & mode));
#endif

    /*
     * Short messages
     */
    if (ucs_likely(supports_short)) {
        size_t max_short = phase->iface_attr->cap.am.max_short - sizeof(ucg_builtin_header_t);
        ucs_assert(phase->iface_attr->cap.am.max_short > sizeof(ucg_builtin_header_t));
        if (ucs_likely(length <= max_short)) {
            /* Short send - single message */
            *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
            step->uct_send        = step->uct_iface->ops.ep_am_short;
            step->fragments_total = phase->ep_cnt;
            return UCS_OK;
        }

#ifdef HAVE_UCT_COLLECTIVES
        size_t max_bcopy       = phase->iface_attr->cap.am.max_bcopy;
        size_t short_msg_count = length / max_short + ((length % max_short) != 0);
        size_t bcopy_msg_count = supports_bcopy ?
                (length / max_bcopy + ((length % max_bcopy) != 0)) : SIZE_MAX;
        int is_short_best = (short_msg_count * phase->iface_attr->overhead_short) <
                            (bcopy_msg_count * phase->iface_attr->overhead_bcopy);
#else
        int is_short_best = 1;
#endif

        if (is_short_best || (!supports_bcopy && !supports_zcopy)) {
            /* Short send - multiple messages */
            *send_flag            = (enum ucg_builtin_op_step_flags)
                                    (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
                                     UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);

            step->uct_send        = step->uct_iface->ops.ep_am_short;
            step->fragment_length = max_short - (max_short % dt_len);
            step->fragments_total = phase->ep_cnt *
                                    (length / step->fragment_length +
                                     ((length % step->fragment_length) > 0));
            return UCS_OK;
        }
    }

    /*
     * Large messages (zero-copy sends)
     */
    if (supports_zcopy) {
        size_t zcopy_threshold = 100000; // TODO: need to calculate the threshold!
        if (length > zcopy_threshold) {
            size_t max_zcopy = phase->iface_attr->cap.am.max_zcopy - sizeof(ucg_builtin_header_t);
            ucs_assert(phase->iface_attr->cap.am.max_zcopy > sizeof(ucg_builtin_header_t));
            if (ucs_likely(length <= max_zcopy)) {
                /* ZCopy send - single message */
                *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
                step->uct_send        = step->uct_iface->ops.ep_am_zcopy;
                step->fragments_total = phase->ep_cnt;
            } else {
                /* ZCopy send - single message */
                *send_flag            = (enum ucg_builtin_op_step_flags)
                                        (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY |
                                         UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
                step->uct_send        = step->uct_iface->ops.ep_am_zcopy;
                step->fragment_length = max_zcopy - (max_zcopy % dt_len);
                step->fragments_total = phase->ep_cnt *
                                        (length / step->fragment_length +
                                         ((length % step->fragment_length) > 0));
            }
            return UCS_OK;
        }
    }

    if (ucs_unlikely(!supports_bcopy)) {
        ucs_error("collective not supported by any transport type");
        return UCS_ERR_UNSUPPORTED;
    }

    /*
     * Medium messages (buffer-copy)
     */
    size_t max_bcopy = phase->iface_attr->cap.am.max_bcopy - sizeof(ucg_builtin_header_t);
    ucs_assert(phase->iface_attr->cap.am.max_bcopy > sizeof(ucg_builtin_header_t));
    if (ucs_likely(length <= max_bcopy)) {
        /* BCopy send - single message */
        *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
        step->uct_send        = step->uct_iface->ops.ep_am_bcopy;
        step->fragment_length = step->buffer_length;
        step->fragments_total = phase->ep_cnt;
    } else {
        /* BCopy send - multiple messages */
        *send_flag            = (enum ucg_builtin_op_step_flags)
                                (UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
                                 UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
        step->uct_send        = step->uct_iface->ops.ep_am_bcopy;
        step->fragment_length = max_bcopy - (max_bcopy % dt_len);
        step->fragments_total = phase->ep_cnt *
                                (length / step->fragment_length +
                                 ((length % step->fragment_length) > 0));
    }

    return UCS_OK;
}

ucs_status_t
ucg_builtin_step_select_packers(const ucg_collective_params_t *params,
                                size_t send_dt_len, int is_send_dt_contig,
                                ucg_builtin_op_step_t *step)
{
    int is_signed;
    uint16_t modifiers = UCG_PARAM_TYPE(params).modifiers;
    int is_sm_reduce   = ((step->phase->method == UCG_PLAN_METHOD_SEND_TO_SM_ROOT) &&
                          (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE));

    if ((is_sm_reduce) &&
        (ucg_global_params.field_mask & UCG_PARAM_FIELD_DATATYPE_CB) &&
        (ucg_global_params.datatype.is_integer_f(params->send.dtype, &is_signed)) &&
        (!is_signed) &&
        (ucg_global_params.field_mask & UCG_PARAM_FIELD_REDUCE_OP_CB) &&
        (ucg_global_params.reduce_op.is_sum_f != NULL) &&
        (ucg_global_params.reduce_op.is_sum_f(params->send.op))) {
        int is_single = (params->send.count == 1);
        // TODO: (un)set UCG_BUILTIN_OP_STEP_FLAG_BCOPY_PACK_LOCK...
        switch (send_dt_len) {
        case 1:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 8) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 8);
            return UCS_OK;

        case 2:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 16) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 16);
            return UCS_OK;

        case 4:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 32) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 32);
            return UCS_OK;

        case 8:
            step->bcopy.pack_single_cb = is_single ?
                    UCG_BUILTIN_PACKER_NAME(_atomic_single_, 64) :
                    UCG_BUILTIN_PACKER_NAME(_atomic_multiple_, 64);
            return UCS_OK;

        default:
            ucs_error("unsupported unsigned integer datatype length: %lu",
                      send_dt_len);
            break; /* fall-back to the MPI reduction callback */
        }
    }

    int is_variadic   = (UCG_PARAM_TYPE(params).modifiers &
                         UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC);

    if (!is_send_dt_contig) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_datatype_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_datatype_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_datatype_, single);
    } else if (is_variadic) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_variadic_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_variadic_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_variadic_, single);
    } else if (is_sm_reduce) {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_reducing_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_reducing_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_reducing_, single);
    } else {
        step->bcopy.pack_full_cb   = UCG_BUILTIN_PACKER_NAME(_, full);
        step->bcopy.pack_part_cb   = UCG_BUILTIN_PACKER_NAME(_, part);
        step->bcopy.pack_single_cb = UCG_BUILTIN_PACKER_NAME(_, single);
    }

    return UCS_OK;
}

#define UCG_BUILTIN_STEP_RECV_FLAGS (UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND |\
                                     UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1|\
                                     UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND)

// TODO: make this function "static inline" again
ucs_status_t ucg_builtin_convert_datatype(void *param_datatype, ucp_datatype_t *ucp_datatype)
{
    if (ucs_unlikely(param_datatype == NULL)) {
        return ucp_dt_make_contig(1);
    }

    if (ucs_likely(ucg_global_params.field_mask & UCG_PARAM_FIELD_DATATYPE_CB)) {
        int ret = ucg_global_params.datatype.convert(param_datatype, ucp_datatype);
        if (ucs_unlikely(ret != 0)) {
            ucs_error("Datatype conversion callback failed");
            return UCS_ERR_INVALID_PARAM;
        }
    } else {
        *ucp_datatype = (ucp_datatype_t)param_datatype;
    }

    return UCS_OK;
}

ucs_status_t ucg_builtin_step_create(ucg_builtin_plan_t *plan,
                                     ucg_builtin_plan_phase_t *phase,
                                     enum ucg_builtin_op_step_flags *flags,
                                     const ucg_collective_params_t *params,
                                     int8_t **current_data_buffer,
                                     int is_send_dt_contig,
                                     int is_recv_dt_contig,
                                     size_t send_dt_len,
                                     ucg_builtin_op_init_cb_t *init_cb,
                                     ucg_builtin_op_fini_cb_t *fini_cb,
                                     ucg_builtin_op_step_t *step,
                                     int *zcopy_step_skip)
{
    ucs_status_t status;
    enum ucg_collective_modifiers modifiers = UCG_PARAM_TYPE(params).modifiers;

    /* Make sure local_id is always nonzero ( @ref ucg_builtin_header_step_t )*/
    ucs_assert_always(phase->step_index    != 0);
    ucs_assert_always(plan->super.group_id != 0);
    ucs_assert_always(phase->host_proc_cnt < (typeof(step->batch_cnt))-1);

    /* See note after ucg_builtin_step_send_flags() call */
    *zcopy_step_skip = 0;
zcopy_redo:

    /* Set the parameters determining the send-flags later on */
    step->phase                   = phase;
    step->ep_cnt                  = phase->ep_cnt;
    step->batch_cnt               = phase->host_proc_cnt - 1;
    step->am_header.group_id      = plan->super.group_id;
    step->am_header.msg.step_idx  = phase->step_index;
    step->am_header.remote_offset = 0;
    step->iter_ep                 = 0;
    step->iter_offset             = 0;
    step->fragment_pending        = NULL;
    step->buffer_length           = send_dt_len * params->send.count;
    step->recv_buffer             = (int8_t*)params->recv.buffer;
    step->uct_md                  = phase->md;
    step->flags                   = ucg_builtin_step_method_flags[phase->method];
    step->uct_iface               = (phase->ep_cnt == 1) ?
                                    phase->single_ep->iface :
                                    phase->multi_eps[0]->iface;
    step->uct_progress            = step->uct_iface->ops.iface_progress;
    /* Note: we assume all the UCT endpoints have the same interface */

    step->bcopy.pack_state.dt.generic.state   = NULL;
    step->bcopy.unpack_state.dt.generic.state = NULL;

    /* If the previous step involved receiving - plan accordingly  */
    if (*flags & UCG_BUILTIN_STEP_RECV_FLAGS) {
        step->send_buffer = *current_data_buffer ?
                            *current_data_buffer :
                            (int8_t*)params->send.buffer;
    } else {
        ucs_assert(*current_data_buffer == NULL);
        step->send_buffer = (params->send.buffer == ucg_global_params.mpi_in_place) ?
                            (int8_t*)params->recv.buffer :
                            (int8_t*)params->send.buffer;
    }

    uint64_t send_flags;
    int is_concat = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_CONCATENATE;
#ifdef HAVE_UCT_COLLECTIVES
    uct_coll_dtype_mode_t mode;
    if (is_concat) {
        if (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC) {
            mode = UCT_COLL_DTYPE_MODE_VAR_DTYPE;
        } else if (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC) {
            mode = UCT_COLL_DTYPE_MODE_VAR_COUNT;
        } else {
            mode = UCT_COLL_DTYPE_MODE_PACKED;
        }
    } else {
        mode = UCT_COLL_DTYPE_MODE_PADDED;
    }

    /* Decide how the messages are sent (regardless of my role) */
    status = ucg_builtin_step_send_flags(step, phase, params, mode,
#else
    status = ucg_builtin_step_send_flags(step, phase, params,
#endif
                                         send_dt_len, is_send_dt_contig, &send_flags);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    /*
     * Note: specifically for steps containing zero-copy communication - an
     *       additional step should precede to facilitate the zero-copy by
     *       exchanging the remote keys (both for shared memory and network).
     *       In order to avoid making "op->steps" a pointer and add dereference
     *       during trigger(), this hack here jumps one step forward (assuming
     *       op has allocated enough of them) and restarts step creation at
     *       this new step+1. The function will also indicate "upwards" to
     *       follow-up, by filling the missing step with remote key exchange.
     */
    if ((send_flags & UCT_IFACE_FLAG_AM_ZCOPY) && !(*zcopy_step_skip)) {
        *zcopy_step_skip = 1;
        step++;
        goto zcopy_redo;
    }

    step->comp_flags = 0;
#ifdef HAVE_UCT_COLLECTIVES
    if (phase->iface_attr->cap.flags & UCT_IFACE_FLAG_INCAST) {
        step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_BATCHED_DATA;
    }
#endif

    int is_fragmented = (send_flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
    if (is_fragmented) {
        step->flags           |= UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
        step->comp_flags      |= UCG_BUILTIN_OP_STEP_COMP_FLAG_FRAGMENTED_DATA;
        step->fragment_pending = (uint8_t*)UCS_ALLOC_CHECK(sizeof(phase->ep_cnt),
                                                           "ucg_builtin_step_pipelining");
    }

    if ((step->buffer_length * plan->super.group_size) > (ucg_offset_t)-1) {
        step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_LONG_BUFFERS;
    }

    /* Do any special assignment w.r.t. the src/dst buffers in this step */
    int is_send            = 0;
    int is_send_after_recv = 0;
    int is_pipelined       = 0;
    int is_reduction       = 0;
    int is_barrier         = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_BARRIER;
    int is_broadcast       = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST;
    int is_one_dest        = modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_DESTINATION;
    switch (phase->method) {
    case UCG_PLAN_METHOD_SEND_TERMINAL:
    case UCG_PLAN_METHOD_SEND_TO_SM_ROOT:
    case UCG_PLAN_METHOD_SCATTER_TERMINAL:
        is_send = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else if ((!is_broadcast) && (!is_one_dest)) {
                *init_cb = ucg_builtin_init_scatter;
            }
        }
        break;

    case UCG_PLAN_METHOD_REDUCE_TERMINAL:
        is_reduction = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else {
                *init_cb = ucg_builtin_init_reduce;
            }
        }
        /* no break */
    case UCG_PLAN_METHOD_GATHER_TERMINAL:
        if ((init_cb != NULL) && (is_concat)) {
            ucs_assert(*init_cb == NULL);
            *init_cb = ucg_builtin_init_gather_terminal;
        }
        /* no break */
    case UCG_PLAN_METHOD_RECV_TERMINAL:
        *current_data_buffer = (int8_t*)params->recv.buffer;
        break;

    case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
        is_reduction = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else {
                *init_cb = ucg_builtin_init_reduce;
            }
        }
        /* no break */
    case UCG_PLAN_METHOD_GATHER_WAYPOINT:
        if ((init_cb != NULL) && (is_concat)) {
            ucs_assert(*init_cb == NULL);
            *init_cb = ucg_builtin_init_gather_waypoint;
        }
        /* no break */
    case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
        if ((init_cb != NULL) && (*init_cb == NULL)) {
            *init_cb = ucg_builtin_init_scatter;
        }
        step->flags         |= UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED;
        step->send_buffer    =
        step->recv_buffer    =
        *current_data_buffer =
                (int8_t*)UCS_ALLOC_CHECK(step->buffer_length,
                                         "ucg_fanin_waypoint_buffer");
        // TODO: memory registration, and de-registration at some point...
        /* no break */
    case UCG_PLAN_METHOD_BCAST_WAYPOINT:
        /* for all *WAYPOINT types */
        is_send_after_recv = 1;
        is_send            = 1;

        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) == 0) {
            step->send_buffer = step->recv_buffer;
        }

        if (is_fragmented) {
            step->flags |= UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
            is_pipelined = 1;
        }
        break;

    case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
        /* First step is the exception to this rule */
        is_send = 1;
        is_reduction = 1;
        if (init_cb != NULL) {
            if (is_barrier) {
                ucs_assert(fini_cb != NULL);
                *init_cb = ucg_builtin_init_barrier;
                *fini_cb = ucg_builtin_finalize_barrier;
            } else {
                *init_cb = ucg_builtin_init_reduce;
            }
        }
        if (phase->step_index == 1) {
            break;
        }
        /* no break */
    case UCG_PLAN_METHOD_NEIGHBOR:
        is_send = 1;
        step->send_buffer = step->recv_buffer;
        break;

    case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
        is_send = 1;
        if (init_cb != NULL) {
            ucs_assert(fini_cb != NULL);
            *init_cb = ucg_builtin_init_alltoall;
            *fini_cb = ucg_builtin_finalize_alltoall;
        }
        break;

    default:
        break;
    }

    if (is_reduction && !is_barrier) {
        if (!(ucg_global_params.field_mask & UCG_PARAM_FIELD_REDUCE_OP_CB)) {
            ucs_error("Cannot perform reductions: Missing ucg_init() parameters");
            return UCS_ERR_INVALID_PARAM;
        }

        if (!ucg_global_params.reduce_op.is_commutative_f(UCG_PARAM_OP(params))) {
            ucs_error("Cannot perform reduction: non-commutative operations unsupported");
            return UCS_ERR_UNSUPPORTED;
            // TODO: set UCG_GROUP_COLLECTIVE_MODIFIER_AGGREGATE_STABLE instead
        }

        if (ucg_global_params.reduce_op.is_loc_expected_f(UCG_PARAM_OP(params))) {
            ucs_error("Cannot perform reductions: MPI's MINLOC/MAXLOC unsupported");
            return UCS_ERR_UNSUPPORTED;
        }
    }

    if (is_send) {
        /* packer callback selection */
        if (send_flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) {
            status = ucg_builtin_step_select_packers(params, send_dt_len,
                                                     is_send_dt_contig, step);
            if (ucs_unlikely(status != UCS_OK)) {
                return status;
            }

            if (!is_send_dt_contig) {
                step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_DATATYPE;
            }
        }

#ifdef HAVE_UCT_COLLECTIVES
        if ((is_send) &&
            (phase->iface_attr->cap.flags & (UCT_IFACE_FLAG_INCAST |
                                             UCT_IFACE_FLAG_BCAST))) {
            send_flags |= UCG_BUILTIN_OP_STEP_FLAG_PACKED_DTYPE_MODE;
            if (send_flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT) {
                if (is_fragmented) {
                    step->fragment_length =
                            UCT_COLL_DTYPE_MODE_PACK(mode, step->fragment_length);
                } else {
                    step->buffer_length =
                            UCT_COLL_DTYPE_MODE_PACK(mode, step->buffer_length);
                }
                step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_LENGTH;
            } else if (modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_VARIADIC) {
                //step->zcopy.iov[1].buffer = (void*)params->send.displs;
            }
        }
#endif
    } else {
        if (!is_recv_dt_contig) {
            step->comp_flags |= UCG_BUILTIN_OP_STEP_COMP_FLAG_PACKED_DATATYPE;
        } else {
            step->buffer_length = params->recv.count * send_dt_len;
            // TODO: fix for cases where send and receive datatypes differ
        }
    }

    if (is_concat) {
#if ENABLE_DEBUG_DATA || ENABLE_FAULT_TOLERANCE
        /* Assume only one-level gathers, so the parent is #0 */
        ucs_assert(phase->indexes[phase->ep_cnt - 1] == 0);
        /* TODO: remove this restriction */
#endif
        /* Assume my peers have a higher rank/index for offset calculation */
        step->am_header.remote_offset = plan->super.my_index * step->buffer_length;
    }

    /* memory registration (using the memory registration cache) */
    int is_zcopy = (send_flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY);
    if (is_zcopy) {
        status = ucg_builtin_step_zcopy_prep(step);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    }

    int is_last = *flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    if (is_last) {
        step->flags |= UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    }

    if (is_barrier || !(step->flags & UCG_BUILTIN_STEP_RECV_FLAGS)) {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_NOP;
    } else if ((send_flags & UCT_IFACE_FLAG_AM_ZCOPY) && (zcopy_step_skip)) {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REMOTE_KEY;
    } else if ((phase->method == UCG_PLAN_METHOD_GATHER_TERMINAL) ||
               (phase->method == UCG_PLAN_METHOD_GATHER_WAYPOINT)) {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_GATHER;
    } else if ((phase->method == UCG_PLAN_METHOD_REDUCE_TERMINAL) ||
               (phase->method == UCG_PLAN_METHOD_REDUCE_RECURSIVE)) {
        if (is_fragmented) {
            step->dtype_length = send_dt_len;
        }
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_REDUCE;
        ucs_assert(params->recv.count > 0);
    } else {
        step->comp_aggregation = UCG_BUILTIN_OP_STEP_COMP_AGGREGATE_WRITE;
    }

    /* Choose a completion criteria to be checked by the incoming AM handler */
    if (phase->ep_cnt == 1) {
        step->flags |= UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT;
        if (is_fragmented) {
            if (is_pipelined) {
                step->comp_criteria =
                        UCG_BUILTIN_OP_STEP_COMP_CRITERIA_BY_FRAGMENT_OFFSET;
            } else {
                step->comp_criteria = is_zcopy ?
                        UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY :
                        UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES;
            }
        } else {
            step->comp_criteria =
                    UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SINGLE_MESSAGE;
        }
    } else {
        step->comp_criteria = is_zcopy ?
                UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES_ZCOPY :
                UCG_BUILTIN_OP_STEP_COMP_CRITERIA_MULTIPLE_MESSAGES;
    }

    if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND) == 0) {
        step->comp_criteria = UCG_BUILTIN_OP_STEP_COMP_CRITERIA_SEND;
    }

    /* Choose the completion action to be taken by the incoming AM handler */
    step->comp_action = is_last ? UCG_BUILTIN_OP_STEP_COMP_OP :
                                  UCG_BUILTIN_OP_STEP_COMP_STEP;
    if (is_send) {
        step->flags |= send_flags;
        if (is_send_after_recv) {
            step->comp_action = UCG_BUILTIN_OP_STEP_COMP_SEND;
        }
    }

    *flags = step->flags; // TODO: handle case with UCT_COLL_TYPE_PACK/UNPACK
    return UCS_OK;
}

ucs_status_t ucg_builtin_step_create_rkey_bcast(ucg_builtin_plan_t *plan,
                                                const ucg_collective_params_t *params,
                                                ucg_builtin_op_step_t *step)
{
    ucg_collective_params_t step_params;

    ucg_builtin_op_step_t *zcopy_step = step + 1;
    ucg_group_member_index_t root     = UCG_PARAM_TYPE(params).root;
    unsigned is_bcast                 = zcopy_step->flags &
                                        UCG_GROUP_COLLECTIVE_MODIFIER_BROADCAST;

    ucs_assert((zcopy_step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_PUT_ZCOPY) ||
               (zcopy_step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_GET_ZCOPY));

    /* Prepare to send out my remote key */
    size_t rkey_size          = step->phase->md_attr->rkey_packed_size;
    size_t info_size          = sizeof(uint64_t) + rkey_size;
    size_t total_size         = is_bcast ? info_size :
                                           info_size * plan->super.group_size;

    uint8_t *info_buffer      = UCS_ALLOC_CHECK(total_size, "builtin_rkey_info");

    /* Set some parameters for step creation */
    memset(&step_params, 0, sizeof(step_params));
    UCG_PARAM_TYPE(&step_params).modifiers = is_bcast |
                                             UCG_GROUP_COLLECTIVE_MODIFIER_SINGLE_SOURCE;
    UCG_PARAM_TYPE(&step_params).root      = root;
    step_params.send.buffer                = info_buffer;
    step_params.send.count                 = 1;
    step_params.recv.buffer                = info_buffer;
    step_params.recv.count                 = 1;

    if (root == plan->super.my_index) {
        ucs_assert(step->send_buffer == step->recv_buffer); // TODO: choose the right buffer!
        *((uint64_t*)info_buffer) = (uint64_t)step->send_buffer;
        info_buffer              += sizeof(uint64_t);

        ucs_status_t status = uct_md_mkey_pack(step->uct_md,
                                               step->zcopy.memh,
                                               info_buffer);
        if (status != UCS_OK) {
            return status;
        }

        if (!is_bcast) {
            /* Copy the same memory key before scattering among the peers */
            int idx;
            uint8_t *info_iter = info_buffer;
            for (idx = 1; idx < plan->super.group_size; idx++) {
                info_iter += sizeof(uint64_t) + rkey_size;
                /*
                 * Note: we can't set the final pointers here just yet, because
                 *       the counters/displacements pointer may be given, but
                 *       may point to an array containing different values on
                 *       every call...
                 */
                memcpy(info_iter, info_buffer, rkey_size);
            }
        }
    }

    /* Create the preliminary remote-key-exchange step */
    int dummy_skip;
    enum ucg_builtin_op_step_flags flags = UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED;
    return ucg_builtin_step_create(plan, step->phase, &flags, &step_params,
                                   NULL, 1, 1, info_size, NULL, NULL, step,
                                   &dummy_skip);
}

ucs_status_t ucg_builtin_op_create(ucg_plan_t *plan,
                                   const ucg_collective_params_t *params,
                                   ucg_op_t **new_op)
{
    ucs_status_t status;
    ucg_builtin_plan_t *builtin_plan     = (ucg_builtin_plan_t*)plan;
    ucg_builtin_plan_phase_t *next_phase = &builtin_plan->phss[0];
    unsigned phase_count                 = builtin_plan->phs_cnt;
    ucg_builtin_op_t *op                 = (ucg_builtin_op_t*)
                                            ucs_mpool_get_inline(&builtin_plan->op_mp);
    ucg_builtin_op_step_t *next_step     = &op->steps[0];
    int is_send_dt_contig                = 1;
    int is_recv_dt_contig                = 1;
    int zcopy_step_skip                  = 0;
    int8_t *current_data_buffer          = NULL;
    op->init_cb                          = NULL;
    op->fini_cb                          = NULL;

    /* obtain UCX datatypes corresponding to the extenral datatypes passed */
    if (params->send.count > 0) {
        status = ucg_builtin_convert_datatype(params->send.dtype, &op->send_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        is_send_dt_contig = UCP_DT_IS_CONTIG(op->send_dt);
    }

    if (params->recv.count > 0) {
        status = ucg_builtin_convert_datatype(params->recv.dtype, &op->recv_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }

        is_recv_dt_contig = UCP_DT_IS_CONTIG(op->recv_dt);
    }

    /* copy the parameters aside, and use those from now on */
    memcpy(&op->super.params, params, sizeof(*params));
    params = &op->super.params;
    size_t send_dt_len = ucp_dt_length(op->send_dt, 1, NULL, next_step->bcopy.pack_state.dt.generic.state);
    /* Note: this needs to be after op->params and op->send_dt are set */

//    /* Check for non-zero-root trees */ // TODO: (alex) replace with something?
//    if (ucs_unlikely(UCG_PARAM_TYPE(params).root != 0)) {
//        /* Assume the plan is tree-based, since Recursive K-ing has no root */
//        status = ucg_builtin_topo_tree_set_root(UCG_PARAM_TYPE(params).root,
//                                                plan->my_index, builtin_plan,
//                                                &next_phase, &phase_count);
//        if (ucs_unlikely(status != UCS_OK)) {
//            return status;
//        }
//    }

    /* Create a step in the op for each phase in the topology */
    enum ucg_builtin_op_step_flags flags = 0;
    if (phase_count == 1) {
        /* The only step in the plan */
        flags = UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
        status = ucg_builtin_step_create(builtin_plan, next_phase, &flags,
                                         params, &current_data_buffer,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, &op->init_cb, &op->fini_cb,
                                         next_step, &zcopy_step_skip);

        if ((status == UCS_OK) && ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
        }
    } else {
        /* First step of many */
        status = ucg_builtin_step_create(builtin_plan, next_phase, &flags,
                                         params, &current_data_buffer,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, &op->init_cb, &op->fini_cb,
                                         next_step, &zcopy_step_skip);
        if (ucs_unlikely(status != UCS_OK)) {
            goto op_cleanup;
        }

        if (ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
            if (ucs_unlikely(status != UCS_OK)) {
                goto op_cleanup;
            }

            zcopy_step_skip = 0;
            next_step++;
        }

        ucg_step_idx_t step_cnt;
        for (step_cnt = 1; step_cnt < phase_count - 1; step_cnt++) {
            status = ucg_builtin_step_create(builtin_plan, ++next_phase, &flags,
                                             params, &current_data_buffer,
                                             is_send_dt_contig, is_recv_dt_contig,
                                             send_dt_len, NULL, NULL, ++next_step,
                                             &zcopy_step_skip);
            if (ucs_unlikely(status != UCS_OK)) {
                goto op_cleanup;
            }

            if (ucs_unlikely(zcopy_step_skip)) {
                status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                            params,
                                                            next_step);
                if (ucs_unlikely(status != UCS_OK)) {
                    goto op_cleanup;
                }

                zcopy_step_skip = 0;
                next_step++;
            }
        }

        /* Last step gets a special flag */
        flags |= UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
        status = ucg_builtin_step_create(builtin_plan, ++next_phase, &flags,
                                         params, &current_data_buffer,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, NULL, NULL, ++next_step,
                                         &zcopy_step_skip);
        if ((status == UCS_OK) && ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
        }
    }
    if (ucs_unlikely(status != UCS_OK)) {
        goto op_cleanup;
    }

    /* Handle non-contiguous datatypes */
    // TODO: handle send-only or recieve-only collectives where the other
    //       datatype should be disregarded, contiguous or not.
    if (!is_send_dt_contig) {
        ucs_assert(op->init_cb == NULL);
        ucs_assert(op->fini_cb == NULL);

        if (!is_recv_dt_contig) {
            op->init_cb = ucg_builtin_init_pack_and_unpack;
            op->fini_cb = ucg_builtin_finalize_pack_and_unpack;
        } else {
            op->init_cb = ucg_builtin_init_pack;
            op->fini_cb = ucg_builtin_finalize_pack;
        }
    } else if (!is_recv_dt_contig) {
        ucs_assert(op->init_cb == NULL);
        ucs_assert(op->fini_cb == NULL);

        op->init_cb = ucg_builtin_init_unpack;
        op->fini_cb = ucg_builtin_finalize_unpack;
    }

    /* Select the right optimization callback */
    status = ucg_builtin_op_consider_optimization(op, builtin_plan->config);
    if (status != UCS_OK) {
        goto op_cleanup;
    }

    UCS_STATIC_ASSERT(sizeof(ucg_builtin_header_t) <= UCP_WORKER_HEADROOM_PRIV_SIZE);
    UCS_STATIC_ASSERT(sizeof(ucg_builtin_header_t) == sizeof(uint64_t));

    op->super.trigger_f = ucg_builtin_op_trigger;
    op->super.discard_f = ucg_builtin_op_discard;
    op->super.plan      = plan;
    op->gctx            = builtin_plan->gctx;
    *new_op             = &op->super;

    return UCS_OK;

op_cleanup:
    ucs_mpool_put_inline(op);
    return status;
}

void ucg_builtin_op_discard(ucg_op_t *op)
{
    ucg_builtin_op_t *builtin_op = (ucg_builtin_op_t*)op;
    ucg_builtin_op_step_t *step = &builtin_op->steps[0];
    do {
        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
            uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
            uct_rkey_release(step->zcopy.cmpt, &step->zcopy.rkey);
        }

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) {
            ucs_free(step->recv_buffer);
        }

        if (step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
            ucs_free((void*)step->fragment_pending);
        }
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    ucs_mpool_put_inline(op);
}

#define UCG_BUILTIN_OP_GET_SLOT_PTR(_gctx, _slot_id) \
    UCS_PTR_BYTE_OFFSET(_gctx, _slot_id * sizeof(ucg_builtin_comp_slot_t))

ucs_status_t ucg_builtin_op_trigger(ucg_op_t *op,
                                    ucg_coll_id_t coll_id,
                                    void *request)
{
    /* Allocate a "slot" for this operation, from a per-group array of slots */
    ucg_builtin_op_t *builtin_op  = (ucg_builtin_op_t*)op;
    unsigned slot_idx             = coll_id % UCG_BUILTIN_MAX_CONCURRENT_OPS;
    ucg_builtin_group_ctx_t *gctx = builtin_op->gctx;
    ucg_builtin_comp_slot_t *slot = UCG_BUILTIN_OP_GET_SLOT_PTR(gctx, slot_idx);

    if (ucs_unlikely(slot->req.expecting.local_id != 0)) {
        ucs_error("UCG Builtin planner exceeded its concurrent collectives limit.");
        return UCS_ERR_NO_RESOURCE;
    }

    /* Initialize the request structure, located inside the selected slot s*/
    ucg_builtin_request_t *builtin_req = &slot->req;
    builtin_req->op                    = builtin_op;
    ucg_builtin_op_step_t *first_step  = builtin_op->steps;
    builtin_req->step                  = first_step;
    builtin_req->pending               = first_step->fragments_total;
    ucg_builtin_header_t header        = first_step->am_header;
    builtin_req->comp_req              = request;
    builtin_op->current                = &builtin_req->step;

    /* Sanity checks */
    ucs_assert(first_step->am_header.msg.step_idx != 0);
    ucs_assert(first_step->iter_offset == 0);
    ucs_assert(first_step->iter_ep == 0);
    ucs_assert(request != NULL);

    /*
     * For some operations, like MPI_Reduce, MPI_Allreduce or MPI_Gather, the
     * local data has to be aggregated along with the incoming data. In others,
     * some shuffle is required once before starting (e.g. Bruck algorithms).
     */
    if (ucs_unlikely(builtin_op->init_cb != NULL)) {
        builtin_op->init_cb(builtin_op, coll_id);
    }

    /* Start the first step, which may actually complete the entire operation */
    header.msg.coll_id = coll_id;
    return ucg_builtin_step_execute(builtin_req, header);
}





// TODO: (alex) relocate these functions left from builtin_cb.inl
//
//
///*
// * Below is a list of possible callback functions for pretreatment before sending.
// */
//
///* send_cb for alltoall to sned discrete elements */
//static void ucg_builtin_send_alltoall(ucg_builtin_request_t *req)
//{
//    unsigned i, k;
//    size_t len = req->step->buf_len_unit;
//    ucg_builtin_op_step_t *step = req->step;
//    size_t buffer_length_discrete = 0;
//    if (step->displs_rule == UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL) {
//        k = (unsigned)step->am_header.step_idx;
//        for (i = 0; i < num_procs; i++) {
//            if ((i >> k) & 1) { //kth bit is 1
//                memcpy(step->send_buffer + buffer_length_discrete * len,
//                    step->recv_buffer + i * len, len);
//                buffer_length_discrete++;
//            }
//        }
//    }
//}
//
///*
// * Below is a list of possible callback functions for operation initialization.
// */
//static void ucg_builtin_init_dummy(ucg_builtin_op_t *op) {}
//
//static void ucg_builtin_init_gather(ucg_builtin_op_t *op)
//{
//    ucg_builtin_op_step_t *step = &op->steps[0];
//    size_t len = step->buffer_length;
//    memcpy(step->recv_buffer + (op->super.plan->group_id * len),
//            step->send_buffer, len);
//}
//
//static void ucg_builtin_init_reduce(ucg_builtin_op_t *op)
//{
//    ucg_builtin_op_step_t *step = &op->steps[0];
//    if (op->super.params.send.buf == MPI_IN_PLACE) {
//        memcpy(step->recv_buffer, op->super.params.recv.buf, step->buffer_length);
//    } else {
//        memcpy(step->recv_buffer, op->super.params.send.buf, step->buffer_length);
//    }
//}
//
//static void ucg_builtin_init_ring(ucg_builtin_op_t *op)
//{
//    ucg_builtin_op_step_t *step = &op->steps[0];
//    size_t len = step->buf_len_unit;
//    unsigned step_idx;
//    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
//        (&op->steps[step_idx])->am_header.remote_offset = (&op->steps[step_idx])->remote_offset;
//    }
//
//    memcpy(step->recv_buffer, step->send_buffer - step->am_header.remote_offset, len);
//}
//
///* for allgather, add initial step for first element storage*/
//static void ucg_builtin_init_allgather(ucg_builtin_op_t *op)
//{
//    ucg_builtin_op_step_t *step = &op->steps[0];
//    size_t len = step->buf_len_unit;
//    memcpy(step->recv_buffer, step->send_buffer, len);
//    //set offset of every step for allgather
//    ucg_builtin_plan_t* builtin_plan = (ucg_builtin_plan_t*)op->super.plan;
//    for (unsigned step_index = 0; step_index < builtin_plan->phs_cnt; step_index++, step++) {
//        step->am_header.remote_offset = len;
//        for (unsigned i = 0; i < step_index; i++) {
//            size_t step_idx_offset = 1UL << i;
//            step->am_header.remote_offset += step_idx_offset * len;
//        }
//    }
//}
//
//static void ucg_builtin_init_allgather_recursive(ucg_builtin_op_t *op)
//{
//    ucg_builtin_op_step_t *step = &op->steps[0];
//    size_t init_offset = 0;
//    init_offset = op->super.plan->my_index * op->super.params.send.count *op->super.params.send.dt_len;
//    memcpy(step->recv_buffer + init_offset, step->send_buffer, step->buffer_length);
//}
//
///* for alltoall, add initial step for local rotation*/
//static void ucg_builtin_init_alltoall(ucg_builtin_op_t *op)
//{
//    const ucg_group_params_t *params = ucg_group_get_params(op->super.plan->group);
//    size_t proc_count = params->member_count;
//    size_t my_index   = op->super.plan->my_index;
//    ucg_builtin_op_step_t *step = &op->steps[0];
//    size_t len = step->buf_len_unit;
//
//    memcpy(step->recv_buffer, step->send_buffer + my_index * len, (proc_count - my_index)*len);
//
//    if (my_index != 0) {
//        memcpy(step->recv_buffer + (proc_count - my_index)*len, step->send_buffer, my_index*len);
//    }
//}
//
//
//
///* local shift for allgather at final step */
//static void ucg_builtin_final_allgather(ucg_builtin_request_t *req)
//{
//    const ucg_group_params_t *params = ucg_group_get_params(req->op->super.plan->group);
//    size_t num_procs_count  = params->member_count;
//    size_t len = req->step->buf_len_unit;
//    size_t my_index   = req->op->super.plan->my_index;
//    size_t len_move = len * (num_procs_count - my_index);
//    void *temp_buffer = ucs_calloc(1, len * (num_procs_count - 1), "ucg_allgather_final_step_buffer");
//    ucs_assert(temp_buffer != NULL);
//    if (req->op->super.plan->my_index != 0) {
//        memcpy(temp_buffer, req->step->recv_buffer, len_move);
//        memmove(req->step->recv_buffer, req->step->recv_buffer + len_move, len*my_index);
//        memcpy(req->step->recv_buffer + len * my_index, temp_buffer, len_move);
//    }
//    free(temp_buffer);
//    temp_buffer = NULL;
//}
//
///* local inverse rotation for alltoall at final step */
//static void ucg_builtin_final_alltoall(ucg_builtin_request_t *req)
//{
//    const ucg_group_params_t *params = ucg_group_get_params(req->op->super.plan->group);
//    size_t num_procs_count = params->member_count;
//    size_t len       = req->step->buf_len_unit;
//    size_t my_index  = req->op->super.plan->my_index;
//
//    size_t dst;
//    unsigned i;
//    size_t len_move = len * num_procs_count;
//    int8_t *temp_buffer = (int8_t*)ucs_calloc(1, len * num_procs_count, "ucg_alltoall_final_step_buffer");
//    ucs_assert(temp_buffer != NULL);
//    for (i = 0; i < num_procs_count; i++) {
//        dst = (my_index - i + num_procs_count) % num_procs_count;
//        memcpy(temp_buffer + dst * len, req->step->recv_buffer + i * len, len);
//    }
//    memcpy(req->step->recv_buffer, temp_buffer, len_move);
//    free(temp_buffer);
//    temp_buffer = NULL;
//}
//
//// TODO: (alex) relocate these functions left from builtin_ops.c
//
//void ucg_builtin_dispose_packet(ucg_builtin_comp_desc_t *desc)
//{
//    /* Dispose of the packet, according to its allocation */
//    if (desc->super.flags == UCT_CB_PARAM_FLAG_DESC) {
//        uct_iface_release_desc(desc);
//    } else {
//        ucs_mpool_put_inline(desc);
//    }
//}
//
//ucs_status_t ucg_builtin_msg_process(ucg_builtin_comp_slot_t *slot, ucg_builtin_request_t *req)
//{
//    static unsigned loop_cnt = 0;
//    static unsigned is_return = 0;
//    unsigned max_msg_list_size = ((ucg_builtin_config_t*) req->op->super.plan->planner->plan_config)->max_msg_list_size;
//
//    /* Look for matches in list of packets waiting on this slot */
//    uint16_t local_id = slot->local_id;
//    ucg_builtin_op_step_t *step = req->step;
//
//    ucg_builtin_comp_desc_t *desc = NULL;
//    ucg_builtin_comp_desc_t *iter = NULL;
//
//    ucs_list_for_each_safe(desc, iter, &slot->msg_head, super.tag_list[0]) {
//        /*
//         * Note: stored message coll_id can be either larger or smaller than
//         * the one currently handled - due to coll_id wrap-around.
//         */
//        if (ucs_likely(desc->header.local_id == local_id)) {
//            /* Check loop count - return in_progress if attach max size */
//            if (++loop_cnt > max_msg_list_size) {
//                is_return = 1;
//                loop_cnt--;
//                return UCS_INPROGRESS;
//            }
//
//            /* Remove the packet (next call may lead here recursively) */
//            ucs_list_del(&desc->super.tag_list[0]);
//
//            char *header_tmp = &desc->data[0];
//            char *recv_buffer_tmp = (char *)slot->req.step->recv_buffer;
//            size_t real_length = desc->super.length;
//            if (req->step->phase->is_swap) {
//                char *temp_buffer = (char*)UCS_ALLOC_CHECK(real_length, "temp buffer");
//                memcpy(temp_buffer, header_tmp, real_length);
//                memcpy(header_tmp, recv_buffer_tmp + desc->header.remote_offset, real_length);
//                memcpy(recv_buffer_tmp + desc->header.remote_offset, temp_buffer, real_length);
//                free(temp_buffer);
//                temp_buffer = NULL;
//            }
//
//            /* Handle this "waiting" packet, possibly completing the step */
//            int is_step_done = step->recv_cb(&slot->req,
//                                             desc->header.remote_offset, &desc->data[0],
//                                             desc->super.length);
//            ucg_builtin_dispose_packet(desc);
//
//            loop_cnt--;
//
//            /* If the step has indeed completed - check the entire op */
//            if (is_step_done) {
//                /* Continue msg processing if return by loop check */
//                if (loop_cnt == 0 && is_return == 1) {
//                    is_return = 0;
//                    return ucg_builtin_msg_process(slot, req);
//                } else {
//                    return (req->comp_req->flags & UCP_REQUEST_FLAG_COMPLETED) ?
//                           req->comp_req->status : UCS_INPROGRESS;
//                }
//            }
//        }
//    }
//
//    return UCS_INPROGRESS;
//}
//
//static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_send_flags(ucg_builtin_op_step_t *step,
//                                                                    ucg_builtin_plan_phase_t *phase,
//                                                                    const ucg_collective_params_t *params,
//                                                                    enum ucg_builtin_op_step_flags *send_flag)
//{
//    size_t length = step->buffer_length;
//    size_t dt_len = params->send.dt_len;
//    unsigned partial_length = 0;
//
//    /* Flag whether to go error and resend data */
//    step->resend_flag = UCG_BUILTIN_OP_STEP_FIRST_SEND;
//
//    /*
//     * Short messages (e.g. RDMA "inline")
//     */
//    if (ucs_likely(length <= phase->send_thresh.max_short_one
//                   && phase->send_thresh.max_short_one != 0)) {
//        /* Short send - single message */
//        *send_flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT;
//        step->fragments = 1;
//    } else if (ucs_likely(length <= phase->send_thresh.max_short_max
//                        && phase->send_thresh.max_short_max != 0
//                        )) {
//        if (ucs_likely(dt_len <= phase->send_thresh.max_short_one)) {
//            /* Short send - multiple messages */
//            step->fragment_length = phase->send_thresh.max_short_one - (phase->send_thresh.max_short_one % dt_len);
//        } else {
//            step->fragment_length = phase->send_thresh.max_short_one;
//        }
//        ucs_assert(step->fragment_length > 0);
//        *send_flag = (enum ucg_builtin_op_step_flags)(UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_SHORT |
//                UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
//        partial_length = (length % step->fragment_length) > 0;
//        step->fragments = length / step->fragment_length + partial_length;
//
//    /*
//     * Large messages, if supported (e.g. RDMA "zero-copy")
//     */
//    } else if (ucs_unlikely((length >  phase->send_thresh.max_bcopy_max) &&
//                            (phase->md_attr->cap.max_reg))) {
//        if (ucs_likely(length < phase->send_thresh.max_zcopy_one)) {
//            /* ZCopy send - single message */
//            *send_flag            = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
//            step->fragments       = 1;
//        } else {
//            /* ZCopy send - multiple message */
//            if (ucs_likely(dt_len <= phase->send_thresh.max_zcopy_one)) {
//                step->fragment_length = phase->send_thresh.max_zcopy_one - (phase->send_thresh.max_zcopy_one % dt_len);
//            } else {
//                step->fragment_length = phase->send_thresh.max_zcopy_one;
//            }
//            ucs_assert(step->fragment_length > 0);
//            *send_flag = (enum ucg_builtin_op_step_flags)(UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY |
//                    UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
//            partial_length = (length % step->fragment_length) > 0;
//            step->fragments = length / step->fragment_length + partial_length;
//        }
//
//        if (phase->method != UCG_PLAN_METHOD_RECV_TERMINAL && phase->method != UCG_PLAN_METHOD_REDUCE_TERMINAL) {
//            /* memory registration (using the memory registration cache) */
//            ucs_status_t status = ucg_builtin_step_zcopy_prep(step);
//            if (ucs_unlikely(status != UCS_OK)) {
//                return status;
//            }
//        } else {
//            /* recv only method */
//            return UCS_OK;
//        }
//
//    /*
//     * Medium messages
//     */
//    } else if (ucs_likely(length <= phase->send_thresh.max_bcopy_one)) {
//        /* BCopy send - single message */
//        *send_flag = UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
//        step->fragment_length = step->buffer_length;
//        step->fragments       = 1;
//    } else {
//        /* BCopy send - multiple messages */
//        if (ucs_likely(dt_len <= phase->send_thresh.max_bcopy_one)) {
//            step->fragment_length = phase->send_thresh.max_bcopy_one - (phase->send_thresh.max_bcopy_one % dt_len);
//        } else {
//            step->fragment_length = phase->send_thresh.max_bcopy_one;
//        }
//        ucs_assert(step->fragment_length > 0);
//        *send_flag = (enum ucg_builtin_op_step_flags)(UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY |
//                UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
//        partial_length = (length % step->fragment_length) > 0;
//        step->fragments = length / step->fragment_length + partial_length;
//    }
//
//    return UCS_OK;
//}
//
//
//static UCS_F_ALWAYS_INLINE void ucg_builtin_step_fragment_flags(size_t thresh_one,
//                                                                size_t dt_len,
//                                                                size_t length,
//                                                                ucg_builtin_op_step_t *step,
//                                                                ucg_builtin_plan_phase_t *phase,
//                                                                enum ucg_builtin_op_step_flags *recv_flag)
//{
//    unsigned partial_length = 0;
//    size_t fragment_length = 0;
//    if (ucs_unlikely(dt_len > thresh_one)) {
//        phase->segmented = 1;
//        fragment_length = thresh_one;
//    } else {
//        fragment_length = thresh_one - (thresh_one % dt_len);
//    }
//
//    if (fragment_length == 0) {
//        return;
//    }
//    *recv_flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
//    partial_length = (length % fragment_length) > 0;
//    step->fragments_recv = length / fragment_length + partial_length;
//}
//
///*
// * For some algorithms (e.g. Bruck, Ring), the thresholds of sender and receiver
// * are not same!
// * So, receiver should set fragment_recv according to phase->max_XXX_recv and
// * recv_flag should also be set to distinguish with send_flag to choose correct recv_cb.
// */
//static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_step_recv_flags(ucg_builtin_op_step_t *step,
//                                                                    ucg_builtin_plan_phase_t *phase,
//                                                                    const ucg_collective_params_t *params,
//                                                                    enum ucg_builtin_op_step_flags *recv_flag)
//{
//    *recv_flag = (enum ucg_builtin_op_step_flags)0;
//    size_t length = step->buffer_length;
//    size_t fragment_length = 0;
//    unsigned partial_length = 0;
//
//    /* for ring, the length of send_buffer and recv_buffer may be different */
//    if (phase->method == UCG_PLAN_METHOD_REDUCE_SCATTER_RING ||
//        phase->method == UCG_PLAN_METHOD_ALLGATHER_RING) {
//        length = step->buffer_length_recv;
//    }
//    /*
//     * Short messages (e.g. RDMA "inline")
//     */
//    if (length <= phase->recv_thresh.max_short_one && is_recv_contig) {
//        /* Short send - single message */
//        step->fragments_recv = 1;
//    } else if (length <= phase->recv_thresh.max_short_max && is_recv_contig) {
//        /* Short send - multiple messages */
//        ucg_builtin_step_fragment_flags(phase->recv_thresh.max_short_one, dt_len, length,
//                                        step, phase, recv_flag);
//    /*
//     * Large messages, if supported (e.g. RDMA "zero-copy")
//     */
//    } else if ((length > phase->recv_thresh.max_bcopy_max) &&
//        (length <= phase->recv_thresh.md_attr_cap_max_reg) && is_recv_contig) {
//        if (length < phase->recv_thresh.max_zcopy_one) {
//            /* ZCopy send - single message */
//            step->fragments_recv = 1;
//        } else {
//            /* ZCopy send - multiple message */
//            ucg_builtin_step_fragment_flags(phase->recv_thresh.max_zcopy_one, dt_len, length,
//                                            step, phase, recv_flag);
//        }
//
//    /*
//     * Medium messages
//     */
//    } else if (length <= phase->recv_thresh.max_bcopy_one) {
//        /* BCopy send - single message */
//        step->fragments_recv = 1;
//    } else {
//        /* BCopy send - multiple messages */
//        if (ucs_unlikely(dt_len > phase->recv_thresh.max_bcopy_one)) {
//            phase->segmented = 1;
//            fragment_length = phase->recv_thresh.max_bcopy_one;
//        } else {
//            fragment_length = phase->recv_thresh.max_bcopy_one - (phase->recv_thresh.max_bcopy_one % dt_len);
//        }
//
//        *recv_flag = UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
//        if (phase->recv_thresh.max_bcopy_one > 0) {
//            partial_length = (length % fragment_length) > 0;
//            step->fragments_recv = length / fragment_length + partial_length;
//        } else {
//            ucs_warn("phase->recv_thresh.max_bcopy_one is negative or zero");
//            partial_length = 0;
//            step->fragments_recv = length;
//        }
//    }
//
//    return UCS_OK;
//}
//
//ucs_status_t ucg_builtin_step_create(ucg_builtin_plan_phase_t *phase,
//                                     unsigned extra_flags,
//                                     unsigned base_am_id,
//                                     ucg_group_id_t group_id,
//                                     const ucg_collective_params_t *params,
//                                     int8_t **current_data_buffer,
//                                     ucg_builtin_op_step_t *step)
//{
//    ucs_status_t status;
//    /* Set the parameters determining the send-flags later on */
//    step->buffer_length      = params->send.dt_len * params->send.count;
//    step->uct_md             = phase->md;
//    if (phase->md) {
//        step->uct_iface      = (phase->ep_cnt == 1) ? phase->single_ep->iface :
//                                                      phase->multi_eps[0]->iface;
//    }
//    /* Note: we assume all the UCT endpoints have the same interface */
//    step->phase              = phase;
//    step->am_id              = base_am_id;
//    step->am_header.group_id = group_id;
//    step->am_header.step_idx = (ucg_step_idx_t)phase->step_index;
//    step->iter_ep            = 0;
//    step->iter_offset        = 0;
//    step->fragment_pending   = NULL;
//    step->recv_buffer        = (int8_t*)params->recv.buf;
//    step->send_buffer        = ((params->send.buf == MPI_IN_PLACE) ||
//            !(extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) ?
//                    (int8_t*)params->recv.buf : (int8_t*)params->send.buf;
//    step->send_cb            = NULL;
//
//    /* special parameter of buffer length should be set for allgather with bruck plan */
//    if (phase->method == UCG_PLAN_METHOD_ALLGATHER_BRUCK) {
//        step->buf_len_unit = step->buffer_length;
//        size_t special_offset = 1UL << phase->step_index;
//        if (extra_flags == UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) {
//            step->buffer_length *= (num_procs - special_offset);
//        } else {
//            step->buffer_length *= special_offset;
//        }
//    }
//
//    /* for alltoall bruck, buffer_length should be changed! */
//    if (phase->method == UCG_PLAN_METHOD_ALLTOALL_BRUCK) {
//        step->displs_rule = UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL;
//        unsigned i, k;
//        size_t buffer_length_discrete = 0;
//        if (step->displs_rule == UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL) {
//            k = (unsigned)step->am_header.step_idx;
//            for (i = 0; i < num_procs; i++) {
//                if ((i >> k) & 1) { // kth bit is 1
//                    buffer_length_discrete++;
//                }
//            }
//        }
//
//        step->buf_len_unit   = step->buffer_length;
//        step->buffer_length *= buffer_length_discrete;
//        /* set send cb for alltoall only, should be move to proper place */
//        step->send_cb = ucg_builtin_send_alltoall;
//    }
//
//    if (phase->method != UCG_PLAN_METHOD_BCAST_WAYPOINT) {
//        if (*current_data_buffer) {
//            step->send_buffer = *current_data_buffer;
//        } else {
//            *current_data_buffer = step->recv_buffer;
//        }
//    }
//
//    if (phase->method == UCG_PLAN_METHOD_REDUCE_SCATTER_RING ||
//        phase->method == UCG_PLAN_METHOD_ALLGATHER_RING) {
//        int num_offset_blocks;
//        int send_position;
//        int recv_position;
//        int quotient = params->send.count / num_procs;
//        int remainder = params->send.count % num_procs;
//
//        step->buf_len_unit   = step->buffer_length; // for ring init
//        step->buffer_length = params->send.dt_len * quotient;
//        num_offset_blocks = (g_myidx - phase->step_index + UCG_BUILTIN_NUM_PROCS_DOUBLE * num_procs) % num_procs;
//        send_position = num_offset_blocks + 1;
//        recv_position = (num_offset_blocks - 1 + num_procs) % num_procs + 1;
//        if (recv_position <= remainder) {
//            step->buffer_length_recv = step->buffer_length + params->send.dt_len;
//        } else {
//            step->buffer_length_recv = step->buffer_length;
//        }
//        if (send_position <= remainder) {
//            step->buffer_length += params->send.dt_len;
//        }
//        step->am_header.remote_offset = params->send.dt_len * (num_offset_blocks * quotient +
//                               (num_offset_blocks <= remainder ? num_offset_blocks : remainder));
//
//        step->remote_offset = step->am_header.remote_offset;
//        step->send_buffer +=  step->am_header.remote_offset;
//    }
//
//    if (phase->method == UCG_PLAN_METHOD_ALLGATHER_RECURSIVE) {
//        size_t power = 1UL << (phase->step_index - 1);
//        size_t base_index = 0;
//        base_index = (g_myidx / power) * power;
//
//        step->am_header.remote_offset = base_index * params->send.count * params->send.dt_len;
//        /* need set the send offset if it's not the first step */
//        if (!(extra_flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) {
//            step->send_buffer += step->am_header.remote_offset;
//        }
//        step->buffer_length *= power;
//    }
//    ucs_assert(base_am_id < UCP_AM_ID_MAX);
//
//    /* Decide how the messages are sent (regardless of my role) */
//    enum ucg_builtin_op_step_flags send_flag, recv_flag;
//    recv_flag = (enum ucg_builtin_op_step_flags) 0;
//    send_flag = (enum ucg_builtin_op_step_flags) 0;
//    /* Note: in principle, step->send_buffer should not be changed after this function */
//    status = ucg_builtin_step_send_flags(step, phase, params, &send_flag);
//    extra_flags |= (send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED);
//    if (ucs_unlikely(status != UCS_OK)) {
//        return status;
//    }
//
//    /* Set the actual step-related parameters */
//    switch (phase->method) {
//        /* Send-only */
//        case UCG_PLAN_METHOD_SCATTER_TERMINAL:
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
//            /* no break */
//        case UCG_PLAN_METHOD_SEND_TERMINAL:
//            step->flags       = send_flag | extra_flags;
//            break;
//
//        /* Recv-only */
//        case UCG_PLAN_METHOD_RECV_TERMINAL:
//        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
//            step->flags       = extra_flags;
//            break;
//
//        /* Recv-all, Send-one */
//        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
//            /* no break */
//        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
//            if ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && ucg_algo.pipeline) {
//                extra_flags  |= UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
//            }
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV_BEFORE_SEND1;
//            step->flags       = send_flag | extra_flags;
//            *current_data_buffer = (int8_t*)ucs_calloc(1, step->buffer_length, "ucg_fanin_waypoint_buffer");
//            if (*current_data_buffer == NULL) {
//                return UCS_ERR_NO_MEMORY;
//            }
//            step->send_buffer = *current_data_buffer;
//            step->recv_buffer = step->send_buffer;
//
//            if (params->send.buf == MPI_IN_PLACE) {
//                memcpy(step->send_buffer, params->recv.buf, step->buffer_length);
//            } else {
//                memcpy(step->send_buffer, params->send.buf, step->buffer_length);
//            }
//
//            if (send_flag & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY) {
//                /* The send buffer changed, reregister it */
//                uct_md_mem_dereg(step->uct_md, step->zcopy.memh);
//                status = uct_md_mem_reg(step->uct_md, step->send_buffer,
//                                        step->buffer_length, UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
//                if (status != UCS_OK) {
//                    return status;
//                }
//            }
//
//            if (!step->recv_buffer) {
//                return UCS_ERR_NO_MEMORY;
//            }
//            break;
//
//        /* Recv-one, Send-all */
//        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
//            if ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && ucg_algo.pipeline) {
//                extra_flags  |= UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
//            }
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
//            step->flags       = send_flag | extra_flags;
//            break;
//
//        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
//            if ((send_flag & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED) && ucg_algo.pipeline) {
//                extra_flags  |= UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
//            }
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV1_BEFORE_SEND;
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_LENGTH_PER_REQUEST;
//            step->flags       = send_flag | extra_flags;
//            *current_data_buffer = (int8_t*)ucs_calloc(1, step->buffer_length, "ucg_fanout_waypoint_buffer");
//            if (*current_data_buffer == NULL) {
//                return UCS_ERR_NO_MEMORY;
//            }
//            step->send_buffer = *current_data_buffer;
//            step->recv_buffer = step->send_buffer;
//            if (!step->recv_buffer) {
//                return UCS_ERR_NO_MEMORY;
//            }
//            break;
//
//        /* Recursive patterns */
//        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
//        case UCG_PLAN_METHOD_ALLGATHER_RECURSIVE:
//            extra_flags      |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
//            step->flags       = send_flag | extra_flags;
//            break;
//
//        /* Bruck patterns for allgather */
//        case UCG_PLAN_METHOD_ALLGATHER_BRUCK:
//            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
//            step->flags = send_flag | extra_flags;
//            break;
//
//        /* Bruck patterns for alltoall */
//        case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
//            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
//            step->flags = send_flag | extra_flags;
//            // should malloc a new buffer to handle ucg_alltoall_step_buffer_discrete
//            step->send_buffer = (int8_t*)params->send.buf;
//            // bellow does not work
//            /* max buffer size for alltoall at every step is num_procs/2 !!!! */
//            break;
//
//        case UCG_PLAN_METHOD_REDUCE_SCATTER_RING:
//        case UCG_PLAN_METHOD_ALLGATHER_RING:
//            extra_flags |= UCG_BUILTIN_OP_STEP_FLAG_RECV_AFTER_SEND;
//            step->flags = send_flag | extra_flags;
//            break;
//
//        default:
//            ucs_error("Invalid method for a collective operation.");
//            return UCS_ERR_INVALID_PARAM;
//    }
//    status = ucg_builtin_step_recv_flags(step, phase, params, &recv_flag);
//    if (status != UCS_OK) {
//        return status;
//    }
//
//    /* fill in additional data before finishing this step */
//    if (phase->ep_cnt == 1) {
//        step->flags |= UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT;
//    }
//
//    if (step->flags & send_flag) {
//        if (phase->method != UCG_PLAN_METHOD_ALLGATHER_RECURSIVE &&
//            phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
//            phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
//            step->am_header.remote_offset = 0;
//        }
//    }
//
//    /* Pipelining preparation */
//    if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) && ucg_algo.pipeline) {
//        step->fragment_pending = (uint8_t*)UCS_ALLOC_CHECK(step->fragments *
//                sizeof(uint8_t*), "ucg_builtin_step_pipelining");
//    }
//
//    if (phase->method != UCG_PLAN_METHOD_ALLGATHER_BRUCK &&
//        phase->method != UCG_PLAN_METHOD_ALLTOALL_BRUCK &&
//        phase->method != UCG_PLAN_METHOD_REDUCE_SCATTER_RING &&
//        phase->method != UCG_PLAN_METHOD_ALLGATHER_RING) {
//        recv_flag = (enum ucg_builtin_op_step_flags)step->flags;
//        step->fragments_recv = step->fragments;
//    }
//
//    if (phase->segmented) {
//        phase->recv_cache_buffer = (int8_t *)UCS_ALLOC_CHECK(params->send.count * params->send.dt_len, "recv_cache_buffer");
//        ucs_debug("segmented phase %p fragments %" PRIu32 "", phase, step->fragments_recv);
//    } else {
//        phase->recv_cache_buffer = NULL;
//    }
//
//    /* Select the right completion callback */
//    return ucg_builtin_step_select_callbacks(phase, &step->recv_cb,
//                                             params->send.count > 0, recv_flag);
//}
