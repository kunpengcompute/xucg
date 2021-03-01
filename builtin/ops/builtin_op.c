/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"

#include <ucp/dt/dt.inl>

/******************************************************************************
 *                                                                            *
 *                             Operation Creation                             *
 *                                                                            *
 ******************************************************************************/

static void UCS_F_ALWAYS_INLINE
ucg_builtin_init_scatter(ucg_builtin_op_t *op)
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

static void UCS_F_ALWAYS_INLINE
ucg_builtin_init_gather_terminal(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buffer_length;
    memcpy(step->recv_buffer + (UCG_PARAM_TYPE(&op->super.params).root * len),
           step->send_buffer, len);
    ucs_assert((step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED) == 0);
}

static void UCS_F_ALWAYS_INLINE
ucg_builtin_init_gather_waypoint(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    memcpy(step->recv_buffer, step->send_buffer, step->buffer_length);
    ucs_assert(step->flags & UCG_BUILTIN_OP_STEP_FLAG_TEMP_BUFFER_USED);
}

/* Alltoall Bruck phase 1/3: shuffle the data */
static void UCS_F_ALWAYS_INLINE
ucg_builtin_init_alltoall(ucg_builtin_op_t *op)
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
static void UCS_F_ALWAYS_INLINE
ucg_builtin_finalize_alltoall(ucg_builtin_op_t *op)
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

#define case_barrier(_is_init, _is_barrier, _is_reduce, _is_alltoall,          \
                     _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt,   \
                     _is_non_contig)                                           \
   case ((_is_barrier     ? UCG_BUILTIN_OP_FLAG_BARRIER         : 0) |         \
         (_is_reduce      ? UCG_BUILTIN_OP_FLAG_REDUCE          : 0) |         \
         (_is_alltoall    ? UCG_BUILTIN_OP_FLAG_ALLTOALL        : 0) |         \
         (_is_scatter     ? UCG_BUILTIN_OP_FLAG_SCATTER         : 0) |         \
         (_is_gather_term ? UCG_BUILTIN_OP_FLAG_GATHER_TERMINAL : 0) |         \
         (_is_gather_wypt ? UCG_BUILTIN_OP_FLAG_GATHER_WAYPOINT : 0) |         \
         (_is_opt         ? UCG_BUILTIN_OP_FLAG_OPTIMIZE_CB     : 0) |         \
         (_is_non_contig  ? UCG_BUILTIN_OP_FLAG_NON_CONTIGUOUS  : 0)):         \
                                                                               \
        if (_is_barrier) {                                                     \
            if (_is_init) {                                                    \
                ucg_collective_acquire_barrier(op->super.plan->group);         \
            } else {                                                           \
                ucg_collective_release_barrier(op->super.plan->group);         \
            }                                                                  \
        }                                                                      \
                                                                               \
        if (_is_reduce && _is_init) {                                          \
            ucg_builtin_op_step_t *step = &op->steps[0];                       \
            memcpy(step->recv_buffer, /* Might be a temporary buffer */        \
                   op->super.params.send.buffer,                               \
                   step->buffer_length);                                       \
        }                                                                      \
                                                                               \
        if (_is_alltoall) {                                                    \
            if (_is_init) {                                                    \
                ucg_builtin_init_alltoall(op);                                 \
            } else {                                                           \
                ucg_builtin_finalize_alltoall(op);                             \
            }                                                                  \
        }                                                                      \
                                                                               \
        if (_is_scatter && _is_init) {                                         \
            ucg_builtin_op_step_t *step = &op->steps[0];                       \
            memcpy(step->recv_buffer, /* Might be a temporary buffer */        \
                   op->super.params.send.buffer,                               \
                   step->buffer_length);                                       \
        }                                                                      \
                                                                               \
        if (_is_gather_term && _is_init) {                                     \
            ucg_builtin_init_gather_terminal(op);                              \
        }                                                                      \
                                                                               \
        if (_is_gather_wypt && _is_init) {                                     \
            ucg_builtin_init_gather_waypoint(op);                              \
        }                                                                      \
                                                                               \
        if (_is_opt && !_is_init && ucs_unlikely(--op->opt_cnt == 0)) {        \
            status = op->optm_cb(op);                                          \
        }                                                                      \
                                                                               \
        if (!_is_non_contig) {                                                 \
            break;                                                             \
        }                                                                      \
                                                                               \
        /* From now on - slow path, only for non-contiguous datatypes */       \
        is_volatile_dt = op_flags & UCG_BUILTIN_OP_FLAG_VOLATILE_DT;           \
        is_send_pack   = op_flags & UCG_BUILTIN_OP_FLAG_SEND_PACK;             \
        is_send_unpack = op_flags & UCG_BUILTIN_OP_FLAG_SEND_UNPACK;           \
        is_recv_pack   = op_flags & UCG_BUILTIN_OP_FLAG_RECV_PACK;             \
        is_recv_unpack = op_flags & UCG_BUILTIN_OP_FLAG_RECV_UNPACK;           \
                                                                               \
        if (is_volatile_dt && _is_init) {                                      \
            if (is_send_pack || is_send_unpack) {                              \
                void *dt = op->super.params.send.dtype;                        \
                if (ucg_global_params.datatype.convert_f(dt, &op->send_dt)) {  \
                    ucs_error("failed to convert external send-datatype");     \
                    status = UCS_ERR_INVALID_PARAM;                            \
                    goto op_error;                                             \
                }                                                              \
            }                                                                  \
                                                                               \
            if (is_recv_pack || is_recv_unpack) {                              \
                void *dt = op->super.params.recv.dtype;                        \
                if (ucg_global_params.datatype.convert_f(dt, &op->recv_dt)) {  \
                    ucs_error("failed to convert external receive-datatype");  \
                    status = UCS_ERR_INVALID_PARAM;                            \
                    goto op_error;                                             \
                }                                                              \
            }                                                                  \
        }                                                                      \
                                                                               \
        if (is_send_pack) {                                                    \
            ucp_dt_generic_t *dt_gen = ucp_dt_to_generic(op->send_dt);         \
            if (is_init) {                                                     \
                op->send_pack = dt_gen->ops.start_pack(dt_gen->context,        \
                                                       params->send.buffer,    \
                                                       params->send.count);    \
            } else {                                                           \
                dt_gen->ops.finish(op->send_pack);                             \
            }                                                                  \
        }                                                                      \
                                                                               \
        if (is_send_unpack) {                                                  \
            ucp_dt_generic_t *dt_gen = ucp_dt_to_generic(op->send_dt);         \
            if (is_init) {                                                     \
                op->send_unpack = dt_gen->ops.start_unpack(dt_gen->context,    \
                                                           params->send.buffer,\
                                                           params->send.count);\
            } else {                                                           \
                dt_gen->ops.finish(op->send_unpack);                           \
            }                                                                  \
        }                                                                      \
                                                                               \
        if (is_recv_pack) {                                                    \
            ucp_dt_generic_t *dt_gen = ucp_dt_to_generic(op->recv_dt);         \
            if (is_init) {                                                     \
                op->recv_pack = dt_gen->ops.start_pack(dt_gen->context,        \
                                                       params->recv.buffer,    \
                                                       params->recv.count);    \
            } else {                                                           \
                dt_gen->ops.finish(op->recv_pack);                             \
            }                                                                  \
        }                                                                      \
                                                                               \
        if (is_recv_unpack) {                                                  \
            ucp_dt_generic_t *dt_gen = ucp_dt_to_generic(op->recv_dt);         \
            if (is_init) {                                                     \
                op->recv_unpack = dt_gen->ops.start_unpack(dt_gen->context,    \
                                                           params->recv.buffer,\
                                                           params->recv.count);\
            } else {                                                           \
                dt_gen->ops.finish(op->recv_unpack);                           \
            }                                                                  \
        }                                                                      \
                                                                               \
        break;

#define  case_reduce(is_init,    _is_reduce, _is_alltoall, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
        case_barrier(is_init, 0, _is_reduce, _is_alltoall, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
        case_barrier(is_init, 1, _is_reduce, _is_alltoall, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig)

#define case_alltoall(is_init,    _is_alltoall, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
          case_reduce(is_init, 0, _is_alltoall, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
          case_reduce(is_init, 1, _is_alltoall, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig)

#define  case_scatter(is_init,    _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
        case_alltoall(is_init, 0, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
        case_alltoall(is_init, 1, _is_scatter, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig)

#define case_gather_term(is_init,    _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
            case_scatter(is_init, 0, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
            case_scatter(is_init, 1, _is_gather_term, _is_gather_wypt, _is_opt_cb, _is_non_contig)

#define case_gather_wypt(is_init,    _is_gather_wypt, _is_opt_cb, _is_non_contig) \
        case_gather_term(is_init, 0, _is_gather_wypt, _is_opt_cb, _is_non_contig) \
        case_gather_term(is_init, 1, _is_gather_wypt, _is_opt_cb, _is_non_contig)

#define      case_opt_cb(is_init,    _is_opt_cb, _is_non_contig) \
        case_gather_wypt(is_init, 0, _is_opt_cb, _is_non_contig) \
        case_gather_wypt(is_init, 1, _is_opt_cb, _is_non_contig)

#define case_non_contig(is_init,    _is_non_contig) \
            case_opt_cb(is_init, 0, _is_non_contig) \
            case_opt_cb(is_init, 1, _is_non_contig)

static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_op_do_by_flags(ucg_builtin_op_t *op, int is_init, ucg_coll_id_t coll_id)
{
    ucs_status_t status;
    int is_volatile_dt;
    int is_send_pack;
    int is_recv_pack;
    int is_send_unpack;
    int is_recv_unpack;

    uint32_t op_flags               = op->flags;
    ucg_collective_params_t *params = &op->super.params;

    switch (op_flags & UCG_BUILTIN_OP_FLAG_SWITCH_MASK) {
    case_non_contig(is_init, 0)
    case_non_contig(is_init, 1)
    }

    return UCS_OK;

op_error:
    return status;
}

static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_op_init_by_flags(ucg_builtin_op_t *op, ucg_coll_id_t coll_id)
{
    return ucg_builtin_op_do_by_flags(op, 1, coll_id);
}

void ucg_builtin_op_finalize_by_flags(ucg_builtin_op_t *op)
{
    (void) ucg_builtin_op_do_by_flags(op, 0, 0);
}


static ucs_status_t UCS_F_ALWAYS_INLINE
ucg_builtin_op_get_dt(void *dtype, size_t *dt_len_p, int *is_contig_p,
                      ucp_datatype_t *dt_p)
{

    ucp_datatype_t ucp_dt;

    if (ucs_unlikely(dtype == NULL)) {
        ucp_dt = ucp_dt_make_contig(1);
    } else {
        ucs_status_t status = ucg_global_params.datatype.convert_f(dtype,
                                                                   &ucp_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    }

    int is_dt_contig = UCP_DT_IS_CONTIG(ucp_dt);

    void *dt_state = NULL;
    ucp_dt_generic_t *dt_gen;
    if (!is_dt_contig) {
        dt_gen = ucp_dt_to_generic(ucp_dt);
        dt_state = dt_gen->ops.start_pack(dt_gen->context, NULL, 1);
    }

    *dt_len_p    = ucp_dt_length(ucp_dt, 1, NULL, dt_state);
    *is_contig_p = is_dt_contig;
    *dt_p        = ucp_dt;

    if (!is_dt_contig) {
        dt_gen->ops.finish(dt_state);
    }

    return UCS_OK;
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
    size_t send_dt_len                   = 0;
    size_t recv_dt_len                   = 0;
    op->flags                            = 0;
    op->send_dt                          = 0;
    op->recv_dt                          = 0;
    op->super.reduce_full_f              = NULL;
    op->super.reduce_frag_f              = NULL;

    /* obtain UCX datatypes corresponding to the extenral datatypes passed */
    if (params->send.count > 0) {
        status = ucg_builtin_op_get_dt(params->send.dtype, &send_dt_len,
                                       &is_send_dt_contig, &op->send_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    }

    if (params->recv.count > 0) {
        status = ucg_builtin_op_get_dt(params->recv.dtype, &recv_dt_len,
                                       &is_recv_dt_contig, &op->recv_dt);
        if (ucs_unlikely(status != UCS_OK)) {
            return status;
        }
    }

    /* copy the parameters aside, and use those from now on */
    memcpy(&op->super.params, params, sizeof(*params));
    params = &op->super.params;
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
                                         &op->super.reduce_full_f,
                                         &op->super.reduce_frag_f,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, recv_dt_len, &op->flags,
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
                                         &op->super.reduce_full_f,
                                         &op->super.reduce_frag_f,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, recv_dt_len, &op->flags,
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
                                             &op->super.reduce_full_f,
                                             &op->super.reduce_frag_f,
                                             is_send_dt_contig, is_recv_dt_contig,
                                             send_dt_len, recv_dt_len, &op->flags,
                                             ++next_step, &zcopy_step_skip);
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
                                         &op->super.reduce_full_f,
                                         &op->super.reduce_frag_f,
                                         is_send_dt_contig, is_recv_dt_contig,
                                         send_dt_len, recv_dt_len, &op->flags,
                                         ++next_step, &zcopy_step_skip);
        if ((status == UCS_OK) && ucs_unlikely(zcopy_step_skip)) {
            status = ucg_builtin_step_create_rkey_bcast(builtin_plan,
                                                        params,
                                                        next_step);
        }
    }
    if (ucs_unlikely(status != UCS_OK)) {
        goto op_cleanup;
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
    op->super.compreq_f = ucg_global_params.completion.coll_comp_cb_f;
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

    ucs_status_t status = ucg_builtin_op_init_by_flags(builtin_op, coll_id);
    if (ucs_unlikely(status != UCS_OK)) {
        return status;
    }

    /* Start the first step, which may actually complete the entire operation */
    header.msg.coll_id = coll_id;
    return ucg_builtin_step_execute(builtin_req, header);
}
