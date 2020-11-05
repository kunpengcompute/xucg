/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"

#include "../builtin.h"
#include <ucs/debug/log.h>
#include <ucs/type/status.h>
#include <ucs/profile/profile.h>
#include <ucg/base/ucg_group.h>
#include <ucg/base/ucg_collective.h>

/*
 * Below is a list of possible callback/helper functions for an incoming message.
 * Upon arrival, a message is typically copied or reduced to its collective's
 * final recieve buffer, though there are some complex collectives which are
 * handled otherwise (using intermediate buffers).
 */

static UCS_F_ALWAYS_INLINE void ucg_builtin_mpi_reduce(void *mpi_op,
        void *src, void *dst, unsigned dcount, void* mpi_datatype)
{
    UCS_PROFILE_CALL_VOID(ucg_builtin_mpi_reduce_cb, mpi_op, src, dst, dcount, mpi_datatype);
}

#define ucg_builtin_mpi_reduce_full(_req, _offset, _data, _length, _params)    \
{                                                                              \
    ucg_collective_params_t *params = _params;                                 \
    ucs_assert(length == (params->recv.count * params->recv.dt_len));          \
    ucg_builtin_mpi_reduce(params->recv.op_ext,                                \
                           _data, (_req)->step->recv_buffer + offset,          \
                           params->recv.count,  params->recv.dt_ext);          \
}

#define ucg_builtin_mpi_reduce_partial(_req, _offset, _data, _length, _params) \
{                                                                              \
    ucg_collective_params_t *params = _params;                                 \
    ucg_builtin_mpi_reduce(params->recv.op_ext,                                \
                           _data, (_req)->step->recv_buffer + offset,          \
                           length / params->recv.dt_len, params->recv.dt_ext); \
}

static UCS_F_ALWAYS_INLINE void ucg_builtin_comp_last_step_cb(ucg_builtin_request_t *req, ucs_status_t status)
{
    /* Sanity checks */
    ucs_assert(((req->comp_req->flags & UCP_REQUEST_FLAG_COMPLETED) == 0) ||
                (req->comp_req->status != UCS_OK));

    /* Mark (per-group) slot as available */
    ucg_builtin_comp_slot_t *slot = ucs_container_of(req, ucg_builtin_comp_slot_t, req);
    slot->cb = NULL;

    /*
     * For some operations, like MPI_Allgather, MPI_Alltoall, the
     * local data should be re-arranged (e.g. Bruck algorithms).
     */
    if (req->op->final_cb != NULL) {
        req->op->final_cb(req);
    }

    /* Mark request as complete */
    req->comp_req->status = status;
    req->comp_req->flags |= UCP_REQUEST_FLAG_COMPLETED;
    UCS_PROFILE_REQUEST_EVENT(req, "complete_coll", 0);
    ucs_trace_req("collective returning completed request=%p (status: %s)",
            req->comp_req, ucs_status_string(status));
}

static UCS_F_ALWAYS_INLINE ucs_status_t ucg_builtin_comp_step_cb(ucg_builtin_request_t *req,
                                                                 ucg_request_t **user_req)
{
    /* Sanity checks */
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED) {
        unsigned frag_idx;
        ucs_assert(req->step->fragment_pending != NULL);
        for (frag_idx = 0; frag_idx < req->step->fragments; frag_idx++) {
            ucs_assert(req->step->fragment_pending[frag_idx] == 0);
        }
    }

    /* Check if this is the last step */
    if (req->step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP) {
        ucs_assert(user_req == NULL); /* not directly from step_execute() */
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        return UCS_OK;
    }

    /* Mark (per-group) slot as available */
    ucg_builtin_comp_slot_t *slot = ucs_container_of(req, ucg_builtin_comp_slot_t, req);
    slot->cb = NULL;

    /* Start on the next step for this collective operation */
    ucg_builtin_op_step_t *next_step = ++req->step;
    req->pending = next_step->fragments_recv * next_step->phase->ep_cnt;
    req->recv_comp = 0;
    ucs_container_of(req, ucg_builtin_comp_slot_t, req)->step_idx =
            next_step->am_header.step_idx;
    ucs_debug("slot next step: %u",next_step->am_header.step_idx);

    return ucg_builtin_step_execute(req, user_req);
}

#define UCG_IF_LAST_MESSAGE(req) \
    ucs_assert((req)->pending > 0); if (--(req)->pending == 0)\

static UCS_F_ALWAYS_INLINE int ucg_builtin_comp_step_check_cb(ucg_builtin_request_t *req)
{
    UCG_IF_LAST_MESSAGE(req) {
        (void) ucg_builtin_comp_step_cb(req, NULL);
        return 1;
    }

    return 0;
}

static UCS_F_ALWAYS_INLINE int ucg_builtin_comp_send_check_cb(ucg_builtin_request_t *req)
{
    UCG_IF_LAST_MESSAGE(req) {
        (void) ucg_builtin_step_execute(req, NULL);
        return 1;
    }

    return 0;
}

static UCS_F_ALWAYS_INLINE int ucg_builtin_comp_send_check_frag_cb(ucg_builtin_request_t *req, uint64_t offset)
{
    ucg_builtin_op_step_t *step = req->step;
    unsigned frag_idx = offset / step->fragment_length;
    ucs_assert(step->fragment_pending[frag_idx] > 0);
    if (--step->fragment_pending[frag_idx] == 0) {
        if (ucs_unlikely(step->iter_offset == UCG_BUILTIN_OFFSET_PIPELINE_PENDING)) {
            step->fragment_pending[frag_idx] = UCG_BUILTIN_FRAG_PENDING;
        } else {
            step->iter_offset = offset;
            (void) ucg_builtin_step_execute(req, NULL);
            return 1;
        }
    }

    return step->iter_offset != UCG_BUILTIN_OFFSET_PIPELINE_READY;
}

static UCS_F_ALWAYS_INLINE void ucg_builtin_comp_zcopy_check_cb(ucg_builtin_request_t *req)
{
    uint32_t num_store = req->step->zcopy.num_store;
    if (--req->pending == num_store) {
        if (num_store == 0) {
            (void) ucg_builtin_comp_step_cb(req, NULL);
            return;
        }
        req->step->zcopy.num_store = 0;
        ucg_builtin_comp_slot_t *slot = ucs_container_of(req, ucg_builtin_comp_slot_t, req);
        (void) ucg_builtin_msg_process(slot, req);
    }
}

static int ucg_builtin_comp_recv_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->recv_buffer, data, length);
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_recv_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->recv_buffer, data, length);
    req->recv_comp = 1;
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

static int ucg_builtin_comp_recv_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->recv_buffer + offset, data, length);
    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_recv_many_then_send_pipe_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->recv_buffer + offset, data, length);
    return ucg_builtin_comp_send_check_frag_cb(req, offset);
}

static int ucg_builtin_comp_recv_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->recv_buffer + offset, data, length);
    if (req->pending == 1) {
        req->recv_comp = 1;
    }
    return ucg_builtin_comp_send_check_cb(req);
}

UCS_PROFILE_FUNC(int, ucg_builtin_comp_reduce_one_cb, (req, offset, data, length),
                 ucg_builtin_request_t *req, uint64_t offset, void *data, size_t length)
{
    ucg_builtin_mpi_reduce_full(req, offset, data, length, &req->op->super.params);
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_reduce_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    ucg_builtin_mpi_reduce_full(req, offset, data, length, &req->op->super.params);
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

UCS_PROFILE_FUNC(int, ucg_builtin_comp_reduce_many_cb, (req, offset, data, length),
                 ucg_builtin_request_t *req, uint64_t offset, void *data, size_t length)
{
    ucg_builtin_mpi_reduce_partial(req, offset, data, length, &req->op->super.params);
    return ucg_builtin_comp_step_check_cb(req);
}

UCS_PROFILE_FUNC(int, ucg_builtin_comp_reduce_full_cb, (req, offset, data, length),
                 ucg_builtin_request_t *req, uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->phase->recv_cache_buffer + offset, data, length);

    if (req->pending == 1) {
        ucg_builtin_mpi_reduce(req->op->super.params.recv.op_ext,
                            req->step->phase->recv_cache_buffer, req->step->recv_buffer,
                            req->op->super.params.recv.count,  req->op->super.params.recv.dt_ext);
    }

    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_reduce_many_then_send_pipe_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    ucg_builtin_mpi_reduce_partial(req, offset, data, length, &req->op->super.params);
    return ucg_builtin_comp_send_check_frag_cb(req, offset);
}

static int ucg_builtin_comp_reduce_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    ucg_builtin_mpi_reduce_partial(req, offset, data, length, &req->op->super.params);
    return ucg_builtin_comp_send_check_cb(req);
}

static int ucg_builtin_comp_reduce_full_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    memcpy(req->step->phase->recv_cache_buffer + offset, data, length);

    if (req->pending == 1) {
        ucg_builtin_mpi_reduce(req->op->super.params.recv.op_ext,
                            req->step->phase->recv_cache_buffer, req->step->recv_buffer,
                            req->op->super.params.recv.count,  req->op->super.params.recv.dt_ext);
    }
    return ucg_builtin_comp_send_check_cb(req);
}

static int ucg_builtin_comp_wait_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_wait_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

static int ucg_builtin_comp_wait_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_wait_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    return ucg_builtin_comp_send_check_cb(req);
}

static int ucg_builtin_comp_last_barrier_step_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    ucg_builtin_comp_last_step_cb(req, UCS_OK);
    ucg_collective_release_barrier(req->op->super.plan->group);
    return 1;
}

static int ucg_builtin_comp_last_barrier_step_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, void *data, size_t length)
{
    UCG_IF_LAST_MESSAGE(req) {
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        ucg_collective_release_barrier(req->op->super.plan->group);
        return 1;
    }
    return 0;
}

static ucs_status_t ucg_builtin_step_select_callbacks(ucg_builtin_plan_phase_t *phase,
                                               ucg_builtin_comp_recv_cb_t *recv_cb, int nonzero_length, int flags)
{
    int is_pipelined  = flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
    int is_fragmented = flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
    int is_single_ep  = flags & UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT;
    int is_last_step  = flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    int is_zcopy      = flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
    int is_segmented  = phase->segmented;
    unsigned is_single_msg = ((is_single_ep) && (!is_fragmented) && (!is_segmented));

    int is_waypoint_fanout = 0;/* special flag for waypoint bcast/scatter, only receive once */

    switch (phase->method) {
        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
            is_waypoint_fanout = 1;
            /* no break */
        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
            if (nonzero_length) {
                *recv_cb = is_fragmented ? (is_pipelined ? ucg_builtin_comp_recv_many_then_send_pipe_cb :
                                                        ucg_builtin_comp_recv_many_then_send_cb) :
                                        ucg_builtin_comp_recv_one_then_send_cb;
            } else {
                *recv_cb = ucg_builtin_comp_wait_one_then_send_cb;
            }
            break;

        case UCG_PLAN_METHOD_SEND_TERMINAL:
        case UCG_PLAN_METHOD_RECV_TERMINAL:
        case UCG_PLAN_METHOD_SCATTER_TERMINAL:
            *recv_cb = is_single_msg ? ucg_builtin_comp_recv_one_cb :
                                    ucg_builtin_comp_recv_many_cb;
            break;

        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
            is_single_msg |= ((phase->ep_cnt == 2) && (!is_fragmented));
            if (is_single_msg) {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_one_then_send_cb :
                                            ucg_builtin_comp_wait_one_then_send_cb;
            } if (is_segmented && nonzero_length){
                *recv_cb = ucg_builtin_comp_reduce_full_then_send_cb;
            } else {
                *recv_cb = nonzero_length ? (is_pipelined ? ucg_builtin_comp_reduce_many_then_send_pipe_cb :
                                                            ucg_builtin_comp_reduce_many_then_send_cb) :
                                            ucg_builtin_comp_wait_many_then_send_cb;
            }
            break;

        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
            if (is_single_msg && !is_zcopy) {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_one_cb :
                                            ucg_builtin_comp_wait_one_cb;
            } if (is_segmented && nonzero_length){
                *recv_cb = ucg_builtin_comp_reduce_full_cb;
            } else {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_many_cb :
                                            ucg_builtin_comp_wait_many_cb;
            }
            break;

        case UCG_PLAN_METHOD_ALLGATHER_BRUCK:
            *recv_cb = nonzero_length ? ucg_builtin_comp_recv_many_cb :
                                        ucg_builtin_comp_wait_many_cb;
            break;

        case UCG_PLAN_METHOD_ALLGATHER_RECURSIVE:
            *recv_cb = nonzero_length ? ucg_builtin_comp_recv_many_cb :
                                        ucg_builtin_comp_wait_many_cb;
            break;

        case UCG_PLAN_METHOD_REDUCE_SCATTER_RING:
            if (is_segmented && nonzero_length){
                *recv_cb = ucg_builtin_comp_reduce_full_cb;
            } else {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_many_cb :
                                            ucg_builtin_comp_wait_many_cb;
            }
            break;

        case UCG_PLAN_METHOD_ALLGATHER_RING:
            *recv_cb = ucg_builtin_comp_recv_many_cb;
            break;

        default:
            ucs_error("Invalid method for a collective operation.");
            return UCS_ERR_INVALID_PARAM;
    }

    /* Special case for barrier release except for waypoint fanout EPs */
    if (ucs_unlikely((!nonzero_length) && (is_last_step) && (!is_waypoint_fanout))) {
        *recv_cb = is_single_ep ? ucg_builtin_comp_last_barrier_step_one_cb :
                                  ucg_builtin_comp_last_barrier_step_many_cb;
    }

    return UCS_OK;
}

/*
 * Below is a list of possible callback functions for pretreatment before sending.
 */

/* send_cb for alltoall to sned discrete elements */
static void ucg_builtin_send_alltoall(ucg_builtin_request_t *req)
{
    unsigned i, k;
    size_t len = req->step->buf_len_unit;
    ucg_builtin_op_step_t *step = req->step;
    size_t buffer_length_discrete = 0;
    if (step->displs_rule == UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL) {
        k = (unsigned)step->am_header.step_idx;
        for (i = 0; i < ucg_builtin_num_procs; i++) {
            if ((i >> k) & 1) { //kth bit is 1
                memcpy(step->send_buffer + buffer_length_discrete * len,
                    step->recv_buffer + i * len, len);
                buffer_length_discrete++;
            }
        }
    }
}

/*
 * Below is a list of possible callback functions for operation initialization.
 */
static void ucg_builtin_init_dummy(ucg_builtin_op_t *op) {}

static void ucg_builtin_init_gather(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buffer_length;
    memcpy(step->recv_buffer + (op->super.plan->group_id * len),
            step->send_buffer, len);
}

static void ucg_builtin_init_reduce(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    if (op->super.params.send.buf == ucg_builtin_mpi_in_place) {
        memcpy(step->recv_buffer, op->super.params.recv.buf, step->buffer_length);
    } else {
        memcpy(step->recv_buffer, op->super.params.send.buf, step->buffer_length);
    }
}

static void ucg_builtin_init_ring(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buf_len_unit;
    unsigned step_idx;
    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
        (&op->steps[step_idx])->am_header.remote_offset = (&op->steps[step_idx])->remote_offset;
    }

    memcpy(step->recv_buffer, step->send_buffer - step->am_header.remote_offset, len);
}

/* for allgather, add initial step for first element storage*/
static void ucg_builtin_init_allgather(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buf_len_unit;
    memcpy(step->recv_buffer, step->send_buffer, len);
    //set offset of every step for allgather
    ucg_builtin_plan_t* builtin_plan = (ucg_builtin_plan_t*)op->super.plan;
    for (unsigned step_index = 0; step_index < builtin_plan->phs_cnt; step_index++, step++) {
        step->am_header.remote_offset = len;
        for (unsigned i = 0; i < step_index; i++) {
            size_t step_idx_offset = 1UL << i;
            step->am_header.remote_offset += step_idx_offset * len;
        }
    }
}

static void ucg_builtin_init_allgather_recursive(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t init_offset = 0;
    init_offset = op->super.plan->my_index * op->super.params.send.count *op->super.params.send.dt_len;
    memcpy(step->recv_buffer + init_offset, step->send_buffer, step->buffer_length);
}

/* for alltoall, add initial step for local rotation*/
static void ucg_builtin_init_alltoall(ucg_builtin_op_t *op)
{
    const ucg_group_params_t *params = ucg_group_get_params(op->super.plan->group);
    size_t proc_count = params->member_count;
    size_t my_index   = op->super.plan->my_index;
    ucg_builtin_op_step_t *step = &op->steps[0];
    size_t len = step->buf_len_unit;

    memcpy(step->recv_buffer, step->send_buffer + my_index * len, (proc_count - my_index)*len);

    if (my_index != 0) {
        memcpy(step->recv_buffer + (proc_count - my_index)*len, step->send_buffer, my_index*len);
    }
}



/* local shift for allgather at final step */
static void ucg_builtin_final_allgather(ucg_builtin_request_t *req)
{
    const ucg_group_params_t *params = ucg_group_get_params(req->op->super.plan->group);
    size_t num_procs_count  = params->member_count;
    size_t len = req->step->buf_len_unit;
    size_t my_index   = req->op->super.plan->my_index;
    size_t len_move = len * (num_procs_count - my_index);
    void *temp_buffer = ucs_calloc(1, len * (num_procs_count - 1), "ucg_allgather_final_step_buffer");
    ucs_assert(temp_buffer != NULL);
    if (req->op->super.plan->my_index != 0) {
        memcpy(temp_buffer, req->step->recv_buffer, len_move);
        memmove(req->step->recv_buffer, req->step->recv_buffer + len_move, len*my_index);
        memcpy(req->step->recv_buffer + len * my_index, temp_buffer, len_move);
    }
    free(temp_buffer);
    temp_buffer = NULL;
}

/* local inverse rotation for alltoall at final step */
static void ucg_builtin_final_alltoall(ucg_builtin_request_t *req)
{
    const ucg_group_params_t *params = ucg_group_get_params(req->op->super.plan->group);
    size_t num_procs_count = params->member_count;
    size_t len       = req->step->buf_len_unit;
    size_t my_index  = req->op->super.plan->my_index;

    size_t dst;
    unsigned i;
    size_t len_move = len * num_procs_count;
    int8_t *temp_buffer = (int8_t*)ucs_calloc(1, len * num_procs_count, "ucg_alltoall_final_step_buffer");
    ucs_assert(temp_buffer != NULL);
    for (i = 0; i < num_procs_count; i++) {
        dst = (my_index - i + num_procs_count) % num_procs_count;
        memcpy(temp_buffer + dst * len, req->step->recv_buffer + i * len, len);
    }
    memcpy(req->step->recv_buffer, temp_buffer, len_move);
    free(temp_buffer);
    temp_buffer = NULL;
}

static ucs_status_t ucg_builtin_op_select_callback(ucg_builtin_plan_t *plan,
                                                   ucg_builtin_op_init_cb_t *init_cb,
                                                   ucg_builtin_op_final_cb_t *final_cb)
{
    switch (plan->phss[0].method) {
        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
            *init_cb  = ucg_builtin_init_reduce;
            *final_cb = NULL;
            break;
        case UCG_PLAN_METHOD_ALLGATHER_RECURSIVE:
            *init_cb = ucg_builtin_init_allgather_recursive;
            *final_cb = NULL;
            break;
        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
            *init_cb  = ucg_builtin_init_gather;
            *final_cb = NULL;
            break;

        case UCG_PLAN_METHOD_ALLGATHER_BRUCK:
            *init_cb  = ucg_builtin_init_allgather;
            *final_cb = ucg_builtin_final_allgather;
            break;

        case UCG_PLAN_METHOD_ALLTOALL_BRUCK:
            *init_cb  = ucg_builtin_init_alltoall;
            *final_cb = ucg_builtin_final_alltoall;
            break;

        case UCG_PLAN_METHOD_REDUCE_SCATTER_RING:
        case UCG_PLAN_METHOD_ALLGATHER_RING:
            *init_cb  = ucg_builtin_init_ring;
            *final_cb = NULL;
            break;

        default:
            *init_cb  = ucg_builtin_init_dummy;
            *final_cb = NULL;
            break;
    }

    return UCS_OK;
}

static void ucg_builtin_step_am_zcopy_comp_step_check_cb(uct_completion_t *self)
{

    ucg_builtin_zcomp_t *zcomp = ucs_container_of(self, ucg_builtin_zcomp_t, comp);
    ucs_status_t status = zcomp->comp.status;
    ucg_builtin_request_t *req = zcomp->req;
    zcomp->comp.count          = 1;

    if (ucs_unlikely(status != UCS_OK)) {
        ucg_builtin_comp_last_step_cb(req, status);
    } else {
        ucg_builtin_comp_zcopy_check_cb(req);
    }
}

static inline ucs_status_t ucg_builtin_step_zcopy_prep(ucg_builtin_op_step_t *step)
{
    /* Allocate callback context for zero-copy sends */
    uint32_t zcomp_cnt         = step->phase->ep_cnt * step->fragments;
    step->zcopy.memh           = NULL; /* - in case the allocation fails... */
    step->zcopy.num_store      = 0;
    ucg_builtin_zcomp_t *zcomp =
             step->zcopy.zcomp = (ucg_builtin_zcomp_t*)UCG_ALLOC_CHECK(zcomp_cnt *
                     sizeof(*zcomp), "ucg_zcopy_completion");

    /* Initialize all the zero-copy send completion structures */
    while (zcomp_cnt--) {
        zcomp->comp.func   = ucg_builtin_step_am_zcopy_comp_step_check_cb;
        zcomp->comp.count  = 1;
        zcomp->comp.status = UCS_OK;
        zcomp++;
    }

    /* Register the buffer, creating a memory handle used in zero-copy sends */
    ucs_status_t status = uct_md_mem_reg(step->uct_md, step->send_buffer,
            step->buffer_length, UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
    if (status != UCS_OK) {
        ucs_free(step->zcopy.zcomp);
        step->zcopy.zcomp = NULL;
        return status;
    }
    return UCS_OK;
}

static ucs_status_t ucg_builtin_optimize_bcopy_to_zcopy(ucg_builtin_op_t *op)
{
    /* This function was called because we want to "upgrade" a bcopy-send to
     * zcopy, by way of memory registration (costly, but hopefully worth it) */
    ucs_status_t status;
    ucg_builtin_op_step_t *step = NULL;
    ucg_step_idx_ext_t  step_idx = 0;
    do {
        step = &op->steps[step_idx++];
        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length) &&
            step->buffer_length != 0) {
            status = ucg_builtin_step_zcopy_prep(step);
            if (status != UCS_OK) {
                goto bcopy_to_zcopy_cleanup;
            }

            step->flags &= ~UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY;
            step->flags |=  UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
            if (step->recv_cb == ucg_builtin_comp_reduce_one_cb) {
                step->recv_cb = ucg_builtin_comp_reduce_many_cb;
            }
        }
    } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    return UCS_OK;

bcopy_to_zcopy_cleanup:
    while (step_idx--) {
        step = &op->steps[step_idx];
        if (step->zcopy.zcomp != NULL) {
            ucs_free(step->zcopy.zcomp);
            step->zcopy.zcomp = NULL;
        }
    }
    return status;
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
static ucs_status_t ucg_builtin_op_consider_optimization(ucg_builtin_op_t *op,
                                                         ucg_builtin_config_t *config)
{
    ucg_builtin_op_step_t *step = NULL;
    ucg_step_idx_ext_t  step_idx = 0;
    unsigned  opt_flag = config->bcopy_to_zcopy_opt;
    do {
        step = &op->steps[step_idx++];
        if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
            (step->phase->md_attr->cap.max_reg > step->buffer_length) && opt_flag) {
            op->optm_cb = ucg_builtin_optimize_bcopy_to_zcopy;
            op->opt_cnt = config->mem_reg_opt_cnt;
            return UCS_OK;
        }
    } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));

    /* Note: This function will be called... after opt_cnt wrap-around */
    op->optm_cb = ucg_builtin_no_optimization;
    op->opt_cnt = 0;
    return UCS_OK;
}