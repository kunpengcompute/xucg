/*
 * Copyright (C) Huawei Technologies Co., Ltd. 2019-2021.  ALL RIGHTS RESERVED.
 * See file LICENSE for terms.
 */

#include "builtin_ops.h"

#include <ucp/dt/dt.h>
#include <ucp/core/ucp_ep.inl>
#include <ucs/debug/log.h>
#include <ucs/type/status.h>
#include <ucs/profile/profile.h>
#include <limits.h>

/*
 * Below is a list of possible callback/helper functions for an incoming message.
 * Upon arrival, a message is typically copied or reduced to its collective's
 * final receieve buffer, though there are some complex collectives which are
 * handled otherwise (using intermediate buffers).
 */

mpi_reduce_f ucg_builtin_mpi_reduce_cb;
static UCS_F_ALWAYS_INLINE void ucg_builtin_mpi_reduce(void *mpi_op,
        const void *src, void *dst, unsigned dcount, void* mpi_datatype)
{
    UCS_PROFILE_CALL_VOID(ucg_builtin_mpi_reduce_cb, mpi_op, (char*)src,
            (char*)dst, dcount, mpi_datatype);
}

void ucg_builtin_mpi_reduce_full(ucg_builtin_request_t *req, size_t offset, const void *data,
                                 size_t length, ucg_collective_params_t *params)
{
    ucp_dt_generic_t *gen_dt = req->op->recv_dt;
    size_t dt_len = params->recv.dt_len;
    void *gen_state = NULL;
    char *reduce_buf = NULL;
    ptrdiff_t dsize = 0;
    ptrdiff_t gap = 0;

    if (dt_len == 0 || length == 0) {
        return;
    }

    if (gen_dt != NULL) {
        dt_len = ucg_builtin_get_dt_len(gen_dt);
        if (dt_len == 0) {
            ucs_info("mpi_reduce_full, dt_len is 0");
            return;
        }
        dsize = req->op->dtspan_f(params->recv.dt_ext, params->recv.count, &gap);
        reduce_buf = (char *)ucs_malloc(dsize, "temp full reduce buffer");
        if (reduce_buf == NULL) {
            ucs_fatal("no memory for malloc, dsize:%lu", dsize);
        }
        gen_state = gen_dt->ops.start_unpack(gen_dt->context, reduce_buf - gap, params->recv.count);
        gen_dt->ops.unpack(gen_state, 0, data, length);
        gen_dt->ops.finish(gen_state);
        data = reduce_buf - gap;
        offset = (offset / dt_len) * params->recv.dt_len;
    }

    ucs_assert(length == (params->recv.count * dt_len));
    ucs_debug("mpi_reduce_full, data:%p, length:%lu, recv_buffer:%p, offset:%lu, dt_len:%lu",
              data, length, req->step->recv_buffer, offset, dt_len);
    ucg_builtin_mpi_reduce(params->recv.op_ext, data, req->step->recv_buffer + offset,
                           params->recv.count, params->recv.dt_ext);

    if (reduce_buf != NULL) {
        ucs_free(reduce_buf);
    }
}

void ucg_builtin_mpi_reduce_partial(ucg_builtin_request_t *req, size_t offset, const void *data,
                                    size_t length, ucg_collective_params_t *params)
{
    ucp_dt_generic_t *gen_dt = req->op->recv_dt;
    size_t dt_len = params->recv.dt_len;
    void *gen_state = NULL;
    char *reduce_buf = NULL;
    ptrdiff_t dsize = 0;
    ptrdiff_t gap = 0;
    size_t count;

    if (dt_len == 0 || length == 0) {
        return;
    }

    if (gen_dt != NULL) {
        dt_len = ucg_builtin_get_dt_len(gen_dt);
        if (dt_len == 0) {
            ucs_info("mpi_reduce_partial, dt_len is 0");
            return;
        }
        count = length / dt_len;
        dsize = req->op->dtspan_f(params->recv.dt_ext, count, &gap);
        reduce_buf = (char *)ucs_malloc(dsize, "temp partial reduce buffer");
        if (reduce_buf == NULL) {
            ucs_fatal("no memory for malloc, dsize:%lu", dsize);
        }
        gen_state = gen_dt->ops.start_unpack(gen_dt->context, reduce_buf - gap, count);
        gen_dt->ops.unpack(gen_state, 0, data, length);
        gen_dt->ops.finish(gen_state);
        data = reduce_buf - gap;
        offset = (offset / dt_len) * params->recv.dt_len;
        ucg_builtin_mpi_reduce(params->recv.op_ext, data, req->step->recv_buffer + offset, length / dt_len,
            params->recv.dt_ext);

        if (reduce_buf != NULL) {
            ucs_free(reduce_buf);
        }
        return;
    }

    /* only the tree algo need the reduce data to be buffered */
    if (req->step->phase->method != UCG_PLAN_METHOD_REDUCE_TERMINAL
        && req->step->phase->method != UCG_PLAN_METHOD_REDUCE_WAYPOINT) {
        ucs_debug("mpi_reduce_partial, data:%p, length:%lu, recv_buffer:%p, offset:%lu, dt_len:%lu",
                  data, length, req->step->recv_buffer, offset, dt_len);
        ucg_builtin_mpi_reduce(params->recv.op_ext, data, req->step->recv_buffer + offset,
                               length / dt_len, params->recv.dt_ext);
        return;
    }

    /* fragmented message not support reduce data buffer */
    if (req->step->reduce_buff == NULL) {
        ucg_builtin_mpi_reduce(params->recv.op_ext, data, req->step->recv_buffer + offset,
                           length / dt_len, params->recv.dt_ext);
        return;
    }

    if (offset > req->step->rbuf_count) {
        ucs_error("Illegal offset:%lu, method:%u", offset, (int)req->step->phase->method);
        return;
    }

    int tLen = offset * length;
    memcpy((char *)req->step->reduce_buff + tLen, data, length);
    if (req->pending > 1) {
        return;
    }
    uint32_t loop = 0;
    while (loop < req->step->rbuf_count) {
        ucg_builtin_mpi_reduce(params->recv.op_ext, (char *)req->step->reduce_buff + loop * length,
                               req->step->recv_buffer, length / dt_len, params->recv.dt_ext);
        loop++;
    }
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
    req->is_send_cb_called = 0;
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

#if ENABLE_UCG_HICOLL
static int ucg_builtin_inc_comp_recv_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    int ret = 1;
    ucs_status_t status = inc_comp_recv_one(req, offset, data, length);
    if (status != UCS_OK) {
        ret = 0;
    }
    (void)ucg_builtin_comp_step_cb(req, NULL);
    return ret;
}

static int ucg_builtin_inc_comp_recv_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    ucs_status_t status = inc_comp_recv_many(req, offset, data, length);
    if (status != UCS_OK) {
        return 0;
    }
    return ucg_builtin_comp_step_check_cb(req);
}
#endif

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
    uint64_t offset, const void *data, size_t length)
{
    memcpy(req->step->recv_buffer, data, length);
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

/* recv_cb will parse the rank and "actual" data */
static int ucg_builtin_comp_recv_var_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    /* the first 64bytes data(payload) is the soure rank */
    ucg_group_member_index_t src_rank = *((ucg_group_member_index_t *)data);

    size_t recv_dt_len = req->op->super.params.recv.dt_len;
    ucg_builtin_coll_params_t *recv_coll_params = req->step->recv_coll_params;

    int64_t recv_buffer_displ = recv_coll_params->displs[src_rank] * recv_dt_len;
    int8_t *recv_buffer = recv_coll_params->init_buf + recv_buffer_displ + offset;
    memcpy(recv_buffer, (int8_t *)data + sizeof(src_rank), length - sizeof(src_rank));
    (void)ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_recv_noncontig_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    req->op->recv_dt->ops.unpack(req->step->non_contig.unpack_state,
                                 offset, data, length);
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_recv_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    memcpy(req->step->recv_buffer, data, length);
    req->recv_comp = 1;
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

static int ucg_builtin_comp_recv_noncontig_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    req->op->recv_dt->ops.unpack(req->step->non_contig.unpack_state,
                                 offset, data, length);
    req->recv_comp = 1;
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

static int ucg_builtin_comp_recv_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    memcpy(req->step->recv_buffer + offset, data, length);
    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_recv_noncontig_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    req->op->recv_dt->ops.unpack(req->step->non_contig.unpack_state,
                                 offset, data, length);
    return ucg_builtin_comp_step_check_cb(req);
}

/* recv_cb will parse the rank and "actual" data */
static int ucg_builtin_comp_recv_var_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    /* the first 64bytes data(payload) is the soure rank */
    ucg_group_member_index_t src_rank = *((ucg_group_member_index_t *)data);

    size_t recv_dt_len = req->op->super.params.recv.dt_len;
    ucg_builtin_coll_params_t *recv_coll_params = req->step->recv_coll_params;

    int64_t recv_buffer_displ = recv_coll_params->displs[src_rank] * recv_dt_len;
    int8_t *recv_buffer = recv_coll_params->init_buf + recv_buffer_displ + offset;
    memcpy(recv_buffer, (int8_t *)data + sizeof(src_rank), length - sizeof(src_rank));
    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_recv_many_then_send_pipe_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    memcpy(req->step->recv_buffer + offset, data, length);
    return ucg_builtin_comp_send_check_frag_cb(req, offset);
}

static int ucg_builtin_comp_recv_noncontig_many_then_send_pipe_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    req->op->recv_dt->ops.unpack(req->step->non_contig.unpack_state,
                                 offset, data, length);
    return ucg_builtin_comp_send_check_frag_cb(req, offset);
}

static int ucg_builtin_comp_recv_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    memcpy(req->step->recv_buffer + offset, data, length);
    if (req->pending == 1) {
        req->recv_comp = 1;
    }
    return ucg_builtin_comp_send_check_cb(req);
}

static int ucg_builtin_comp_recv_noncontig_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    req->op->recv_dt->ops.unpack(req->step->non_contig.unpack_state,
                                 offset, data, length);
    if (req->pending == 1) {
        req->recv_comp = 1;
    }
    return ucg_builtin_comp_send_check_cb(req);
}

UCS_PROFILE_FUNC(int, ucg_builtin_comp_reduce_one_cb, (req, offset, data, length),
                 ucg_builtin_request_t *req, uint64_t offset, const void *data, size_t length)
{
    ucg_builtin_mpi_reduce_full(req, offset, data, length, &req->op->super.params);
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_reduce_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    ucg_builtin_mpi_reduce_full(req, offset, data, length, &req->op->super.params);
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

UCS_PROFILE_FUNC(int, ucg_builtin_comp_reduce_many_cb, (req, offset, data, length),
                 ucg_builtin_request_t *req, uint64_t offset, const void *data, size_t length)
{
    ucg_builtin_mpi_reduce_partial(req, offset, data, length, &req->op->super.params);
    return ucg_builtin_comp_step_check_cb(req);
}

UCS_PROFILE_FUNC(int, ucg_builtin_comp_reduce_full_cb, (req, offset, data, length),
                 ucg_builtin_request_t *req, uint64_t offset, const void *data, size_t length)
{
    ucs_debug("comp_reduce_full_cb, data:%p, length:%lu, offset:%lu", data, length, offset);
    memcpy(req->step->phase->recv_cache_buffer + offset, data, length);

    if (req->pending == 1) {
        char *tmp_buffer = NULL;
        char *netdata = (char *)req->step->phase->recv_cache_buffer;
        ucp_dt_generic_t *gen_dt = req->op->recv_dt;
        void *state_pack = req->step->non_contig.pack_state_recv;
        void *state_unpack = req->step->non_contig.unpack_state;
        ucg_collective_params_t *params = &req->op->super.params;
        size_t dt_len = (gen_dt == NULL) ? params->recv.dt_len :
                        ucg_builtin_get_dt_len(gen_dt);
        size_t total_length = params->recv.count * dt_len;

        if (gen_dt != NULL && req->step->phase->is_swap) {
            tmp_buffer = (char *)ucs_malloc(total_length, "temp swap buffer");
            if (tmp_buffer == NULL) {
                ucs_fatal("no memory for malloc, total_length:%lu", total_length);
            }

            memcpy(tmp_buffer, netdata, total_length);
            gen_dt->ops.pack(state_pack, 0, netdata, total_length);
            gen_dt->ops.unpack(state_unpack, 0, tmp_buffer, total_length);
            ucs_free(tmp_buffer);
        }

        ucg_builtin_mpi_reduce_full(req, 0, netdata, total_length, params);
    }

    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_reduce_many_then_send_pipe_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    ucg_builtin_mpi_reduce_partial(req, offset, data, length, &req->op->super.params);
    return ucg_builtin_comp_send_check_frag_cb(req, offset);
}

static int ucg_builtin_comp_reduce_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    ucg_builtin_mpi_reduce_partial(req, offset, data, length, &req->op->super.params);
    return ucg_builtin_comp_send_check_cb(req);
}

static int ucg_builtin_comp_reduce_full_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
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
    uint64_t offset, const void *data, size_t length)
{
    (void) ucg_builtin_comp_step_cb(req, NULL);
    return 1;
}

static int ucg_builtin_comp_wait_one_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    (void) ucg_builtin_step_execute(req, NULL);
    return 1;
}

static int ucg_builtin_comp_wait_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    return ucg_builtin_comp_step_check_cb(req);
}

static int ucg_builtin_comp_wait_many_then_send_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    return ucg_builtin_comp_send_check_cb(req);
}

static int ucg_builtin_comp_last_barrier_step_one_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    ucg_builtin_comp_last_step_cb(req, UCS_OK);
    ucg_collective_release_barrier(req->op->super.plan->group);
    return 1;
}

static int ucg_builtin_comp_last_barrier_step_many_cb(ucg_builtin_request_t *req,
    uint64_t offset, const void *data, size_t length)
{
    UCG_IF_LAST_MESSAGE(req) {
        ucg_builtin_comp_last_step_cb(req, UCS_OK);
        ucg_collective_release_barrier(req->op->super.plan->group);
        return 1;
    }
    return 0;
}

/* For variable-length buffers, the value is calculated based on the pending value. */
void ucg_builtin_step_var_callbacks(unsigned pending, ucg_builtin_comp_recv_cb_t *recv_cb)
{
    *recv_cb = (pending == 1 ? ucg_builtin_comp_recv_var_one_cb : ucg_builtin_comp_recv_var_many_cb);
}

static ucs_status_t ucg_builtin_step_select_callbacks(ucg_builtin_plan_phase_t *phase, int is_contig_recv,
                                               ucg_builtin_comp_recv_cb_t *recv_cb, int nonzero_length, int flags)
{
    int is_pipelined  = flags & UCG_BUILTIN_OP_STEP_FLAG_PIPELINED;
    int is_fragmented = flags & UCG_BUILTIN_OP_STEP_FLAG_FRAGMENTED;
    int is_single_ep  = flags & UCG_BUILTIN_OP_STEP_FLAG_SINGLE_ENDPOINT;
    int is_last_step  = flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP;
    int is_zcopy      = flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_ZCOPY;
    int is_segmented  = phase->segmented;
    int is_partial    = phase->ex_attr.is_partial;
    unsigned is_single_msg = (is_single_ep && (!is_fragmented) && (!is_segmented) && (!is_partial));
    int is_waypoint_fanout = 0;/* special flag for waypoint bcast/scatter, only receive once */
    const int cnt_num = 2;

    ucs_debug("step select callback, method:%d, flags:0x%x, is_segmented:%d, nonzero_length:%d, recv_contig:%d",
              phase->method, flags, is_segmented, nonzero_length, is_contig_recv);

    switch (phase->method) {
        case UCG_PLAN_METHOD_BCAST_WAYPOINT:
        case UCG_PLAN_METHOD_SCATTER_WAYPOINT:
            is_waypoint_fanout = 1;
            /* no break */
        case UCG_PLAN_METHOD_GATHER_WAYPOINT:
            if (!is_contig_recv) {
                if (nonzero_length) {
                    *recv_cb = is_fragmented ? (is_pipelined ? ucg_builtin_comp_recv_noncontig_many_then_send_pipe_cb :
                                                ucg_builtin_comp_recv_noncontig_many_then_send_cb) :
                                                ucg_builtin_comp_recv_noncontig_one_then_send_cb;
                } else {
                    *recv_cb = ucg_builtin_comp_wait_one_then_send_cb;
                }
            } else if (nonzero_length) {
                *recv_cb = is_fragmented ? (is_pipelined ? ucg_builtin_comp_recv_many_then_send_pipe_cb :
                                            ucg_builtin_comp_recv_many_then_send_cb) :
                                            ucg_builtin_comp_recv_one_then_send_cb;
            } else {
                *recv_cb = ucg_builtin_comp_wait_one_then_send_cb;
            }
            break;

        case UCG_PLAN_METHOD_SEND_TERMINAL:
        case UCG_PLAN_METHOD_RECV_TERMINAL:
            if (!is_contig_recv) {
                *recv_cb = is_single_msg ? ucg_builtin_comp_recv_noncontig_one_cb :
                                           ucg_builtin_comp_recv_noncontig_many_cb;
                break;
            }
        case UCG_PLAN_METHOD_SCATTER_TERMINAL:
            *recv_cb = is_single_msg ? ucg_builtin_comp_recv_one_cb :
                                    ucg_builtin_comp_recv_many_cb;
            break;

        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
            is_single_msg |= ((phase->ep_cnt == cnt_num) && (!is_fragmented));
            if (is_single_msg) {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_one_then_send_cb :
                                            ucg_builtin_comp_wait_one_then_send_cb;
            }
            if (is_segmented && nonzero_length){
                *recv_cb = ucg_builtin_comp_reduce_full_then_send_cb;
            } else {
                *recv_cb = nonzero_length ? (is_pipelined ? ucg_builtin_comp_reduce_many_then_send_pipe_cb :
                                                            ucg_builtin_comp_reduce_many_then_send_cb) :
                                            ucg_builtin_comp_wait_many_then_send_cb;
            }
            break;

        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
        case UCG_PLAN_METHOD_REDUCE_SCATTER_RECURSIVE:
            if (is_single_msg && !is_zcopy) {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_one_cb :
                                            ucg_builtin_comp_wait_one_cb;
            } else if (is_segmented && nonzero_length) {
                *recv_cb = ucg_builtin_comp_reduce_full_cb;
            } else {
                *recv_cb = nonzero_length ? ucg_builtin_comp_reduce_many_cb :
                                            ucg_builtin_comp_wait_many_cb;
            }
            break;
#if ENABLE_UCG_HICOLL
        case UCG_PLAN_METHOD_INC:
            if (is_single_msg && !is_zcopy){
                *recv_cb = nonzero_length ? ucg_builtin_inc_comp_recv_one_cb :
                                            ucg_builtin_comp_wait_one_cb;
            } else {
                *recv_cb = nonzero_length ? ucg_builtin_inc_comp_recv_many_cb :
                                            ucg_builtin_comp_wait_many_cb;
            }
            break;
#endif
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

        case UCG_PLAN_METHOD_EXCHANGE:
            if (is_single_msg && !is_zcopy){
                *recv_cb = nonzero_length ? ucg_builtin_comp_recv_one_cb :
                                            ucg_builtin_comp_wait_one_cb;
            } else {
                *recv_cb = nonzero_length ? ucg_builtin_comp_recv_many_cb :
                                            ucg_builtin_comp_wait_many_cb;
            }
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

/* send_cb for INC */
void ucg_builtin_send_inc(ucg_builtin_request_t *req)
{
#if ENABLE_UCG_HICOLL
    inc_send_cb(req);
#endif
}

/* send_cb for alltoall to send discrete elements */
static void ucg_builtin_send_alltoall(ucg_builtin_request_t *req)
{
    unsigned i, k;
    size_t len = req->step->buf_len_unit;
    ucg_builtin_op_step_t *step = req->step;
    size_t buffer_length_discrete = 0;
    if (step->displs_rule == UCG_BUILTIN_OP_STEP_DISPLS_RULE_BRUCK_ALLTOALL) {
        k = (unsigned)step->am_header.step_idx;
        for (i = 0; i < num_procs; i++) {
            if ((i >> k) & 1) { //kth bit is 1
                memcpy(step->send_buffer + buffer_length_discrete * len,
                    step->recv_buffer + i * len, len);
                buffer_length_discrete++;
            }
        }
    }
}

/* To support the negative offset, the maximum memory size is 2GBytes. */
ucs_status_t ucg_builtin_plummer_check_data_size(size_t dtype_size, int count)
{
    uint64_t total_size = dtype_size * count;
    if (total_size > INT_MAX) {
        ucs_error("The buffer limit supported by the alltoallv plummer algorithm is exceeded.");
        return UCS_ERR_OUT_OF_RANGE;
    }
    return UCS_OK;
}

ucs_status_t ucg_builtin_plummer_check_overflow(int lvalue, int rvalue)
{
    if (INT_MAX - lvalue >= rvalue) {
        return UCS_OK;
    } else {
        ucs_error("The buffer limit supported by the alltoallv plummer algorithm is exceeded.");
        return UCS_ERR_OUT_OF_RANGE;
    }
}

#define PLUMMER_CHECK_DATA_SIZE(dtype_size, count)  do  {  \
    status = ucg_builtin_plummer_check_data_size(dtype_size, count);  \
    if (status != UCS_OK) {   \
        req->plummer_req_status = status;  \
        return;  \
    }  \
} while (0)

#define PLUMMER_CHECK_OVERFLOW(left, right)  do  {  \
    status = ucg_builtin_plummer_check_overflow(left, right);  \
    if (status != UCS_OK) {   \
        req->plummer_req_status = status;  \
        return;  \
    }  \
} while (0)

void ucg_builtin_plummer_gather_send_counts_cb(ucg_builtin_request_t *req)
{
    ucg_builtin_op_step_t *step = req->step;
    if (step->phase->ex_attr.is_node_leader) {
        unsigned ppn = step->phase->ex_attr.ppn;
        size_t buffer_size = ppn * step->buf_len_unit;
        step->recv_buffer = (int8_t *)ucs_malloc(buffer_size, "allocate gather send counts buffers");
        if (step->recv_buffer == NULL) {
            ucs_fatal("no memory for malloc, buffer_size: %lu", buffer_size);
        }
        unsigned local_index = step->phase->ex_attr.recv_start_block;
        memcpy(step->recv_buffer + local_index * step->buf_len_unit, step->send_buffer, step->buf_len_unit);
        req->op->temp_data_buffer = step->recv_buffer; /* Save for future use */
        /* single ep remote offset = 0 */
        if ( step->phase->ep_cnt == 1) {
            step->recv_buffer += step->buf_len_unit;
        }
    }
}

void ucg_builtin_plummer_gather_send_buffers_cb(ucg_builtin_request_t *req)
{
    ucs_status_t status = UCS_OK;
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_coll_params_t *recv_coll_params = step->recv_coll_params;
    ucg_builtin_coll_params_t *send_coll_params = step->send_coll_params;
    size_t dt_len = params->send.dt_len;
    unsigned member_cnt = phase->ex_attr.member_cnt;
    unsigned ppn = phase->ex_attr.ppn;

    /*initialize step recv coll parameters */
    if (step->phase->ex_attr.is_node_leader) {
        int *temp_send_counts = (int *)req->op->temp_data_buffer;
        int8_t *init_send_buf = params->send.buf == MPI_IN_PLACE ? (int8_t *)params->recv.buf
                                                                  : (int8_t *)params->send.buf;
        unsigned i, j, k;
        for (i =0; i < ppn; i++) {
            k = i * member_cnt;
            for (j =0; j < member_cnt; j++) {
                recv_coll_params->counts[i] += temp_send_counts[k++];
            }
        }

        for (i =0; i < (ppn-1); i++) {
            PLUMMER_CHECK_OVERFLOW(recv_coll_params->displs[i], recv_coll_params->counts[i]);
            recv_coll_params->displs[i+1] = recv_coll_params->displs[i] + recv_coll_params->counts[i];
        }

        int total_recv_count = recv_coll_params->counts[ppn-1] + recv_coll_params->displs[ppn-1];
        PLUMMER_CHECK_DATA_SIZE(dt_len, total_recv_count);

        size_t total_recv_buffer = total_recv_count * dt_len;
        req->op->temp_exchange_buffer = (int8_t *)ucs_malloc(total_recv_buffer, "allocate send buffer");
        if (req->op->temp_exchange_buffer== NULL) {
            ucs_fatal("no memory for malloc, total_recv_buffer: %lu", total_recv_buffer);
        }
        recv_coll_params->init_buf = req->op->temp_exchange_buffer;

        memcpy(recv_coll_params->init_buf, init_send_buf,
               dt_len * (params->send.counts[member_cnt-1] + params->send.displs[member_cnt-1]));

    } else {
        send_coll_params->init_buf = params->send.buf == MPI_IN_PLACE ? (int8_t *)params->recv.buf
                                                                  : (int8_t *)params->send.buf;

        send_coll_params->counts[0] = params->send.counts[member_cnt-1] + params->send.displs[member_cnt-1];
        send_coll_params->displs[0] = 0;
        /* initialize step other parameters */
        step->send_buffer = send_coll_params->init_buf;
        step->buffer_length = send_coll_params->counts[0] * dt_len;
        status = ucg_builtin_step_alloc_pack_rank_buffer(step, send_coll_params->counts[0] * dt_len);
        if (status!= UCS_OK) {
            req->plummer_req_status = status;
        }
    }
}

void ucg_builtin_plummer_gather_recv_counts_cb(ucg_builtin_request_t *req)
{
    ucg_builtin_op_step_t *step = req->step;
    if (step->phase->ex_attr.is_node_leader) {
        unsigned ppn = step->phase->ex_attr.ppn;
        size_t buffer_size = ppn * step->buf_len_unit;
        step->recv_buffer = (int8_t *)ucs_malloc(buffer_size, "allocate gather send counts buffers");
        if (step->recv_buffer == NULL) {
            ucs_fatal("no memory for malloc, buffer_size: %lu", buffer_size);
        }
        unsigned local_index = step->phase->ex_attr.recv_start_block;
        memcpy(step->recv_buffer + local_index * step->buf_len_unit, step->send_buffer, step->buf_len_unit);
        req->op->temp_data_buffer1 = step->recv_buffer; /* Save the buffer for future use. */
        /* single ep remote offset = 0 */
        if ( step->phase->ep_cnt == 1) {
            step->recv_buffer += step->buf_len_unit;
        }
    }
}

void ucg_builtin_plummer_inter_alltoallv_cb(ucg_builtin_request_t *req)
{
    ucs_status_t status = UCS_OK;
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_op_t *op = req->op;
    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_coll_params_t *recv_coll_params = step->recv_coll_params;
    ucg_builtin_coll_params_t *send_coll_params = step->send_coll_params;
    size_t send_dt_len = params->send.dt_len;
    unsigned node_cnt = phase->ex_attr.member_cnt;
    unsigned ppn = phase->ex_attr.ppn;

    /* init send and recv counts */
    int *temp_send_counts = (int *)req->op->temp_data_buffer;
    int *temp_recv_counts = (int *)req->op->temp_data_buffer1;

    unsigned i, j, k;
    unsigned counter = 0;
    for (i = 0; i < ppn; i++) {
        for (j = 0; j < node_cnt; j++) {
            for(k = 0; k < ppn; k++) {
                send_coll_params->counts[j] += temp_send_counts[counter];
                recv_coll_params->counts[j] += temp_recv_counts[counter];
                counter++;
            }
        }
    }

    /* init send and recv displs */
    for (j = 0; j < (node_cnt-1); j++) {
        PLUMMER_CHECK_OVERFLOW(send_coll_params->counts[j], send_coll_params->displs[j]);
        PLUMMER_CHECK_OVERFLOW(recv_coll_params->counts[j], recv_coll_params->displs[j]);
        send_coll_params->displs[j+1] = send_coll_params->counts[j] + send_coll_params->displs[j];
        recv_coll_params->displs[j+1] = recv_coll_params->counts[j] + recv_coll_params->displs[j];
    }

    /* init send and recv buffer, memory redistribution */
    int *temp_send_displs = (int *)ucs_malloc(counter * sizeof(int), "allocate temp displs");
    if (temp_send_displs == NULL) {
        ucs_fatal("no memory for malloc, counter_size: %lu", counter * sizeof(int));
    }
    memset(temp_send_displs, 0, counter * sizeof(int));

    for (i = 0; i < (ppn*node_cnt*ppn-1); i++) {
        temp_send_displs[i+1] =  temp_send_displs[i] + temp_send_counts[i];
    }

    status = ucg_builtin_plummer_check_data_size(send_dt_len,
        send_coll_params->counts[node_cnt-1]+send_coll_params->displs[node_cnt-1]);
    if (status != UCS_OK) {
        req->plummer_req_status = status;
        ucg_builtin_free((void **)&temp_send_displs);
        return;
    }


    size_t send_buf_size = (send_coll_params->counts[node_cnt-1] + send_coll_params->displs[node_cnt-1]) * send_dt_len;
    send_coll_params->init_buf = (int8_t *)ucs_malloc(send_buf_size, "allocate init buffer");
    if (send_coll_params->init_buf == NULL) {
        ucg_builtin_free((void **)&temp_send_displs);
        ucs_fatal("no memory for malloc, send_buf_size: %lu", send_buf_size);
    }

    int8_t *temp_init_buf = send_coll_params->init_buf;

    unsigned count, disp, idx;
    for (j = 0; j < node_cnt; j++) {
        for (k = 0; k < ppn; k++) {
            for (i = 0; i < ppn; i++) {
                idx = i * ppn * node_cnt + j * ppn + k;
                count = temp_send_counts[idx] * send_dt_len;
                disp = temp_send_displs[idx] * send_dt_len;
                if (count > 0) {
                    memcpy(temp_init_buf, req->op->temp_exchange_buffer+disp, count);
                    temp_init_buf += count;
                }
            }
        }
    }
    ucg_builtin_free((void **)&temp_send_displs);
    PLUMMER_CHECK_DATA_SIZE(send_dt_len, (recv_coll_params->counts[node_cnt-1]+recv_coll_params->displs[node_cnt-1]));

    size_t recv_buf_size = (recv_coll_params->counts[node_cnt-1] + recv_coll_params->displs[node_cnt-1]) * send_dt_len;
    recv_coll_params->init_buf = (int8_t *)ucs_malloc(recv_buf_size, "allocate init buffer");
    if (recv_coll_params->init_buf == NULL) {
        ucg_builtin_free((void **)&send_coll_params->init_buf);
        ucs_fatal("no memory for malloc, recv_buf_size: %lu", recv_buf_size);
    }
    /* copy to myself */
    unsigned local_index = phase->ex_attr.packed_rank;
    memcpy(recv_coll_params->init_buf+recv_coll_params->displs[local_index]*send_dt_len,
           send_coll_params->init_buf+send_coll_params->displs[local_index]*send_dt_len,
           send_coll_params->counts[local_index]*send_dt_len);

    /* release old buffers, use redistribute buffer */
    ucg_builtin_free((void **)&op->temp_exchange_buffer);
    op->temp_exchange_buffer = send_coll_params->init_buf;
    op->temp_exchange_buffer1 = recv_coll_params->init_buf;
    step->send_buffer = send_coll_params->init_buf;

    unsigned send_start_block = phase->ex_attr.start_block;
    unsigned send_num_blocks = phase->ex_attr.num_blocks;
    unsigned member_cnt = node_cnt;
    unsigned phase_send_buffer_length = 0;

    unsigned block_idx = send_start_block;
    while (block_idx < (send_start_block + send_num_blocks)) {
        int real_block_idx = block_idx % member_cnt;
        phase_send_buffer_length += step->send_coll_params->counts[real_block_idx];
        block_idx++;
    }
    phase_send_buffer_length *= send_dt_len;

    status = ucg_builtin_step_alloc_pack_rank_buffer(step, phase_send_buffer_length);
    if (status != UCS_OK) {
        req->plummer_req_status = status;
        return;
    }
}

void ucg_builtin_plummer_scatter_recv_buffers_cb(ucg_builtin_request_t *req)
{
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_coll_params_t *recv_coll_params = step->recv_coll_params;
    ucg_builtin_coll_params_t *send_coll_params = step->send_coll_params;
    size_t send_dt_len = params->send.dt_len;
    unsigned member_cnt = phase->ex_attr.member_cnt;
    unsigned ppn = phase->ex_attr.ppn;

    /* initialize send coll parameters */
    if (phase->ex_attr.is_node_leader) {
        /* temp recv counts */
        int *temp_recv_counts = (int *)req->op->temp_data_buffer1;

        /* init send counts and displs*/
        unsigned i, j, k;
        for (i = 0; i < ppn; i++) {
            k = i * member_cnt;
            for (j = 0; j < member_cnt; j++) {
                send_coll_params->counts[i] += temp_recv_counts[k++];
            }
        }
        for (i = 0; i < (ppn-1); i++) {
            send_coll_params->displs[i+1] = send_coll_params->displs[i] + send_coll_params->counts[i];
        }

        /* init send buffers, first memory redistribution */
        size_t member_cnt_size = member_cnt * sizeof(int);
        int *temp_send_counts_new = (int *)ucs_malloc(member_cnt_size, "allocate temp displs");
        if (temp_send_counts_new == NULL) {
            ucs_fatal("no memory for malloc, member_cnt_size: %lu", member_cnt_size);
        }
        memset(temp_send_counts_new, 0, member_cnt_size);

        unsigned idx1;
        unsigned node_cnt = member_cnt / ppn;
        for (k = 0; k < node_cnt; k++) {
            idx1 = k * ppn;
            for (i = 0; i < ppn; i++) {
                for (j = 0; j < ppn; j++) {
                    temp_send_counts_new[idx1+i] += temp_recv_counts[i*member_cnt+idx1+j];
                }
            }
        }

        int *temp_send_displs_new = (int *)ucs_malloc(member_cnt_size, "allocate temp displs");
        if (temp_send_displs_new == NULL) {
            ucg_builtin_free((void **)&temp_send_counts_new);
            ucs_fatal("no memory for malloc, member_cnt_size: %lu", member_cnt_size);
        }
        memset(temp_send_displs_new, 0, member_cnt_size);

        for (i = 0; i < (member_cnt-1); i++) {
            temp_send_displs_new[i+1] = temp_send_displs_new[i] + temp_send_counts_new[i];
        }

        size_t send_buf_size = (temp_send_counts_new[member_cnt-1] + temp_send_displs_new[member_cnt-1]) * send_dt_len;
        send_coll_params->init_buf = (int8_t *)ucs_malloc(send_buf_size, "allocate init buffer");
        if (send_coll_params->init_buf == NULL) {
            ucg_builtin_free((void **)&temp_send_counts_new);
            ucg_builtin_free((void **)&temp_send_displs_new);
            ucs_fatal("no memory for malloc, send_buf_size: %lu", send_buf_size);
        }

        int8_t *temp_init_buf = send_coll_params->init_buf;
        for (j = 0; j < ppn; j++) {
            for (k = 0; k < node_cnt; k++) {
                idx1 = k * ppn + j;
                unsigned count = temp_send_counts_new[idx1] * send_dt_len;
                unsigned disp = temp_send_displs_new[idx1] * send_dt_len;
                memcpy(temp_init_buf , req->op->temp_exchange_buffer1 + disp, count);
                temp_init_buf += count;
            }
        }
        ucg_builtin_free((void **)&temp_send_counts_new);
        ucg_builtin_free((void **)&temp_send_displs_new);
        ucg_builtin_free((void **)&req->op->temp_exchange_buffer1);

        req->op->temp_exchange_buffer1 = send_coll_params->init_buf;

        memcpy((int8_t *)params->recv.buf, send_coll_params->init_buf, send_dt_len * send_coll_params->counts[0]);

        unsigned send_start_block = phase->ex_attr.start_block;
        unsigned send_num_blocks = phase->ex_attr.num_blocks;
        unsigned block_idx = send_start_block;
        unsigned phase_send_buffer_length = 0;

        while (block_idx < (send_start_block + send_num_blocks)) {
            unsigned real_block_idx = block_idx % ppn;
            phase_send_buffer_length += send_coll_params->counts[real_block_idx];
            block_idx++;
        }
        phase_send_buffer_length *= send_dt_len;

        ucs_status_t status = ucg_builtin_step_alloc_pack_rank_buffer(step, phase_send_buffer_length);
        if (status != UCS_OK) {
            req->plummer_req_status = status;
        }
    } else {
        /* initialize recv coll parameters */
        recv_coll_params->init_buf = (int8_t *)params->recv.buf;
        recv_coll_params->counts[0] = params->recv.counts[member_cnt-1] + params->recv.displs[member_cnt-1];
        recv_coll_params->displs[0] = 0;
    }
}

/* send cb for reduce-way-point */
void ucg_builtin_send_reduce(ucg_builtin_request_t *req)
{
    ucg_builtin_op_step_t *step = req->step;
    if (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_FIRST_STEP)) {
        /* copy reduced data to allocated send buffer */
        memcpy(step->send_buffer, req->op->super.params.recv.buf, step->buffer_length);
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
    if (op->super.params.send.buf == MPI_IN_PLACE) {
        memcpy(step->recv_buffer, op->super.params.recv.buf, step->buffer_length);
    } else {
        memcpy(step->recv_buffer, op->super.params.send.buf, step->buffer_length);
    }
}

static void ucg_builtin_init_rabenseifner(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    unsigned step_idx;
    size_t len = op->super.params.send.count * op->super.params.send.dt_len;
    if (op->super.params.send.buf == MPI_IN_PLACE) {
        memcpy(step->recv_buffer, op->super.params.recv.buf, len);
    } else {
        if (step->recv_buffer != op->super.params.send.buf) {
            memcpy(step->recv_buffer, op->super.params.send.buf, len);
        }
    }
    /* Prevent remote_offset from being set to 0 by multiple calls */
    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
        (&op->steps[step_idx])->am_header.remote_offset = (&op->steps[step_idx])->remote_offset;
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

void ucg_builtin_init_inc(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    unsigned buf_size;
    buf_size = op->super.params.send.count * op->super.params.send.dt_len;
    if (step->recv_buffer != NULL && op->super.params.send.buf != NULL && buf_size > 0) {
        memcpy(step->recv_buffer, op->super.params.send.buf, buf_size);
    }
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
    size_t init_offset;
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

/* for UCG_PLAN_METHOD_EXCHANGE, pairwise at initial step */
static void ucg_builtin_init_pairwise(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    const ucg_group_params_t *params = ucg_group_get_params(op->super.plan->group);
    size_t proc_count  = params->member_count;

    if (op->super.params.send.buf != MPI_IN_PLACE) {
        memcpy(step->recv_buffer, op->super.params.send.buf, step->buffer_length * proc_count);
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

static UCS_F_ALWAYS_INLINE void
ucg_builtin_init_dt_state(ucg_builtin_op_step_t *step, int option,
                       ucp_dt_generic_t *dt_gen,
                       const ucg_collective_params_t *params)
{
    void *state_gen = NULL;

    /* send or recv count is 0 */
    if (dt_gen == NULL) {
        return;
    }

    ucs_debug("ucg_builtin_init_dt_state, option:%d", option);

    switch (option) {
        case UCG_BUILTIN_OP_DT_RECV:
            state_gen = dt_gen->ops.start_unpack(dt_gen->context, step->recv_buffer,
                                                params->recv.count);

            step->non_contig.unpack_state = state_gen;
            break;

        case UCG_BUILTIN_OP_DT_SEND:
            state_gen = dt_gen->ops.start_pack(dt_gen->context, step->send_buffer,
                                            params->send.count);

            step->non_contig.pack_state = state_gen;
            break;

        case UCG_BUILTIN_OP_DT_SWAP:
            state_gen = dt_gen->ops.start_pack(dt_gen->context, step->recv_buffer,
                                            params->recv.count);

            step->non_contig.pack_state_recv = state_gen;
            break;

        default:
            ucs_debug("ucg_builtin_init_dt_state, invalid option:%d", option);
            break;
    }
}

static UCS_F_ALWAYS_INLINE void
ucg_builtin_finalize_dt_state(ucg_builtin_op_step_t *step, int option,
                           ucp_dt_generic_t *dt_gen)
{
    /* send or recv count is 0 */
    if (dt_gen == NULL) {
        return;
    }

    ucs_debug("ucg_builtin_finalize_dt_state, option:%d", option);

    switch (option) {
        case UCG_BUILTIN_OP_DT_RECV:
            dt_gen->ops.finish(step->non_contig.unpack_state);
            break;

        case UCG_BUILTIN_OP_DT_SEND:
            dt_gen->ops.finish(step->non_contig.pack_state);
            break;

        case UCG_BUILTIN_OP_DT_SWAP:
            dt_gen->ops.finish(step->non_contig.pack_state_recv);
            break;

        default:
            ucs_debug("ucg_builtin_finalize_dt_state, invalid option:%d", option);
            break;
    }
}

static void ucg_builtin_init_pack(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    do {
        ucg_builtin_init_dt_state(step, UCG_BUILTIN_OP_DT_SEND, op->send_dt, &op->super.params);
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
}

static void ucg_builtin_init_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    do {
        ucg_builtin_init_dt_state(step, UCG_BUILTIN_OP_DT_RECV, op->recv_dt, &op->super.params);
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
}

static void ucg_builtin_init_pack_and_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_op_step_t *step = &op->steps[0];
    do {
        ucg_builtin_init_dt_state(step, UCG_BUILTIN_OP_DT_SEND, op->send_dt, &op->super.params);
        ucg_builtin_init_dt_state(step, UCG_BUILTIN_OP_DT_RECV, op->recv_dt, &op->super.params);
        if (step->phase->is_swap) {
            ucg_builtin_init_dt_state(step, UCG_BUILTIN_OP_DT_SWAP, op->recv_dt, &op->super.params);
        }
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
}

static void ucg_builtin_init_reduce_and_pack(ucg_builtin_op_t *op)
{
    ucg_builtin_init_reduce(op);
    ucg_builtin_init_pack(op);
}

static void ucg_builtin_init_reduce_and_unpack(ucg_builtin_op_t *op)
{
    ucg_builtin_init_reduce(op);
    ucg_builtin_init_unpack(op);
}

static void ucg_builtin_finalize_pack(ucg_builtin_request_t *req)
{
    ucg_builtin_op_t *op        = req->op;
    ucg_builtin_op_step_t *step = &op->steps[0];
    do {
        ucg_builtin_finalize_dt_state(step, UCG_BUILTIN_OP_DT_SEND, op->send_dt);
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
}

static void ucg_builtin_finalize_unpack(ucg_builtin_request_t *req)
{
    ucg_builtin_op_t *op        = req->op;
    ucg_builtin_op_step_t *step = &op->steps[0];
    do {
        ucg_builtin_finalize_dt_state(step, UCG_BUILTIN_OP_DT_RECV, op->recv_dt);
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
}

static void ucg_builtin_finalize_pack_and_unpack(ucg_builtin_request_t *req)
{
    ucg_builtin_op_t *op        = req->op;
    ucg_builtin_op_step_t *step = &op->steps[0];
    do {
        ucg_builtin_finalize_dt_state(step, UCG_BUILTIN_OP_DT_SEND, op->send_dt);
        ucg_builtin_finalize_dt_state(step, UCG_BUILTIN_OP_DT_RECV, op->recv_dt);
        if (step->phase->is_swap) {
            ucg_builtin_finalize_dt_state(step, UCG_BUILTIN_OP_DT_SWAP, op->recv_dt);
        }
    } while (!((step++)->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
}

/* alltoallv throttled scatter algorithm at initial step */
void ucg_builtin_init_throttled_scatter(ucg_builtin_op_t *op)
{
    ucg_collective_params_t *params = &(op->super.params);
    if (params->send.buf != MPI_IN_PLACE) {
        size_t my_index     = op->super.plan->my_index;
        int send_count      = params->send.counts[my_index];
        int send_displ      = params->send.displs[my_index];
        int recv_displ      = params->recv.displs[my_index];

        if (send_count > 0) {
            uint64_t buffer_len = send_count * params->send.dt_len;
            uint64_t send_buffer_displ = send_displ * params->send.dt_len;
            uint64_t recv_buffer_displ = recv_displ * params->recv.dt_len;

            memcpy(((int8_t *)params->recv.buf) + recv_buffer_displ,
                   ((int8_t *)params->send.buf) + send_buffer_displ, buffer_len);
        }
    }

    unsigned step_idx;
    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
        (&op->steps[step_idx])->am_header.remote_offset = (&op->steps[step_idx])->remote_offset = 0;
    }
}

void ucg_builtin_final_throttled_scatter(ucg_builtin_request_t *req)
{
    ucg_builtin_op_t *op = req->op;

    unsigned step_idx;
    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
        ucg_builtin_op_step_t *step = &(op->steps[step_idx]);
        ucg_builtin_step_free_pack_rank_buffer(step);
        ucg_builtin_free((void **)&step->send_coll_params);
        ucg_builtin_free((void **)&step->recv_coll_params);
    }
}

void ucg_builtin_throttled_scatter_alltoallv_cb(ucg_builtin_request_t *req)
{
    ucg_collective_params_t *params = &(req->op->super.params);
    ucg_builtin_op_step_t *step = req->step;
    ucg_builtin_plan_phase_t *phase = step->phase;
    ucg_builtin_coll_params_t *recv_coll_params = step->recv_coll_params;
    ucg_builtin_coll_params_t *send_coll_params = step->send_coll_params;

    /* initialize step send coll parameters */
    send_coll_params->init_buf = params->send.buf == MPI_IN_PLACE ? (int8_t *)params->recv.buf
                                                                  : (int8_t *)params->send.buf;
    send_coll_params->counts = params->send.counts;
    send_coll_params->displs = params->send.displs;

    /* initialize step recv coll parameters */
    recv_coll_params->init_buf = (int8_t *)params->recv.buf;
    recv_coll_params->counts = params->recv.counts;
    recv_coll_params->displs = params->recv.displs;

    /*allocate pack rank buffer */
    unsigned send_dt_len = params->send.dt_len;
    unsigned send_start_block = phase->ex_attr.start_block;
    unsigned send_num_blocks = phase->ex_attr.num_blocks;
    unsigned member_cnt = phase->ex_attr.member_cnt;
    unsigned block_idx = send_start_block;
    unsigned phase_send_buffer_length = 0;

    while (block_idx < (send_start_block + send_num_blocks)) {
        unsigned real_block_idx = block_idx % member_cnt;
        phase_send_buffer_length += step->send_coll_params->counts[real_block_idx];
        block_idx++;
    }
    phase_send_buffer_length *= send_dt_len;

    ucs_status_t status = ucg_builtin_step_alloc_pack_rank_buffer(step, phase_send_buffer_length);
    if (status != UCS_OK) {
        req->ladd_req_status = status;
    }
}

void ucg_builtin_init_plummer(ucg_builtin_op_t *op)
{
    ucg_collective_params_t *params = &(op->super.params);
    if (params->send.buf != MPI_IN_PLACE) {
        /* Copy its own data from the sendbuffer to the recvbuffer. */
        ucg_group_member_index_t my_index     = op->super.plan->my_index;
        int send_count      = params->send.counts[my_index];
        int send_displ      = params->send.displs[my_index];
        int recv_displ      = params->recv.displs[my_index];

        if (send_count > 0) {
            uint64_t buffer_len = send_count * params->send.dt_len;
            uint64_t send_buffer_displ = send_displ * params->send.dt_len;
            uint64_t recv_buffer_displ = recv_displ * params->recv.dt_len;

            memcpy(((int8_t *)params->recv.buf) + recv_buffer_displ,
                   ((int8_t *)params->send.buf) + send_buffer_displ, buffer_len);
        }
    }
    /* In alltoallv, both remote_offset and am_header.remote_offset initial values are 0.
     * The value is calculated dynamically during message sending and receiving.
     */
    unsigned step_idx;
    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
        (&op->steps[step_idx])->am_header.remote_offset = (&op->steps[step_idx])->remote_offset;
    }
}

void ucg_builtin_final_plummer(ucg_builtin_request_t *req)
{
    ucg_builtin_op_t *op = req->op;

    unsigned step_idx;
    for (step_idx = 0; step_idx < ((ucg_builtin_plan_t *)op->super.plan)->phs_cnt; step_idx++) {
        ucg_builtin_op_step_t *step = &(op->steps[step_idx]);
        ucg_builtin_step_free_pack_rank_buffer(step);

        if (step->phase->ex_attr.is_variable_len) {
            if (step->phase->send_ep_cnt > 0) {
                ucg_builtin_free_coll_params(&(step->send_coll_params));
            }
            if (step->phase->recv_ep_cnt > 0) {
                ucg_builtin_free_coll_params(&(step->recv_coll_params));
            }
        }
    }
    ucg_builtin_free((void **)&(op->temp_data_buffer));
    ucg_builtin_free((void **)&(op->temp_data_buffer1));
    ucg_builtin_free((void **)&(op->temp_exchange_buffer));
    ucg_builtin_free((void **)&(op->temp_exchange_buffer1));
}

static ucs_status_t ucg_builtin_op_select_callback(ucg_builtin_plan_t *plan,
                                                   int is_send_contig,
                                                   int is_recv_contig,
                                                   ucg_builtin_op_init_cb_t *init_cb,
                                                   ucg_builtin_op_final_cb_t *final_cb)
{
    ucs_info("op select callback, method:%d, send_contig:%d, recv_contig:%d",
              plan->phss[0].method, is_send_contig, is_recv_contig);
    unsigned is_allgather = plan->super.type.modifiers & UCG_GROUP_COLLECTIVE_MODIFIER_ALLGATHER;
    switch (plan->phss[0].method) {
        case UCG_PLAN_METHOD_REDUCE_WAYPOINT:
        case UCG_PLAN_METHOD_REDUCE_TERMINAL:
        case UCG_PLAN_METHOD_REDUCE_RECURSIVE:
            if (!is_send_contig) {
                if (!is_recv_contig) {
                    *init_cb  = ucg_builtin_init_pack_and_unpack;
                    *final_cb = ucg_builtin_finalize_pack_and_unpack;
                } else {
                    *init_cb  = ucg_builtin_init_reduce_and_pack;
                    *final_cb = ucg_builtin_finalize_pack;
                }
            } else if (!is_recv_contig) {
                *init_cb  = ucg_builtin_init_reduce_and_unpack;
                *final_cb = ucg_builtin_finalize_unpack;
            } else {
                *init_cb  = ucg_builtin_init_reduce;
                *final_cb = NULL;
            }
            break;

        case UCG_PLAN_METHOD_REDUCE_SCATTER_RECURSIVE:
            *init_cb = ucg_builtin_init_rabenseifner;
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
        case UCG_PLAN_METHOD_INC:
            *init_cb  = ucg_builtin_init_inc;
            *final_cb = NULL;
            break;
        case UCG_PLAN_METHOD_EXCHANGE:
            *init_cb  = is_allgather ? ucg_builtin_init_gather : ucg_builtin_init_pairwise;
            *final_cb = NULL;
            break;
        case UCG_PLAN_METHOD_ALLTOALLV_LADD:
            *init_cb  = ucg_builtin_init_throttled_scatter;
            *final_cb = ucg_builtin_final_throttled_scatter;
            break;
        default:
            if (!is_send_contig) {
                if (!is_recv_contig) {
                    *init_cb  = ucg_builtin_init_pack_and_unpack;
                    *final_cb = ucg_builtin_finalize_pack_and_unpack;
                } else {
                    *init_cb  = ucg_builtin_init_pack;
                    *final_cb = ucg_builtin_finalize_pack;
                }
            } else if (!is_recv_contig) {
                *init_cb  = ucg_builtin_init_unpack;
                *final_cb = ucg_builtin_finalize_unpack;
            } else {
                *init_cb  = ucg_builtin_init_dummy;
                *final_cb = NULL;
            }
            break;
    }

    if (plan->ucg_algo.plummer && (plan->phss[0].method != UCG_PLAN_METHOD_ALLTOALLV_LADD)) {
        *init_cb = ucg_builtin_init_plummer;
        *final_cb = ucg_builtin_final_plummer;
    }
    return UCS_OK;
}

static void ucg_builtin_step_am_zcopy_comp_step_check_cb(uct_completion_t *self,
                                                         ucs_status_t status)
{

    ucg_builtin_zcomp_t *zcomp = ucs_container_of(self, ucg_builtin_zcomp_t, comp);
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
             step->zcopy.zcomp = (ucg_builtin_zcomp_t*)UCS_ALLOC_CHECK(zcomp_cnt *
                     sizeof(*zcomp), "ucg_zcopy_completion");

    /* Initialize all the zero-copy send completion structures */
    while (zcomp_cnt--) {
        zcomp->comp.func  = ucg_builtin_step_am_zcopy_comp_step_check_cb;
        zcomp->comp.count = 1;
        zcomp++;
    }

    /* Register the buffer, creating a memory handle used in zero-copy sends */
    ucs_status_t status = uct_md_mem_reg(step->uct_md, step->send_buffer,
            step->buffer_length, UCT_MD_MEM_ACCESS_ALL, &step->zcopy.memh);
    if (status != UCS_OK) {
        ucs_error("failed to register memory %p, length %ld", step->send_buffer, step->buffer_length);
        ucs_free(zcomp);
        zcomp = NULL;
        return status;
    }
    return UCS_OK;
}

static inline ucs_status_t ucg_builtin_dynamic_zcopy_prep(ucg_builtin_op_step_t *step, unsigned ep_index)
{
    /* Allocate callback context for zero-copy sends */
    ucg_builtin_zcopy_info_t *zcopy = &step->zcopys[ep_index];
    if ((!zcopy->memh) && (!zcopy->zcomp)) {
        uint32_t zcomp_cnt             = step->fragments;
        zcopy->zcopy_pending           = zcomp_cnt;
        zcopy->memh                    = NULL;  /* - in case the allocation fails... */
        zcopy->num_store               = 0;
        ucg_builtin_zcomp_t  *zcomp          =
                        zcopy->zcomp = (ucg_builtin_zcomp_t *)UCS_ALLOC_CHECK(zcomp_cnt *
                        sizeof(*zcomp), "ucg_zcopys_completion");
        ucp_ep_h ucp_ep                = step->phase->ucp_eps[ep_index];
        zcopy->uct_md                  = ucp_ep_get_am_uct_md(ucp_ep);

        /* Initialize all the zero-copy send completion structures */
        while(zcomp_cnt--) {
            zcomp->comp.func = ucg_builtin_step_am_zcopy_comp_step_check_cb;
            zcomp->comp.count = 1;
            zcomp++;
        }

        /* Register the buffer, creating a memory handle used in zero-copy sends */
        ucs_status_t status = uct_md_mem_reg(zcopy->uct_md, step->send_buffer,
            step->buffer_length, UCT_MD_MEM_ACCESS_ALL, &zcopy->memh);

        if (status != UCS_OK) {
            ucs_error("failed to register memory %p, length %ld", step->send_buffer, step->buffer_length);
            ucg_builtin_free((void **)&zcomp);
            return status;
        }

        /* set "current" step->zcopy point to step->zcopys[ep_index] for sending */
        step->zcopy.memh = step->zcopys[ep_index].memh;
        step->zcopy.num_store = step->zcopys[ep_index].num_store;
        step->zcopy.zcomp = step->zcopys[ep_index].zcomp;
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
            (step->phase->md_attr->cap.flags & UCT_MD_FLAG_NEED_MEMH) &&
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
        ucg_builtin_free((void **)op->steps[step_idx].zcopy.zcomp);
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
    /* Currently, this function is shielded in the
     * alltoallv scenario because the buffer length changes.
     */
    if (op->steps[0].phase->method == UCG_PLAN_METHOD_ALLTOALLV_LADD) {
        opt_flag = 0;
    }
    if (opt_flag && !op->send_dt) {
        do {
            step = &op->steps[step_idx++];
            if ((step->flags & UCG_BUILTIN_OP_STEP_FLAG_SEND_AM_BCOPY) &&
                (step->phase->md_attr->cap.max_reg > step->buffer_length) &&
                (step->phase->md_attr->cap.flags & UCT_MD_FLAG_NEED_MEMH)) {
                op->optm_cb = ucg_builtin_optimize_bcopy_to_zcopy;
                op->opt_cnt = config->mem_reg_opt_cnt;
                return UCS_OK;
            }
        } while (!(step->flags & UCG_BUILTIN_OP_STEP_FLAG_LAST_STEP));
    }

    /* Note: This function will be called... after opt_cnt wrap-around */
    op->optm_cb = ucg_builtin_no_optimization;
    op->opt_cnt = 0;
    return UCS_OK;
}
